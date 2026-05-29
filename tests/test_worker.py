import threading
from datetime import timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from django.tasks import TaskResultStatus
from django.utils import timezone

from django_tasks_google.management.commands.run_tasks import Command
from django_tasks_google.models import ScheduledTask, TaskExecution
from django_tasks_google.scheduler import delete_scheduled_task, schedule_task
from django_tasks_google.worker import child_entrypoint
from tests.fake_tasks import process_task


class FakeProcess:
    """Stand-in for multiprocessing.Process in worker unit tests."""

    next_pid = 1000

    def __init__(self, target=None, args=(), daemon=False):
        self.target = target
        self.args = args
        self.daemon = daemon
        FakeProcess.next_pid += 1
        self.pid = FakeProcess.next_pid
        self._alive = False
        self.started = False
        self.terminate_count = 0
        self.kill_count = 0

    def start(self):
        self.started = True
        self._alive = True

    def is_alive(self):
        return self._alive

    def join(self, timeout=None):
        return None

    def terminate(self):
        self.terminate_count += 1
        self._alive = False

    def kill(self):
        self.kill_count += 1
        self._alive = False


class FakeContext:
    def __init__(self):
        self.processes = []

    def Process(self, target, args=(), daemon=False):
        proc = FakeProcess(target=target, args=args, daemon=daemon)
        self.processes.append(proc)
        return proc


@pytest.fixture
def make_worker():
    def _make(**overrides):
        cmd = Command()
        cmd.aliases = overrides.get("aliases", {"process"})
        cmd.queues = overrides.get("queues")
        cmd.catch_up = overrides.get("catch_up", False)
        cmd.max_workers = overrides.get("max_workers", 2)
        cmd.batch_size = overrides.get("batch_size", cmd.max_workers)
        cmd.poll_interval = overrides.get("poll_interval", 1.0)
        cmd.active = {}
        cmd.signaled = set()
        cmd.stopping = threading.Event()
        cmd._initialized = set()
        cmd.mp_context = FakeContext()
        return cmd

    return _make


def make_execution(**kwargs):
    defaults = {
        "module_path": "tests.fake_tasks.process_task",
        "backend_alias": "process",
        "queue_name": "default",
        "args": [],
        "kwargs": {},
        "max_attempts": 3,
    }
    defaults.update(kwargs)
    return TaskExecution.objects.create(**defaults)


# -- Child entrypoint (in-process; no real subprocess) -------------------------


@pytest.mark.django_db
def test_child_entrypoint_runs_task():
    execution = make_execution(args=[1, 2])

    with patch("django.db.connections.close_all"):
        child_entrypoint(execution.pk)

    execution.refresh_from_db()
    assert execution.status == TaskResultStatus.SUCCESSFUL
    assert execution.return_value == {"args": [1, 2], "kwargs": {}}
    assert len(execution.worker_ids) == 1


@pytest.mark.django_db
def test_child_entrypoint_one_attempt_then_retryable():
    execution = make_execution(module_path="tests.fake_tasks.always_fails")

    with patch("django.db.connections.close_all"):
        child_entrypoint(execution.pk)

    execution.refresh_from_db()
    # One attempt, not yet at max_attempts=3 -> back to READY for the poller.
    assert execution.status == TaskResultStatus.READY
    assert len(execution.worker_ids) == 1


@pytest.mark.django_db
def test_child_entrypoint_retries_exhaust_to_failed():
    execution = make_execution(module_path="tests.fake_tasks.always_fails")

    with patch("django.db.connections.close_all"):
        for _ in range(3):  # max_attempts=3; the poller would respawn each time
            child_entrypoint(execution.pk)

    execution.refresh_from_db()
    assert execution.status == TaskResultStatus.FAILED
    assert len(execution.worker_ids) == 3
    assert len(execution.errors) == 3


# -- poll_once claim/spawn -----------------------------------------------------


@pytest.mark.django_db
def test_poll_spawns_one_process_for_ready_row(make_worker):
    execution = make_execution()
    cmd = make_worker()

    cmd.poll_once()

    assert execution.pk in cmd.active
    assert len(cmd.mp_context.processes) == 1
    proc = cmd.mp_context.processes[0]
    assert proc.started
    assert proc.args == (execution.pk,)


@pytest.mark.django_db
def test_poll_does_not_double_spawn(make_worker):
    make_execution()
    cmd = make_worker()

    cmd.poll_once()
    cmd.poll_once()  # process still alive + in active -> no second spawn

    assert len(cmd.mp_context.processes) == 1


@pytest.mark.django_db
def test_poll_respects_run_after(make_worker):
    now = timezone.now()
    execution = make_execution(run_after=now + timedelta(minutes=5))
    cmd = make_worker()

    cmd.poll_once(now=now)
    assert execution.pk not in cmd.active
    assert cmd.mp_context.processes == []

    cmd.poll_once(now=now + timedelta(minutes=6))
    assert execution.pk in cmd.active


@pytest.mark.django_db
def test_poll_ignores_non_process_aliases(make_worker):
    execution = TaskExecution.objects.create(
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        args=[],
        kwargs={},
        max_attempts=3,
    )
    cmd = make_worker()

    cmd.poll_once()

    assert execution.pk not in cmd.active
    assert cmd.mp_context.processes == []


@pytest.mark.django_db
def test_poll_respects_queue_filter(make_worker):
    make_execution(queue_name="other")
    cmd = make_worker(queues={"default"})

    cmd.poll_once()

    assert cmd.active == {}


@pytest.mark.django_db
def test_reap_frees_slot_and_respawns_still_ready_row(make_worker):
    execution = make_execution()
    cmd = make_worker()

    cmd.poll_once()
    proc1 = cmd.mp_context.processes[0]
    # Simulate the child exiting without completing the task (row stays READY).
    proc1._alive = False

    cmd.poll_once()

    assert len(cmd.mp_context.processes) == 2  # reaped then respawned
    assert execution.pk in cmd.active


@pytest.mark.django_db
def test_max_workers_bounds_active(make_worker):
    make_execution()
    make_execution()
    cmd = make_worker(max_workers=1, batch_size=5)

    cmd.poll_once()

    assert len(cmd.active) == 1
    assert len(cmd.mp_context.processes) == 1


# -- Force cancellation --------------------------------------------------------


@pytest.mark.django_db
def test_check_force_cancellations_terminates_once(make_worker):
    execution = make_execution()
    cmd = make_worker()
    cmd.poll_once()
    proc = cmd.active[execution.pk]

    TaskExecution.objects.filter(pk=execution.pk).update(
        force_cancel=True, cancelled_at=timezone.now()
    )

    cmd._check_force_cancellations()
    cmd._check_force_cancellations()  # already signaled -> not terminated again

    assert proc.terminate_count == 1
    assert execution.pk in cmd.signaled


@pytest.mark.django_db
def test_check_force_cancellations_ignores_graceful_cancel(make_worker):
    execution = make_execution()
    cmd = make_worker()
    cmd.poll_once()
    proc = cmd.active[execution.pk]

    # Graceful cancel: cancelled_at set but force_cancel stays False.
    TaskExecution.objects.filter(pk=execution.pk).update(cancelled_at=timezone.now())

    cmd._check_force_cancellations()

    assert proc.terminate_count == 0
    assert cmd.signaled == set()


# -- Scheduled (cron) tasks ----------------------------------------------------


def _make_scheduled(**kwargs):
    defaults = {
        "name": "sched",
        "schedule": "* * * * *",
        "module_path": "tests.fake_tasks.process_task",
        "backend_alias": "process",
        "queue_name": "default",
    }
    defaults.update(kwargs)
    return ScheduledTask.objects.create(**defaults)


def _process_count():
    return TaskExecution.objects.filter(backend_alias="process").count()


@pytest.mark.django_db
def test_maybe_fire_seeds_first_sight_then_fires_next_slot(make_worker):
    sched = _make_scheduled()
    cmd = make_worker()
    now = timezone.now().replace(second=0, microsecond=0)

    cmd._maybe_fire(sched, now)
    assert _process_count() == 0
    sched.refresh_from_db()
    assert sched.idempotency_key.startswith(f"local:{sched.pk}:")

    cmd._maybe_fire(sched, now + timedelta(minutes=1))
    assert _process_count() == 1

    cmd._maybe_fire(sched, now + timedelta(minutes=1))
    assert _process_count() == 1


@pytest.mark.django_db
def test_maybe_fire_catch_up_fires_on_first_sight(make_worker):
    sched = _make_scheduled(schedule="0 12 * * *", name="daily-noon")
    cmd = make_worker(catch_up=True)
    now = timezone.now().replace(hour=18, minute=0, second=0, microsecond=0)

    cmd._maybe_fire(sched, now)

    assert _process_count() == 1


@pytest.mark.django_db
def test_maybe_fire_no_catch_up_skips_elapsed_slot_on_first_sight(make_worker):
    sched = _make_scheduled(schedule="0 12 * * *", name="daily-noon")
    cmd = make_worker(catch_up=False)
    now = timezone.now().replace(hour=18, minute=0, second=0, microsecond=0)

    cmd._maybe_fire(sched, now)

    assert _process_count() == 0
    sched.refresh_from_db()
    assert sched.idempotency_key is not None


@pytest.mark.django_db
def test_maybe_fire_uses_task_timezone(make_worker):
    sched = _make_scheduled(name="tz-minute", time_zone="Asia/Kolkata")
    cmd = make_worker()
    now = timezone.now()

    cmd._maybe_fire(sched, now)

    sched.refresh_from_db()
    expected = now.astimezone(ZoneInfo("Asia/Kolkata"))
    assert "+05:30" in sched.idempotency_key
    assert str(expected.year) in sched.idempotency_key


@pytest.mark.django_db
def test_poll_scheduled_ignores_disabled(make_worker):
    _make_scheduled(name="disabled", state=ScheduledTask.State.DISABLED)
    cmd = make_worker()

    cmd._poll_scheduled(timezone.now())

    assert _process_count() == 0


# -- Scheduler sync is a no-op for process backends ----------------------------


@pytest.mark.django_db
def test_schedule_task_does_not_sync_to_cloud_scheduler():
    with patch("google.cloud.scheduler_v1.CloudSchedulerClient") as client_cls:
        sched = schedule_task(
            process_task,
            "* * * * *",
            name="local-sched",
            backend="process",
        )

    client_cls.assert_not_called()
    assert ScheduledTask.objects.filter(pk=sched.pk).exists()
    assert sched.cloud_scheduler_job_name is None


@pytest.mark.django_db
def test_delete_scheduled_task_does_not_call_cloud_scheduler():
    sched = schedule_task(
        process_task,
        "* * * * *",
        name="local-sched",
        backend="process",
    )

    with patch(
        "django_tasks_google.scheduler.delete_cloud_scheduler_job_if_exists"
    ) as delete_mock:
        delete_scheduled_task(sched.pk)

    delete_mock.assert_not_called()
    assert not ScheduledTask.objects.filter(pk=sched.pk).exists()
