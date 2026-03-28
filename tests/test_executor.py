from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import PropertyMock, patch

import pytest
from django.tasks.base import TaskResultStatus
from django.utils import timezone

from django_tasks_google.executor import TaskExecutor, execute_task, try_acquire_lease
from django_tasks_google.models import TaskExecution


@pytest.fixture
def execution():
    return TaskExecution.objects.create(
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        args=[],
        kwargs={},
    )


@pytest.mark.django_db
def test_try_acquire_execution_lease_sets_running_state(execution):
    leased = try_acquire_lease(execution.pk, attempt=1)
    assert leased is not None
    worker_id = leased.lease_worker_id
    leased.refresh_from_db()

    assert worker_id == leased.lease_worker_id
    assert leased.status == TaskResultStatus.RUNNING
    assert leased.started_at is not None
    assert leased.last_attempted_at is not None
    assert worker_id in leased.worker_ids
    assert leased.lease_expires_at is not None


@pytest.mark.django_db
def test_try_acquire_execution_lease_returns_none_for_successful_task(execution):
    execution.status = TaskResultStatus.SUCCESSFUL
    execution.save(update_fields=["status"])

    assert try_acquire_lease(execution.pk, attempt=1) is None


@pytest.mark.django_db
def test_try_acquire_execution_lease_returns_none_for_active_lease(execution):
    execution.status = TaskResultStatus.RUNNING
    execution.lease_worker_id = "existing-worker"
    execution.lease_expires_at = timezone.now() + timedelta(minutes=1)
    execution.save(update_fields=["status", "lease_worker_id", "lease_expires_at"])

    assert try_acquire_lease(execution.pk, attempt=1) is None


@pytest.mark.django_db
def test_save_task_result_success_updates_execution_when_worker_matches(execution):
    acquired = try_acquire_lease(execution.pk, attempt=1)
    executor = TaskExecutor(1, acquired)
    task_result = executor.save_task_result(return_value={"ok": True})
    execution.refresh_from_db()

    assert task_result is not None
    assert execution.status == TaskResultStatus.SUCCESSFUL
    assert execution.return_value == {"ok": True}
    assert execution.finished_at is not None
    assert execution.lease_worker_id is None
    assert execution.lease_expires_at is None


@pytest.mark.django_db
def test_save_task_result_failure_records_error_and_clears_lease(execution):
    acquired = try_acquire_lease(execution.pk, attempt=1)
    error = RuntimeError("boom")

    executor = TaskExecutor(1, acquired)
    task_result = executor.save_task_result(exception=error)
    execution.refresh_from_db()

    assert task_result is not None
    assert execution.status == TaskResultStatus.READY
    assert execution.finished_at is None
    assert execution.lease_worker_id is None
    assert execution.lease_expires_at is None
    assert len(execution.errors) == 1
    assert execution.errors[0]["exception_class_path"].endswith("RuntimeError")


@pytest.mark.django_db
def test_execute_task_success_returns_false_and_persists_result(execution):
    fake_backend = SimpleNamespace(
        heartbeat_enabled=False,
        heartbeat_timeout=timedelta(seconds=3),
        heartbeat_interval=timedelta(seconds=1),
        heartbeat_join_timeout=timedelta(seconds=1),
        run_once=False,
        max_history_entries=100,
    )
    fake_task = SimpleNamespace(
        call=lambda *args, **kwargs: 42,
        module_path="tests.fake_tasks.sample_task",
    )

    with (
        patch.object(TaskExecution, "task", new_callable=PropertyMock) as task_prop,
        patch.object(
            TaskExecution, "backend", new_callable=PropertyMock
        ) as backend_prop,
    ):
        task_prop.return_value = fake_task
        backend_prop.return_value = fake_backend

        should_retry = execute_task(execution.pk, attempt=1)

    execution.refresh_from_db()
    assert should_retry is False
    assert execution.status == TaskResultStatus.SUCCESSFUL
    assert execution.return_value == 42


@pytest.mark.django_db
def test_execute_task_failure_returns_true_and_records_error(execution):
    fake_backend = SimpleNamespace(
        heartbeat_enabled=False,
        heartbeat_timeout=timedelta(seconds=3),
        heartbeat_interval=timedelta(seconds=1),
        heartbeat_join_timeout=timedelta(seconds=1),
        run_once=False,
        max_history_entries=100,
    )

    def _raise_error(*args, **kwargs):
        raise ValueError("bad-task")

    fake_task = SimpleNamespace(
        call=_raise_error,
        module_path="tests.fake_tasks.sample_task",
    )

    with (
        patch.object(TaskExecution, "task", new_callable=PropertyMock) as task_prop,
        patch.object(
            TaskExecution, "backend", new_callable=PropertyMock
        ) as backend_prop,
    ):
        task_prop.return_value = fake_task
        backend_prop.return_value = fake_backend

        should_retry = execute_task(execution.pk, attempt=1)

    execution.refresh_from_db()
    assert should_retry is True
    assert execution.status == TaskResultStatus.READY
    assert len(execution.errors) == 1
    assert execution.errors[0]["exception_class_path"].endswith("ValueError")
