import logging
import multiprocessing
import signal
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from django.core.management.base import BaseCommand, CommandError
from django.db import close_old_connections, transaction
from django.db.models import Q
from django.tasks import TaskResultStatus, task_backends
from django.utils import timezone

from django_tasks_google.backends import ProcessBackend
from django_tasks_google.models import ScheduledTask, TaskExecution
from django_tasks_google.worker import child_entrypoint

logger = logging.getLogger("django_tasks_google")

# How long to wait for child processes to exit during shutdown before SIGKILL.
SHUTDOWN_TIMEOUT_SECONDS = 10


class Command(BaseCommand):
    help = (
        "Run a local polling worker that executes queued tasks in background "
        "subprocesses (one per task) and fires due scheduled (cron) tasks. For "
        "use with ProcessBackend during local development."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--backend",
            action="append",
            default=[],
            dest="backends",
            help=(
                "ProcessBackend alias to serve. May be repeated. "
                "Defaults to every ProcessBackend in TASKS."
            ),
        )
        parser.add_argument(
            "--queue",
            action="append",
            default=[],
            dest="queues",
            help="Only run tasks on this queue. May be repeated.",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Run a single poll iteration, wait for it to finish, and exit.",
        )
        parser.add_argument("--max-workers", type=int, default=None)
        parser.add_argument("--poll-interval", type=float, default=None)
        parser.add_argument("--batch-size", type=int, default=None)
        parser.add_argument(
            "--catch-up",
            action="store_true",
            help=(
                "Also fire a scheduled task for its current cron slot the first "
                "time the worker sees it (e.g. a slot that elapsed before the "
                "worker started). Off by default, matching cron semantics."
            ),
        )

    def handle(self, *args, **options):
        self.aliases = self._resolve_aliases(options["backends"])
        self.queues = set(options["queues"]) or None
        self.catch_up = options["catch_up"]

        # Backend-configured defaults, overridable per invocation.
        backends = [task_backends[a] for a in self.aliases]
        self.max_workers = options["max_workers"] or max(
            (b.max_workers for b in backends), default=4
        )
        self.poll_interval = options["poll_interval"] or min(
            (b.poll_interval for b in backends), default=1.0
        )
        self.batch_size = options["batch_size"] or max(
            (b.batch_size for b in backends), default=self.max_workers
        )

        # spawn: a fresh interpreter per child, safe with Django's open DB
        # connections (fork would copy connection file descriptors).
        self.mp_context = multiprocessing.get_context("spawn")
        self.active = {}  # execution_id -> Process
        self.signaled = set()  # execution_ids already SIGTERM'd (force cancel)
        self.stopping = threading.Event()
        # Scheduled task pks seen during this worker run (see _maybe_fire).
        self._initialized = set()

        self._install_signal_handlers()
        logger.info(
            "run_tasks starting: aliases=%s queues=%s max_workers=%s",
            sorted(self.aliases),
            sorted(self.queues) if self.queues else "all",
            self.max_workers,
        )

        try:
            if options["once"]:
                self.poll_once()
                self._join_active()
            else:
                while not self.stopping.is_set():
                    self.poll_once()
                    self.stopping.wait(self.poll_interval)
        finally:
            self._drain()

    def _resolve_aliases(self, requested):
        process_aliases = {
            alias
            for alias in task_backends.settings
            if isinstance(task_backends[alias], ProcessBackend)
        }
        if not requested:
            if not process_aliases:
                raise CommandError("No ProcessBackend is configured in TASKS.")
            return process_aliases
        unknown = set(requested) - process_aliases
        if unknown:
            raise CommandError(
                f"Not ProcessBackend aliases: {', '.join(sorted(unknown))}"
            )
        return set(requested)

    def _install_signal_handlers(self):
        if threading.current_thread() is not threading.main_thread():
            return

        def handler(signum, frame):
            logger.info("run_tasks received signal %s; shutting down", signum)
            self.stopping.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, handler)
            except ValueError:
                logger.warning("Could not register handler for signal %s", sig)

    def poll_once(self, now=None):
        now = now or timezone.now()
        close_old_connections()
        self._reap()
        self._poll_scheduled(now)
        self._check_force_cancellations()

        if len(self.active) >= self.max_workers:
            return

        candidates = TaskExecution.objects.filter(
            status=TaskResultStatus.READY,
            backend_alias__in=self.aliases,
        ).filter(Q(run_after__isnull=True) | Q(run_after__lte=now))
        if self.queues:
            candidates = candidates.filter(queue_name__in=self.queues)
        candidates = candidates.order_by("-priority", "enqueued_at")

        ids = list(candidates.values_list("pk", flat=True)[: self.batch_size])
        for execution_id in ids:
            if len(self.active) >= self.max_workers:
                break
            if execution_id in self.active:
                continue
            self._spawn(execution_id)

    def _spawn(self, execution_id):
        # try_acquire_lease (in the child) is the atomic gate against double
        # execution across processes; self.active prevents this worker from
        # spawning a second child for a row it already launched.
        proc = self.mp_context.Process(
            target=child_entrypoint, args=(execution_id,), daemon=False
        )
        proc.start()
        self.active[execution_id] = proc

    def _reap(self):
        for execution_id, proc in list(self.active.items()):
            if not proc.is_alive():
                proc.join()
                del self.active[execution_id]
                self.signaled.discard(execution_id)

    def _check_force_cancellations(self):
        if not self.active:
            return
        forced = TaskExecution.objects.filter(
            pk__in=list(self.active),
            force_cancel=True,
            cancelled_at__isnull=False,
        ).values_list("pk", flat=True)
        for execution_id in forced:
            if execution_id in self.signaled:
                continue
            proc = self.active.get(execution_id)
            if proc and proc.is_alive():
                logger.info(
                    "Force cancelling task id=%s (SIGTERM pid=%s)",
                    execution_id,
                    proc.pid,
                )
                proc.terminate()  # SIGTERM on POSIX
                self.signaled.add(execution_id)

    def _join_active(self):
        # Used by --once: wait for spawned children to finish, reaping them.
        while self.active:
            self._reap()
            if not self.active:
                break
            next(iter(self.active.values())).join(0.1)

    def _drain(self):
        self.stopping.set()
        deadline = time.monotonic() + SHUTDOWN_TIMEOUT_SECONDS
        for proc in self.active.values():
            proc.terminate()  # SIGTERM -> task's handler -> TaskCancelledError
        for proc in self.active.values():
            proc.join(max(0.0, deadline - time.monotonic()))
            if proc.is_alive():
                proc.kill()  # SIGKILL last resort
                proc.join()
        self.active.clear()
        self.signaled.clear()

    # -- Scheduled (cron) tasks -------------------------------------------------

    def _poll_scheduled(self, now):
        scheduled = ScheduledTask.objects.filter(state=ScheduledTask.State.ENABLED)
        for sched in scheduled:
            try:
                backend = sched.backend
            except Exception:
                logger.exception(
                    "Could not resolve backend for scheduled task id=%s", sched.pk
                )
                continue
            if not isinstance(backend, ProcessBackend):
                continue
            if self.aliases and backend.alias not in self.aliases:
                continue
            try:
                self._maybe_fire(sched, now)
            except Exception:
                logger.exception("Failed evaluating scheduled task id=%s", sched.pk)

    def _maybe_fire(self, sched, now):
        try:
            from croniter import croniter
        except ImportError as err:
            raise CommandError(
                "croniter is required for scheduled tasks with ProcessBackend. "
                "Install it with: pip install 'django-tasks-google[local]'"
            ) from err

        tz = ZoneInfo(sched.time_zone or "UTC")
        local_now = now.astimezone(tz)
        prev_fire = croniter(sched.schedule, local_now).get_prev(datetime)
        # Namespaced so it never collides with the Cloud Scheduler key format
        # ("{job_name}:{schedule_time}") used by schedule_task_view.
        idempotency_key = f"local:{sched.pk}:{prev_fire.isoformat()}"

        with transaction.atomic():
            locked = ScheduledTask.objects.select_for_update().get(pk=sched.pk)
            if locked.state != ScheduledTask.State.ENABLED:
                return
            if locked.idempotency_key == idempotency_key:
                return  # already fired this slot

            # First time we've seen this task this run: record the current slot
            # without firing (cron semantics - don't backfill a slot that
            # elapsed before the worker started) unless --catch-up was given.
            first_sight = sched.pk not in self._initialized
            self._initialized.add(sched.pk)

            locked.idempotency_key = idempotency_key
            locked.save(update_fields=["idempotency_key"])

            if first_sight and not self.catch_up:
                logger.info(
                    "Seeding scheduled task id=%s at slot %s (not firing)",
                    sched.pk,
                    prev_fire.isoformat(),
                )
                return
            locked.enqueue()
