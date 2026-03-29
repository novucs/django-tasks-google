import contextlib
import json
import logging
import signal
import threading
import uuid

from django.core.serializers.json import DjangoJSONEncoder
from django.db import close_old_connections, connection, transaction
from django.tasks.base import TaskResultStatus
from django.tasks.signals import task_finished, task_started
from django.utils import timezone

from django_tasks_google.base import (
    PermanentTaskError,
    TaskCancelledError,
    TaskContext,
    TaskError,
)
from django_tasks_google.models import TaskExecution

logger = logging.getLogger("django_tasks_google")


class TaskExecutor:
    def __init__(self, attempt: int, execution: TaskExecution):
        self.attempt = attempt

        self.execution_id = execution.pk
        self.worker_id = execution.lease_worker_id
        self.backend = execution.backend
        self.path = execution.module_path
        self.args = execution.args
        self.kwargs = execution.kwargs
        self.takes_context = execution.takes_context
        self.task_result = execution.task_result
        self.task = execution.task

        self.stop_event = threading.Event()
        self.lease_lost_event = threading.Event()
        self.heartbeat_thread = threading.Thread(
            target=self.heartbeat_loop, daemon=True
        )

    def execute(self):
        if self.backend.heartbeat_enabled:
            self.heartbeat_thread.start()
        try:
            # When a task accepts context, attach a cancellable context linked to lease
            # ownership. Cancellation here is intentionally cooperative: Django's task
            # framework makes context optional, and this executor cannot safely or
            # portably preempt arbitrary user code. Providing a best-effort cancellation
            # signal gives tasks that want stronger behavior a way to stop early, while
            # preserving compatibility with tasks that do not require that guarantee.
            args = self.args
            if self.takes_context:
                context = TaskContext(
                    task_result=self.task_result,
                    _cancel_event=self.lease_lost_event,
                    _attempt=self.attempt,
                )
                args = (context, *args)

            exception = None
            return_value = None
            try:
                with try_register_sigterm_handler(self.handle_sigterm):
                    return_value = self.task.call(*args, **self.kwargs)
                try:
                    json.dumps(return_value, cls=DjangoJSONEncoder)
                except TypeError as err:
                    raise PermanentTaskError("Cannot serialize return value") from err
            except Exception as err:
                exception = err
            task_result = self.save_task_result(
                return_value=return_value,
                exception=exception,
            )
            return task_result
        finally:
            if self.backend.heartbeat_enabled:
                self.stop_event.set()
                self.heartbeat_thread.join(
                    self.backend.heartbeat_join_timeout.total_seconds()
                )
                if self.heartbeat_thread.is_alive():
                    logger.error(
                        (
                            "Task id=%s path=%s worker=%s failed to shut down "
                            "heartbeat thread within timeout"
                        ),
                        self.execution_id,
                        self.path,
                        self.worker_id,
                    )

    def handle_sigterm(self, signum, frame):
        logger.exception(
            "Task id=%s path=%s worker=%s received cancel signal",
            self.execution_id,
            self.path,
            self.worker_id,
        )
        raise TaskCancelledError("SIGTERM received")

    def save_task_result(self, return_value=None, exception=None):
        if self.lease_lost_event.is_set():
            logger.warning(
                "Task id=%s path=%s worker=%s skipping completion; lease lost",
                self.execution_id,
                self.path,
                self.worker_id,
            )
            return None

        close_old_connections()
        with transaction.atomic():
            execution = (
                TaskExecution.objects.select_for_update()
                .filter(pk=self.execution_id)
                .first()
            )
            if (
                execution is None
                or execution.lease_worker_id != self.worker_id
                or execution.status != TaskResultStatus.RUNNING
            ):
                logger.warning(
                    "Task id=%s path=%s worker=%s lease lost",
                    self.execution_id,
                    self.path,
                    self.worker_id,
                )
                return None

            now = timezone.now()
            execution.lease_worker_id = None
            execution.lease_expires_at = None
            if exception:
                task_is_on_last_attempt = (
                    execution.max_attempts and self.attempt == execution.max_attempts
                )
                exception_is_permanent = (
                    isinstance(exception, TaskError) and not exception.retryable
                )
                if exception_is_permanent or task_is_on_last_attempt:
                    execution.status = TaskResultStatus.FAILED
                    execution.finished_at = now
                else:
                    execution.status = TaskResultStatus.READY
                execution.append_error_entry(exception)
            else:
                execution.status = TaskResultStatus.SUCCESSFUL
                execution.finished_at = now
                execution.return_value = return_value

            execution.save()
            return execution.task_result

    def heartbeat_loop(self):
        timeout = self.backend.heartbeat_timeout
        interval = self.backend.heartbeat_interval
        last_successful_heartbeat_at = timezone.now()

        try:
            while not self.stop_event.wait(interval.total_seconds()):
                close_old_connections()

                try:
                    now = timezone.now()
                    updated_count = TaskExecution.objects.filter(
                        pk=self.execution_id,
                        lease_worker_id=self.worker_id,
                        status=TaskResultStatus.RUNNING,
                    ).update(lease_expires_at=now + timeout)

                    if updated_count == 0:
                        logger.warning(
                            "Task id=%s path=%s worker=%s lease lost",
                            self.execution_id,
                            self.path,
                            self.worker_id,
                        )
                        self.lease_lost_event.set()
                        return

                    last_successful_heartbeat_at = now

                except Exception as err:
                    logger.exception(
                        "Task id=%s path=%s worker=%s heartbeat failed: %s",
                        self.execution_id,
                        self.path,
                        self.worker_id,
                        err,
                    )

                    if timezone.now() >= last_successful_heartbeat_at + timeout:
                        logger.warning(
                            (
                                "Task id=%s path=%s worker=%s heartbeat deadline "
                                "exceeded; treating lease as lost"
                            ),
                            self.execution_id,
                            self.path,
                            self.worker_id,
                        )
                        self.lease_lost_event.set()
                        return
        finally:
            connection.close()


def execute_task(execution_id, attempt):
    execution = try_acquire_lease(execution_id, attempt)
    if not execution:
        return False
    if len(execution.worker_ids) == 1:
        # Failed signals are automatically logged for us.
        # Also, Django's Task Framework automatically logs the task events.
        task_started.send_robust(
            sender=type(execution.backend),
            task_result=execution.task_result,
        )
    logger.info(
        "Task id=%s path=%s starting attempt %s/%s",
        execution_id,
        execution.module_path,
        attempt,
        execution.max_attempts,
    )
    executor = TaskExecutor(attempt, execution)
    task_result = executor.execute()
    logger.info(
        "Task id=%s path=%s finished attempt %s/%s",
        execution_id,
        execution.module_path,
        attempt,
        execution.max_attempts,
    )
    if not task_result:
        return False  # Lease was lost during execution, do not retry.

    if task_result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
        task_finished.send_robust(
            sender=type(execution.backend),
            task_result=task_result,
        )

    # Only attempt to retry if the task has been put back to "READY" status.
    return task_result.status == TaskResultStatus.READY


def try_acquire_lease(execution_id, attempt):
    now = timezone.now()
    with transaction.atomic():
        execution = (
            TaskExecution.objects.select_for_update().filter(pk=execution_id).first()
        )
        if not execution:
            logger.warning("Task id=%s not found", execution_id)
            return None

        if execution.is_finished:
            logger.warning(
                "Task id=%s path=%s status=%s is already finished",
                execution_id,
                execution.module_path,
                execution.status,
            )
            return None

        backend = execution.backend
        if (
            execution.status == TaskResultStatus.RUNNING
            and execution.lease_expires_at
            and execution.lease_expires_at > now
        ):
            logger.warning(
                "Task id=%s path=%s lease is already claimed by worker=%s",
                execution_id,
                execution.module_path,
                execution.lease_worker_id,
            )
            return None

        if backend.run_once and len(execution.worker_ids) > 0:
            logger.warning(
                "Task id=%s path=%s cannot be run more than once",
                execution_id,
                execution.module_path,
            )
            return None

        if execution.max_attempts and attempt > execution.max_attempts:
            logger.warning(
                "Task id=%s path=%s has exceeded its max attempts",
                execution_id,
                execution.module_path,
            )
            return None

        worker_id = str(uuid.uuid4())
        execution.status = TaskResultStatus.RUNNING
        execution.last_attempted_at = now
        execution.started_at = execution.started_at or now
        execution.lease_worker_id = worker_id
        execution.lease_expires_at = (
            now + backend.heartbeat_timeout if backend.heartbeat_enabled else None
        )
        execution.worker_ids = [*execution.worker_ids, worker_id]
        if len(execution.worker_ids) > backend.max_history_entries:
            logger.warning(
                "Task id=%s path=%s clipping worker_ids to the last %s",
                execution_id,
                execution.module_path,
                backend.max_history_entries,
            )
            execution.worker_ids = execution.worker_ids[-backend.max_history_entries :]
        execution.save(
            update_fields=[
                "status",
                "last_attempted_at",
                "started_at",
                "lease_worker_id",
                "lease_expires_at",
                "worker_ids",
            ]
        )
        logger.info(
            "Task id=%s path=%s lease successfully claimed by worker=%s",
            execution_id,
            execution.module_path,
            worker_id,
        )
        return execution


@contextlib.contextmanager
def try_register_sigterm_handler(func):
    # We only want to register sigterm handlers within Cloud Run Jobs
    # execution environments, which will be in the main thread.
    # We do NOT want to register sigterm handlers when we are within
    # a Django view.
    if threading.current_thread() is not threading.main_thread():
        yield  # We're not in the main thread, so we can silently ignore.
        return
    old_handler = signal.getsignal(signal.SIGTERM)
    try:
        try:
            signal.signal(signal.SIGTERM, func)
        except ValueError:
            logger.exception("Could not register SIGTERM handler")
        yield
    finally:
        if old_handler is not None:
            try:
                signal.signal(signal.SIGTERM, old_handler)
            except ValueError:
                pass  # Already logged
