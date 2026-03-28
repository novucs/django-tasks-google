import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import timedelta

from django.db import close_old_connections, connection, transaction
from django.tasks.base import TaskContext, TaskResultStatus
from django.tasks.signals import task_finished, task_started
from django.utils import timezone

from django_tasks_google.models import TaskExecution

logger = logging.getLogger("django_tasks_google")


class TaskError(Exception):
    retryable = True


class PermanentTaskError(TaskError):
    retryable = False


class TaskCancelledError(TaskError):
    retryable = False


@dataclass(frozen=True, slots=True, kw_only=True)
class CancellableTaskContext(TaskContext):
    cancel_event: threading.Event

    def is_cancelled(self):
        return self.cancel_event.is_set()

    def raise_if_cancelled(self):
        if self.is_cancelled():
            raise TaskCancelledError("Task has been cancelled")


def execute_task(execution_id):
    """
    Execute a task associated with a TaskExecution record.

    This function attempts to acquire a lease for the given execution and, if
    successful, runs the task while maintaining a heartbeat to extend the lease.
    The task is executed with optional context support for cooperative
    cancellation.

    Execution flow:
        - Attempts to acquire a lease for the task.
        - Starts a heartbeat thread (if enabled) to maintain the lease.
        - Emits `task_started` signal before execution.
        - Executes the task callable.
        - On success, finalizes the task as SUCCESSFUL and emits `task_finished`.
        - On failure, finalizes the task as FAILED and emits `task_finished`.
        - If the lease is lost at any point, skips finalization.

    Cancellation:
        If the task accepts a context, it receives a `CancellableTaskContext`
        which allows it to detect lease loss and abort cooperatively.

    Lease semantics:
        - Only the worker holding the lease may finalize the task.
        - If the lease is lost (e.g. heartbeat failure or takeover by another worker),
          the task result is ignored and not persisted.

    :param execution_id: Primary key of the TaskExecution to run.

    :return:
        bool:
            - True if the task should be retried (i.e. failure was retryable).
            - False if:
                - execution was skipped (e.g. lease not acquired),
                - task completed successfully,
                - task failed with a non-retryable error,
                - or lease was lost during execution.
    """

    execution = try_acquire_execution_lease(execution_id)
    if not execution:
        return False

    worker_id = execution.lease_worker_id
    backend = execution.backend
    path = execution.module_path
    stop_event = threading.Event()
    lease_lost_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=heartbeat_loop,
        daemon=True,
        kwargs={
            "stop_event": stop_event,
            "lease_lost_event": lease_lost_event,
            "execution_id": execution_id,
            "path": path,
            "worker_id": worker_id,
            "timeout": backend.heartbeat_timeout,
            "interval": backend.heartbeat_interval,
        },
    )
    if backend.heartbeat_enabled:
        heartbeat_thread.start()

    try:
        args = execution.args
        if execution.takes_context:
            context = CancellableTaskContext(
                task_result=execution.task_result,
                cancel_event=lease_lost_event,
            )
            args = (context, *args)
        task_started.send_robust(
            sender=type(backend), task_result=execution.task_result
        )
        return_value = execution.task.call(*args, **execution.kwargs)
        if lease_lost_event.is_set():
            logger.warning(
                "Task id=%s path=%s skipping finalization due to lost lease",
                execution_id,
                path,
            )
            return False
        task_result = finalize_completion(execution_id, path, worker_id, return_value)
        if not task_result:
            return False
        task_finished.send_robust(sender=type(backend), task_result=task_result)
        return False
    except Exception as err:
        logger.exception("Task id=%s path=%s failed: %s", execution_id, path, err)
        if lease_lost_event.is_set():
            logger.warning(
                "Task id=%s path=%s skipping finalization due to lost lease",
                execution_id,
                path,
            )
            return False
        task_result = finalize_failure(execution_id, path, worker_id, err)
        if not task_result:
            return False
        task_finished.send_robust(sender=type(backend), task_result=task_result)
        return err.retryable if isinstance(err, TaskError) else True

    finally:
        if backend.heartbeat_enabled:
            stop_event.set()
            heartbeat_thread.join(backend.heartbeat_join_timeout.total_seconds())


def try_acquire_execution_lease(execution_id: int):
    now = timezone.now()
    with transaction.atomic():
        execution = (
            TaskExecution.objects.select_for_update().filter(pk=execution_id).first()
        )
        if not execution:
            logger.warning("Task id=%s not found", execution_id)
            return None

        if execution.status == TaskResultStatus.SUCCESSFUL:
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

        worker_id = str(uuid.uuid4())
        execution.status = TaskResultStatus.RUNNING
        execution.last_attempted_at = now
        execution.started_at = execution.started_at or now
        execution.lease_worker_id = worker_id
        execution.lease_expires_at = (
            now + backend.heartbeat_timeout if backend.heartbeat_enabled else None
        )
        execution.worker_ids = [*execution.worker_ids, worker_id]
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


def heartbeat_loop(
    *,
    stop_event: threading.Event,
    lease_lost_event: threading.Event,
    execution_id: int,
    path: str,
    worker_id: str,
    timeout: timedelta,
    interval: timedelta,
):
    last_successful_heartbeat_at = timezone.now()

    try:
        while not stop_event.wait(interval.total_seconds()):
            close_old_connections()

            try:
                now = timezone.now()
                updated_count = TaskExecution.objects.filter(
                    pk=execution_id,
                    lease_worker_id=worker_id,
                    status=TaskResultStatus.RUNNING,
                ).update(lease_expires_at=now + timeout)

                if updated_count == 0:
                    logger.warning(
                        "Task id=%s path=%s worker=%s lease lost",
                        execution_id,
                        path,
                        worker_id,
                    )
                    lease_lost_event.set()
                    return

                last_successful_heartbeat_at = now

            except Exception as err:
                logger.exception(
                    "Task id=%s path=%s heartbeat failed: %s",
                    execution_id,
                    path,
                    err,
                )

                if timezone.now() >= last_successful_heartbeat_at + timeout:
                    logger.warning(
                        (
                            "Task id=%s path=%s worker=%s heartbeat deadline exceeded; "
                            "treating lease as lost"
                        ),
                        execution_id,
                        path,
                        worker_id,
                    )
                    lease_lost_event.set()
                    return
    finally:
        connection.close()


def finalize_completion(execution_id, path, worker_id, return_value):
    close_old_connections()
    with transaction.atomic():
        execution = (
            TaskExecution.objects.select_for_update().filter(pk=execution_id).first()
        )
        if (
            execution is None
            or execution.lease_worker_id != worker_id
            or execution.status != TaskResultStatus.RUNNING
        ):
            logger.warning(
                "Task id=%s path=%s worker=%s lease lost",
                execution_id,
                path,
                worker_id,
            )
            return None

        execution.finished_at = timezone.now()
        execution.status = TaskResultStatus.SUCCESSFUL
        execution.return_value = return_value
        execution.lease_worker_id = None
        execution.lease_expires_at = None
        execution.save(
            update_fields=[
                "finished_at",
                "status",
                "return_value",
                "lease_worker_id",
                "lease_expires_at",
            ]
        )
        return execution.task_result


def finalize_failure(execution_id, path, worker_id, exception):
    close_old_connections()
    with transaction.atomic():
        execution = (
            TaskExecution.objects.select_for_update().filter(pk=execution_id).first()
        )
        if (
            execution is None
            or execution.lease_worker_id != worker_id
            or execution.status != TaskResultStatus.RUNNING
        ):
            logger.warning(
                "Task id=%s path=%s worker=%s lease lost",
                execution_id,
                path,
                worker_id,
            )
            return None

        execution.finished_at = timezone.now()
        execution.status = TaskResultStatus.FAILED
        execution.lease_worker_id = None
        execution.lease_expires_at = None
        execution.append_error_entry(exception)
        execution.save(
            update_fields=[
                "finished_at",
                "status",
                "errors",
                "lease_worker_id",
                "lease_expires_at",
            ]
        )
        return execution.task_result
