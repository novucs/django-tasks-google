import logging
import threading
import uuid
from datetime import timedelta
from traceback import format_exception

from django.db import close_old_connections, connection, transaction
from django.tasks import task_backends
from django.tasks.base import TaskContext, TaskResultStatus
from django.tasks.signals import task_finished, task_started
from django.utils import timezone

from django_tasks_google.models import TaskExecution

logger = logging.getLogger("django_tasks_google")


class TaskAlreadyFinished(Exception):
    pass


class TaskLeaseConflict(Exception):
    pass


def execute_task(execution_id):
    worker_id, execution = try_acquire_execution_lease(execution_id)
    backend = task_backends[execution.backend]
    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=heartbeat_loop,
        daemon=True,
        kwargs={
            "stop_event": stop_event,
            "execution_id": execution_id,
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
            args = (TaskContext(task_result=execution.task_result), *args)
        task_started.send(sender=type(backend), task_result=execution.task_result)
        return_value = execution.task.call(*args, **execution.kwargs)
        updated = finalize_success(execution_id, worker_id, return_value)
        if not updated:
            return False
        task_finished.send(sender=type(backend), task_result=execution.task_result)
        logger.info(f"{execution} completed")
        return True

    except Exception as e:
        logger.exception(f"An error occurred during {execution}")
        updated = finalize_failure(execution_id, worker_id, e)
        if updated:
            task_finished.send(sender=type(backend), task_result=execution.task_result)
        return False

    finally:
        if backend.heartbeat_enabled:
            stop_event.set()
            heartbeat_thread.join(backend.heartbeat_join_timeout.total_seconds())


def try_acquire_execution_lease(execution_id: int):
    now = timezone.now()
    with transaction.atomic():
        execution = TaskExecution.objects.select_for_update().get(pk=execution_id)
        if execution.status == TaskResultStatus.SUCCESSFUL:
            raise TaskAlreadyFinished(f"{execution} is already done")

        backend = task_backends[execution.backend]
        if (
            execution.status == TaskResultStatus.RUNNING
            and execution.lease_expires_at
            and execution.lease_expires_at > now
        ):
            raise TaskLeaseConflict(
                f"{execution} is already claimed by {execution.lease_worker_id}"
            )

        worker_id = str(uuid.uuid4())
        execution.status = TaskResultStatus.RUNNING
        execution.last_attempted_at = now
        execution.started_at = execution.started_at or now
        execution.lease_worker_id = worker_id
        execution.lease_expires_at = (
            now + backend.heartbeat_timeout if backend.heartbeat_enabled else None
        )
        execution.worker_ids.append(worker_id)
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
        logger.info(f"{execution} acquired by {worker_id}")
        return worker_id, execution


def heartbeat_loop(
    *,
    stop_event: threading.Event,
    execution_id: int,
    worker_id: str,
    timeout: timedelta,
    interval: timedelta,
):
    try:
        while not stop_event.wait(interval.total_seconds()):
            close_old_connections()
            now = timezone.now()
            updated_count = TaskExecution.objects.filter(
                pk=execution_id,
                lease_worker_id=worker_id,
                status=TaskResultStatus.RUNNING,
            ).update(lease_expires_at=now + timeout)
            if updated_count == 0:
                logger.warning(
                    f"{worker_id} lost the lease for execution {execution_id}"
                )
                return
    finally:
        connection.close()


def finalize_success(execution_id, worker_id, return_value):
    close_old_connections()
    with transaction.atomic():
        execution = TaskExecution.objects.select_for_update().get(pk=execution_id)
        if (
            execution.lease_worker_id != worker_id
            or execution.status != TaskResultStatus.RUNNING
        ):
            logger.warning(f"{worker_id} unable to update {execution}")
            return False

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
        return True


def finalize_failure(execution_id, worker_id, e):
    exception_type = type(e)
    error_entry = {
        "exception_class_path": f"{exception_type.__module__}.{exception_type.__qualname__}",
        "traceback": "".join(format_exception(e)),
    }

    close_old_connections()
    with transaction.atomic():
        execution = TaskExecution.objects.select_for_update().get(pk=execution_id)
        if (
            execution.lease_worker_id != worker_id
            or execution.status != TaskResultStatus.RUNNING
        ):
            logger.warning(f"{worker_id} unable to update {execution}")
            return False

        execution.finished_at = timezone.now()
        execution.status = TaskResultStatus.FAILED
        execution.errors = [*(execution.errors or []), error_entry]
        execution.lease_worker_id = None
        execution.lease_expires_at = None
        execution.save(
            update_fields=[
                "finished_at",
                "status",
                "errors",
                "lease_worker_id",
                "lease_expires_at",
            ]
        )
        return True
