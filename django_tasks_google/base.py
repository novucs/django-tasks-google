import threading
from dataclasses import dataclass

from django.db import transaction
from django.tasks import TaskContext as DjangoTaskContext
from django.tasks import TaskResultStatus
from django.utils import timezone


class TaskError(Exception):
    retryable = True


class PermanentTaskError(TaskError):
    retryable = False


class TaskCancelledError(BaseException):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskContext(DjangoTaskContext):
    _cancel_event: threading.Event
    _attempt: int

    @property
    def attempt(self):
        return self._attempt


def is_task_cancelled(context: DjangoTaskContext, *, refresh=False):
    # Accept DjangoTaskContext to stay compatible with Django's API,
    # but cancellation is only supported on our TaskContext subclass.
    # We enforce this at runtime to safely access internal state and
    # avoid silently returning incorrect results.
    if not isinstance(context, TaskContext):
        raise TypeError(
            f"Expected {TaskContext.__module__}.{TaskContext.__qualname__}, "
            f"got {type(context).__module__}.{type(context).__qualname__}"
        )

    if context._cancel_event.is_set():  # noqa
        return True
    if not refresh:
        return False

    from django_tasks_google.models import TaskExecution

    cancelled = TaskExecution.objects.filter(
        pk=context.task_result.id,
        cancelled_at__isnull=False,
    ).exists()
    if cancelled:
        context._cancel_event.set()  # noqa
    return cancelled


def cancel_task(task_result_id, *, force=False):
    from django_tasks_google.models import TaskExecution

    with transaction.atomic():
        execution = TaskExecution.objects.select_for_update().get(pk=task_result_id)
        if force and not execution.cloud_run_job_execution_name:
            raise NotImplementedError("Only Cloud Run Jobs may be forcibly cancelled")

        now = timezone.now()
        execution.lease_worker_id = None
        execution.lease_expires_at = None
        execution.cancelled_at = now
        execution.finished_at = now
        execution.status = TaskResultStatus.FAILED
        execution.save(
            update_fields=[
                "lease_worker_id",
                "lease_expires_at",
                "cancelled_at",
                "finished_at",
                "status",
            ],
        )

    if force:
        from google.cloud import run_v2

        client = run_v2.ExecutionsClient()
        client.cancel_execution(name=execution.cloud_run_job_execution_name)
