import threading
from dataclasses import dataclass

from django.tasks import TaskContext as DjangoTaskContext
from django.tasks import TaskResult


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


def cancel_task(task_result_or_id, *, force=False) -> None:
    from django_tasks_google.models import TaskExecution

    task_result_id = (
        task_result_or_id.id
        if isinstance(task_result_or_id, TaskResult)
        else task_result_or_id
    )

    execution = TaskExecution.objects.select_for_update().get(pk=task_result_id)
    execution.cancel(force=force)
