from __future__ import annotations

import logging
from traceback import format_exception

from django.db import models
from django.tasks import (
    DEFAULT_TASK_BACKEND_ALIAS,
    DEFAULT_TASK_QUEUE_NAME,
    Task,
    TaskResult,
    TaskResultStatus,
    task_backends,
)
from django.tasks.base import DEFAULT_TASK_PRIORITY, TaskError
from django.utils.module_loading import import_string

logger = logging.getLogger("django_tasks_google")


class ScheduledTask(models.Model):
    class State(models.TextChoices):
        ENABLED = "enabled"
        DISABLED = "disabled"

    name = models.TextField(unique=True)
    description = models.TextField(blank=True, default="")
    schedule = models.TextField()
    time_zone = models.TextField(default="UTC")
    state = models.TextField(choices=State, default=State.ENABLED)

    priority = models.IntegerField(null=True, blank=True)
    module_path = models.TextField()
    backend_alias = models.TextField(blank=True, default="")
    queue_name = models.TextField(blank=True, default="")
    run_after = models.DateTimeField(null=True, blank=True)
    takes_context = models.BooleanField(null=True, blank=True)

    args = models.JSONField(default=list, blank=True)
    kwargs = models.JSONField(default=dict, blank=True)

    cloud_scheduler_job_name = models.TextField(null=True, unique=True)
    idempotency_key = models.TextField(null=True)

    def sync(self):
        from django_tasks_google.scheduler import sync_scheduled_task

        sync_scheduled_task(self.pk)

    def __str__(self):
        return f"ScheduledTask id={self.pk} path={self.module_path} state={self.state}"

    @property
    def backend(self):
        default = import_string(self.module_path)
        return task_backends[self.backend_alias or default.backend]

    @property
    def task(self) -> Task:
        default = import_string(self.module_path)

        def coalesce(*args):
            return next((v for v in args if v is not None), None)

        return Task(
            priority=coalesce(self.priority, default.priority),
            func=default.func,
            backend=self.backend_alias or default.backend,
            queue_name=self.queue_name or default.queue_name,
            run_after=coalesce(self.run_after, default.run_after),
            takes_context=coalesce(self.takes_context, default.takes_context),
        )

    def enqueue(self):
        self.backend.enqueue(self.task, self.args, self.kwargs)


class TaskExecution(models.Model):
    priority = models.IntegerField(default=DEFAULT_TASK_PRIORITY)
    module_path = models.TextField()
    backend_alias = models.TextField(default=DEFAULT_TASK_BACKEND_ALIAS)
    queue_name = models.TextField(default=DEFAULT_TASK_QUEUE_NAME)
    run_after = models.DateTimeField(null=True)
    takes_context = models.BooleanField(default=False)

    args = models.JSONField(default=list)
    kwargs = models.JSONField(default=dict)

    status = models.TextField(
        choices=TaskResultStatus,
        default=TaskResultStatus.READY.value,
    )
    enqueued_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True)
    finished_at = models.DateTimeField(null=True)
    last_attempted_at = models.DateTimeField(null=True)
    errors = models.JSONField(default=list)
    worker_ids = models.JSONField(default=list)

    return_value = models.JSONField(null=True)

    cancelled_at = models.DateTimeField(null=True)
    cloud_run_job_execution_name = models.TextField(null=True, unique=True)
    cloud_task_name = models.TextField(null=True, unique=True)
    max_attempts = models.IntegerField(null=True)

    lease_worker_id = models.TextField(null=True)
    lease_expires_at = models.DateTimeField(null=True)

    def __str__(self):
        return (
            f"TaskExecution id={self.pk} path={self.module_path} status={self.status}"
        )

    @property
    def backend(self):
        from django_tasks_google.backends import DjangoTasksGoogleBackend

        backend = task_backends[self.backend_alias]
        if not isinstance(backend, DjangoTasksGoogleBackend):
            raise TypeError(
                f"Backend '{self.backend_alias}' must be an instance of "
                f"DjangoTasksGoogleBackend, not {type(backend).__name__}."
            )
        return backend

    @property
    def task(self) -> Task:
        return Task(
            priority=self.priority,
            func=import_string(self.module_path).func,
            backend=self.backend_alias,
            queue_name=self.queue_name,
            run_after=self.run_after,
            takes_context=self.takes_context,
        )

    @property
    def task_result(self) -> TaskResult:
        task_result = TaskResult(
            task=self.task,
            id=str(self.pk),
            status=self.status,
            enqueued_at=self.enqueued_at,
            started_at=self.started_at,
            finished_at=self.finished_at,
            last_attempted_at=self.last_attempted_at,
            args=self.args,
            kwargs=self.kwargs,
            backend=self.backend_alias,
            errors=self.task_errors,
            worker_ids=self.worker_ids,
        )
        object.__setattr__(task_result, "_return_value", self.return_value)
        return task_result

    @property
    def task_errors(self) -> list[TaskError]:
        return [
            TaskError(
                exception_class_path=error["exception_class_path"],
                traceback=error["traceback"],
            )
            for error in self.errors
        ]

    @property
    def is_finished(self):
        return self.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED)

    def append_error_entry(self, exception: BaseException):
        exception_type = type(exception)
        error_entry = {
            "exception_class_path": (
                f"{exception_type.__module__}.{exception_type.__qualname__}"
            ),
            "traceback": "".join(format_exception(exception)),
        }
        self.errors = [*(self.errors or []), error_entry]

        max_history_entries = self.backend.max_history_entries
        if len(self.errors) > max_history_entries:
            logger.warning(
                "Task id=%s path=%s clipping errors to the last %s",
                self.pk,
                self.module_path,
                max_history_entries,
            )
            self.errors = self.errors[-max_history_entries:]
