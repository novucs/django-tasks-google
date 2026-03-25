from django.db import models
from django.tasks import (
    TaskResultStatus,
    Task,
    TaskResult,
    DEFAULT_TASK_QUEUE_NAME,
    task_backends,
    DEFAULT_TASK_BACKEND_ALIAS,
)
from django.tasks.base import TaskError, DEFAULT_TASK_PRIORITY
from django.utils import timezone
from django.utils.module_loading import import_string


class ScheduledTask(models.Model):
    class State(models.TextChoices):
        ENABLED = "enabled"
        DISABLED = "disabled"

    name = models.TextField(unique=True)
    description = models.TextField(blank=True, default="")
    module_path = models.TextField()
    backend = models.TextField(default="scheduler")
    takes_context = models.BooleanField(default=False)
    args = models.JSONField(default=list, blank=True)
    kwargs = models.JSONField(default=dict, blank=True)

    schedule = models.TextField()
    time_zone = models.TextField(default="UTC")
    cloud_scheduler_job_name = models.TextField(null=True, unique=True)
    state = models.TextField(choices=State, default=State.ENABLED)

    def sync(self):
        from django_tasks_google.scheduler import sync_scheduled_task

        sync_scheduled_task(self)


class TaskExecution(models.Model):
    priority = models.IntegerField(default=DEFAULT_TASK_PRIORITY)
    module_path = models.TextField()
    backend = models.TextField(default=DEFAULT_TASK_BACKEND_ALIAS)
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
    cloud_scheduler_idempotency_key = models.TextField(null=True, unique=True)

    def __str__(self):
        idempotency_key = (
            self.cloud_run_job_execution_name
            or self.cloud_task_name
            or self.cloud_scheduler_idempotency_key
        )
        return f"[{self.status.upper()}] {idempotency_key} (#{self.pk}) at {self.enqueued_at:%Y-%m-%d %H:%M}"

    @property
    def task(self) -> Task:
        return Task(
            priority=DEFAULT_TASK_PRIORITY,
            func=import_string(self.module_path).func,
            backend=self.backend,
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
            backend=self.backend,
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
    def backend_class(self):
        return task_backends[self.backend]

    def cancel(self):
        if not self.cloud_run_job_execution_name:
            raise NotImplementedError("Only Cloud Run Jobs may be cancelled")

        from google.cloud import run_v2

        client = run_v2.ExecutionsClient()
        client.cancel_execution(
            run_v2.CancelExecutionRequest(name=self.cloud_run_job_execution_name)
        )
        self.cancelled_at = timezone.now()
        self.finished_at = self.cancelled_at
        self.status = TaskResultStatus.FAILED
        self.save(update_fields=["cancelled_at", "finished_at", "status"])
