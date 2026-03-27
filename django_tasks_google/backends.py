import logging
from abc import ABC, abstractmethod
from datetime import timedelta
from functools import partial
from urllib.parse import urlencode, urlparse

from django.core.exceptions import ImproperlyConfigured
from django.db import transaction
from django.tasks import TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.signals import task_enqueued

from django_tasks_google.models import TaskExecution

DEFAULT_HEARTBEAT_ENABLED = True
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10
DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 30
DEFAULT_HEARTBEAT_JOIN_TIMEOUT_SECONDS = 5

logger = logging.getLogger("django_tasks_google")


def get_oidc_audience(url):
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}"


class DjangoTasksGoogleBackend(BaseTaskBackend, ABC):
    def __init__(self, alias, params):
        super().__init__(alias, params)
        self.project_id = self.options.get("project_id")
        self.location = self.options.get("location")
        self.heartbeat_enabled = self.options.get(
            "heartbeat_enabled", DEFAULT_HEARTBEAT_ENABLED
        )
        self.heartbeat_interval = timedelta(
            seconds=self.options.get(
                "heartbeat_interval_seconds", DEFAULT_HEARTBEAT_INTERVAL_SECONDS
            )
        )
        self.heartbeat_timeout = timedelta(
            seconds=self.options.get(
                "heartbeat_timeout_seconds", DEFAULT_HEARTBEAT_TIMEOUT_SECONDS
            )
        )
        self.heartbeat_join_timeout = timedelta(
            seconds=self.options.get(
                "heartbeat_join_timeout_seconds", DEFAULT_HEARTBEAT_JOIN_TIMEOUT_SECONDS
            )
        )
        self.base_url = self.options.get("base_url")
        self.oidc_service_account = self.options.get("oidc_service_account")

        if not self.project_id:
            raise ImproperlyConfigured("project_id is required")
        if not self.location:
            raise ImproperlyConfigured("location is required")
        if self.heartbeat_interval > self.heartbeat_timeout:
            raise ImproperlyConfigured(
                "heartbeat_interval_seconds cannot be greater than "
                "heartbeat_timeout_seconds"
            )
        if not self.base_url:
            raise ImproperlyConfigured("base_url is required")
        if not self.oidc_service_account:
            raise ImproperlyConfigured("oidc_service_account is required")

        self.base_url = self.base_url.rstrip("/") + "/"
        self.execute_url = self.options.get("execute_url", self.base_url + "execute/")
        self.schedule_url = self.options.get(
            "schedule_url", self.base_url + "schedule/"
        )
        self.oidc_audience = self.options.get(
            "oidc_audience", get_oidc_audience(self.base_url)
        )

    def enqueue(self, task, args, kwargs):
        self.validate_task(task)
        with transaction.atomic():
            execution = TaskExecution.objects.create(
                priority=task.priority,
                module_path=task.module_path,
                backend_alias=self.alias,
                queue_name=task.queue_name,
                run_after=task.run_after,
                takes_context=task.takes_context,
                args=list(args),
                kwargs=dict(kwargs),
            )
            task_result = execution.task_result
            transaction.on_commit(partial(self.enqueue_gcp, execution.pk))
        return task_result

    @abstractmethod
    def enqueue_gcp(self, execution_id):
        pass

    def get_result(self, result_id):
        try:
            return TaskExecution.objects.get(pk=result_id).task_result
        except TaskExecution.DoesNotExist:
            return None


class CloudRunJobsBackend(DjangoTasksGoogleBackend):
    supports_defer = False
    supports_async_task = True
    supports_get_result = True
    supports_priority = False

    def __init__(self, alias, params):
        super().__init__(alias, params)
        self.command = self.options.get(
            "command",
            ["python", "manage.py", "execute_task"],
        )

    def enqueue_gcp(self, execution_id):
        from google.cloud import run_v2

        # Intentionally hold a row lock while creating the remote job/task.
        # The worker also acquires select_for_update() on this TaskExecution row
        # before running. This prevents the worker from starting and locking the row
        # before we have persisted the remote execution/task identifier.
        with transaction.atomic():
            execution = TaskExecution.objects.select_for_update().get(pk=execution_id)
            client = run_v2.JobsClient()
            request = run_v2.RunJobRequest(
                name=f"projects/{self.project_id}/locations/{self.location}/jobs/{execution.queue_name}",  # type: ignore
                overrides=run_v2.RunJobRequest.Overrides(  # type: ignore
                    container_overrides=[  # type: ignore
                        run_v2.RunJobRequest.Overrides.ContainerOverride(
                            args=[*self.command, str(execution.pk)]  # type: ignore
                        )
                    ]
                ),
            )
            try:
                operation = client.run_job(request=request)
            except Exception as err:
                logger.exception(
                    "Failed enqueuing Cloud Run job execution_id=%s queue=%s",
                    execution_id,
                    execution.queue_name,
                )
                execution.status = TaskResultStatus.FAILED
                execution.append_error_entry(err)
                execution.save(update_fields=["status", "errors"])
                return
            execution.cloud_run_job_execution_name = operation.metadata.name
            execution.save(update_fields=["cloud_run_job_execution_name"])
            transaction.on_commit(
                partial(
                    task_enqueued.send, type(self), task_result=execution.task_result
                )
            )


class CloudTasksBackend(DjangoTasksGoogleBackend):
    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = False

    def enqueue_gcp(self, execution_id):
        from google.cloud import tasks_v2
        from google.protobuf import timestamp_pb2

        # Intentionally hold a row lock while creating the remote job/task.
        # The worker also acquires select_for_update() on this TaskExecution row
        # before running. This prevents the worker from starting and locking the row
        # before we have persisted the remote execution/task identifier.
        with transaction.atomic():
            execution = TaskExecution.objects.select_for_update().get(pk=execution_id)
            client = tasks_v2.CloudTasksClient()
            payload = {
                "execution_id": str(execution_id),
                "backend": execution.backend_alias,
            }
            cloud_task_definition = tasks_v2.Task(
                http_request=tasks_v2.HttpRequest(  # type: ignore
                    http_method=tasks_v2.HttpMethod.POST,  # type: ignore
                    url=self.execute_url,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    body=urlencode(payload).encode(),  # type: ignore
                    oidc_token=tasks_v2.OidcToken(  # type: ignore
                        service_account_email=self.oidc_service_account,
                        audience=self.oidc_audience,
                    ),
                ),
            )
            if execution.run_after:
                schedule_time = timestamp_pb2.Timestamp()
                schedule_time.FromDatetime(execution.run_after)
                cloud_task_definition.schedule_time = schedule_time
            try:
                cloud_task = client.create_task(
                    parent=f"projects/{self.project_id}/locations/{self.location}/queues/{execution.queue_name}",
                    task=cloud_task_definition,
                )
            except Exception as err:
                logger.exception(
                    "Failed enqueuing Cloud Task execution_id=%s queue=%s",
                    execution_id,
                    execution.queue_name,
                )
                execution.status = TaskResultStatus.FAILED
                execution.append_error_entry(err)
                execution.save(update_fields=["status", "errors"])
                return
            execution.cloud_task_name = cloud_task.name
            execution.save(update_fields=["cloud_task_name"])
            transaction.on_commit(
                partial(
                    task_enqueued.send, type(self), task_result=execution.task_result
                )
            )
