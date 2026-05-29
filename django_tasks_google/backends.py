import logging
from abc import ABC, abstractmethod
from datetime import timedelta
from functools import partial
from urllib.parse import urlencode, urlparse

from django.core.cache import InvalidCacheBackendError, caches
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
    # Backends that dispatch work to Google Cloud require GCP configuration
    # (project_id, location, base_url, oidc_service_account) and sync scheduled
    # tasks to Cloud Scheduler. Local backends set these False to opt out.
    requires_gcp_config = True
    uses_cloud_scheduler = True
    # Whether cancel_task(..., force=True) is supported by this backend.
    supports_force_cancel = False

    def __init__(self, alias, params):
        super().__init__(alias, params)
        self.project_id = self.options.get("project_id")
        self.location = self.options.get("location")
        self.base_url = self.options.get("base_url")
        self.oidc_service_account = self.options.get("oidc_service_account")
        self.run_once = self.options.get("run_once", False)
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

        if self.heartbeat_interval > self.heartbeat_timeout:
            raise ImproperlyConfigured(
                "heartbeat_interval_seconds cannot be greater than "
                "heartbeat_timeout_seconds"
            )

        if self.requires_gcp_config:
            if not self.project_id:
                raise ImproperlyConfigured("project_id is required")
            if not self.location:
                raise ImproperlyConfigured("location is required")
            if not self.base_url:
                raise ImproperlyConfigured("base_url is required")
            if not self.oidc_service_account:
                raise ImproperlyConfigured("oidc_service_account is required")

            self.base_url = self.base_url.rstrip("/") + "/"
            self.execute_url = self.options.get(
                "execute_url", self.base_url + "execute/"
            )
            self.schedule_url = self.options.get(
                "schedule_url", self.base_url + "schedule/"
            )
            self.oidc_audience = self.options.get(
                "oidc_audience", get_oidc_audience(self.base_url)
            )
        else:
            self.execute_url = None
            self.schedule_url = None
            self.oidc_audience = None

        self.max_history_entries = self.options.get("max_history_entries", 100)
        self.cache_alias = self.options.get("cache_alias", "default")
        self.cache_prefix = self.options.get("cache_prefix", "django-tasks-google")
        self.cache_ttl_max_attempts = self.options.get("cache_ttl_max_attempts", 600)
        self.queue_aliases = self.options.get("queue_aliases", {})

    def resolve_queue_name(self, queue_name):
        """Map a logical queue name to the real GCP resource name.

        Returns the configured alias target, or the name unchanged when no
        alias is defined. Used only when building GCP resource paths; the
        logical name is what gets validated and stored.
        """
        return self.queue_aliases.get(queue_name, queue_name)

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

    @abstractmethod
    def get_max_attempts(self, queue_name):
        pass

    def force_cancel(self, execution):
        # Backend-specific forceful cancellation. The row has already been marked
        # cancelled/FAILED by TaskExecution.cancel(); subclasses that support force
        # cancellation do the work needed to actually interrupt the running task.
        pass

    def get_max_attempts_with_cache(self, queue_name):
        if self.cache_ttl_max_attempts <= 0:
            return self.get_max_attempts(queue_name)
        try:
            cache = caches[self.cache_alias]
        except InvalidCacheBackendError:
            return self.get_max_attempts(queue_name)
        cache_key = f"{self.cache_prefix}:max_attempts:{queue_name}"
        value = cache.get(cache_key)
        if value is None:
            value = self.get_max_attempts(queue_name)
            cache.set(cache_key, value, self.cache_ttl_max_attempts)
        return value


class CloudRunJobsBackend(DjangoTasksGoogleBackend):
    supports_defer = False
    supports_async_task = True
    supports_get_result = True
    supports_priority = False
    supports_force_cancel = True

    def __init__(self, alias, params):
        super().__init__(alias, params)
        self.command = self.options.get(
            "command",
            ["python", "manage.py", "execute_task"],
        )

    def force_cancel(self, execution):
        if not execution.cloud_run_job_execution_name:
            return

        from google.api_core.exceptions import FailedPrecondition
        from google.cloud import run_v2

        client = run_v2.ExecutionsClient()
        try:
            client.cancel_execution(name=execution.cloud_run_job_execution_name)
        except FailedPrecondition as e:
            # The execution is already terminal (succeeded/failed)
            # or already actively cancelling.
            logger.warning(
                "Execution %s is not in a cancellable state: %s",
                execution.cloud_run_job_execution_name,
                e.message,
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
            resolved_name = self.resolve_queue_name(execution.queue_name)
            job_path = (
                f"projects/{self.project_id}/"
                f"locations/{self.location}/"
                f"jobs/{resolved_name}"
            )
            request = run_v2.RunJobRequest(
                name=job_path,  # type: ignore
                overrides=run_v2.RunJobRequest.Overrides(  # type: ignore
                    container_overrides=[  # type: ignore
                        run_v2.RunJobRequest.Overrides.ContainerOverride(
                            args=[*self.command, str(execution.pk)]  # type: ignore
                        )
                    ]
                ),
            )
            try:
                max_attempts = self.get_max_attempts_with_cache(execution.queue_name)
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
            execution.max_attempts = max_attempts
            execution.save(
                update_fields=["cloud_run_job_execution_name", "max_attempts"]
            )
            transaction.on_commit(
                partial(
                    task_enqueued.send_robust,
                    type(self),
                    task_result=execution.task_result,
                )
            )

    def get_max_attempts(self, queue_name):
        from google.cloud import run_v2

        client = run_v2.JobsClient()
        resolved_name = self.resolve_queue_name(queue_name)
        job_config = client.get_job(
            name=f"projects/{self.project_id}/locations/{self.location}/jobs/{resolved_name}"
        )
        retries = job_config.template.template.max_retries
        return retries + 1  # We want total attempts


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
                max_attempts = self.get_max_attempts_with_cache(execution.queue_name)
                resolved_name = self.resolve_queue_name(execution.queue_name)
                cloud_task = client.create_task(
                    parent=f"projects/{self.project_id}/locations/{self.location}/queues/{resolved_name}",
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
            execution.max_attempts = max_attempts
            execution.save(update_fields=["cloud_task_name", "max_attempts"])
            transaction.on_commit(
                partial(
                    task_enqueued.send_robust,
                    type(self),
                    task_result=execution.task_result,
                )
            )

    def get_max_attempts(self, queue_name):
        from google.cloud import tasks_v2

        client = tasks_v2.CloudTasksClient()
        resolved_name = self.resolve_queue_name(queue_name)
        queue = client.get_queue(
            name=f"projects/{self.project_id}/locations/{self.location}/queues/{resolved_name}"
        )
        max_attempts = queue.retry_config.max_attempts
        return max_attempts if max_attempts >= 0 else None


class ProcessBackend(DjangoTasksGoogleBackend):
    """Local, GCP-free backend for development and testing.

    Tasks are persisted as ``TaskExecution`` rows and executed in background
    subprocesses (one per task execution) by the ``run_tasks`` management command
    (a polling worker). It can be configured to mock either Cloud Tasks or Cloud
    Run Jobs via the ``mode`` option, which differ in whether deferred
    (``run_after``) tasks and forceful cancellation are supported.
    """

    requires_gcp_config = False
    uses_cloud_scheduler = False
    supports_async_task = True
    supports_get_result = True
    supports_priority = False
    # supports_defer and supports_force_cancel are set per-instance based on mode.

    def __init__(self, alias, params):
        super().__init__(alias, params)
        mode = self.options.get("mode", "cloud_tasks")
        if mode not in ("cloud_tasks", "cloud_run_jobs"):
            raise ImproperlyConfigured(
                "ProcessBackend mode must be 'cloud_tasks' or 'cloud_run_jobs', "
                f"not {mode!r}"
            )
        self.mode = mode
        # Cloud Tasks supports run_after; Cloud Run Jobs does not.
        self.supports_defer = mode == "cloud_tasks"
        # Like Cloud Run Jobs, cloud_run_jobs mode can be forcibly cancelled
        # (the worker SIGTERMs the task's subprocess).
        self.supports_force_cancel = mode == "cloud_run_jobs"
        self.max_attempts = self.options.get("max_attempts", 1)
        # Worker tuning lives on the backend so multiple aliases can differ.
        self.poll_interval = self.options.get("poll_interval_seconds", 1.0)
        self.max_workers = self.options.get("max_workers", 4)
        self.batch_size = self.options.get("batch_size", self.max_workers)

    def enqueue_gcp(self, execution_id):
        # No GCP. Persist max_attempts (so try_acquire_lease's attempt cap works)
        # and fire task_enqueued on commit, matching the Cloud* backends so signal
        # receivers behave identically. The polling worker runs the task.
        with transaction.atomic():
            execution = TaskExecution.objects.select_for_update().get(pk=execution_id)
            execution.max_attempts = self.max_attempts
            execution.save(update_fields=["max_attempts"])
            transaction.on_commit(
                partial(
                    task_enqueued.send_robust,
                    type(self),
                    task_result=execution.task_result,
                )
            )

    def get_max_attempts(self, queue_name):
        return self.max_attempts
