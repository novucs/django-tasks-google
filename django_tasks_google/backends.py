import json

from django.core.exceptions import ImproperlyConfigured
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.signals import task_enqueued

from django_tasks_google.models import TaskExecution


class CloudRunJobsBackend(BaseTaskBackend):
    supports_defer = False
    supports_async_task = True
    supports_get_result = True
    supports_priority = False

    def __init__(self, alias, params):
        super().__init__(alias, params)
        self.project_id = self.options.get("project_id")
        self.location = self.options.get("location")
        if not self.project_id:
            raise ImproperlyConfigured("project_id is required")
        if not self.location:
            raise ImproperlyConfigured("location is required")

    def enqueue(self, task, args, kwargs):
        from google.cloud import run_v2

        self.validate_task(task)
        execution = TaskExecution.objects.create(
            priority=task.priority,
            module_path=task.module_path,
            backend=self.alias,
            queue_name=task.queue_name,
            run_after=task.run_after,
            takes_context=task.takes_context,
            args=list(args),
            kwargs=dict(kwargs),
        )
        client = run_v2.JobsClient()
        request = run_v2.RunJobRequest(
            name=f"projects/{self.project_id}/locations/{self.location}/jobs/{task.queue_name}",  # type: ignore
            overrides=run_v2.RunJobRequest.Overrides(  # type: ignore
                container_overrides=[  # type: ignore
                    run_v2.RunJobRequest.Overrides.ContainerOverride(
                        args=["python", "manage.py", "execute_task", str(execution.pk)]  # type: ignore
                    )
                ]
            ),
        )
        operation = client.run_job(request=request)
        execution.cloud_run_job_execution_name = operation.metadata.name
        execution.save(update_fields=["cloud_run_job_execution_name"])
        task_result = execution.task_result
        task_enqueued.send(sender=type(self), task_result=task_result)
        return task_result

    def get_result(self, result_id):
        return TaskExecution.objects.get(pk=result_id).task_result


class CloudTasksBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = False

    def __init__(self, alias, params):
        super().__init__(alias, params)
        self.project_id = self.options.get("project_id")
        self.location = self.options.get("location")
        self.target_url = self.options.get("target_url")
        self.oidc_service_account = self.options.get("oidc_service_account")
        if not self.project_id:
            raise ImproperlyConfigured("project_id is required")
        if not self.location:
            raise ImproperlyConfigured("location is required")
        if not self.target_url:
            raise ImproperlyConfigured("target_url is required")
        if not self.oidc_service_account:
            raise ImproperlyConfigured("oidc_service_account is required")

    def enqueue(self, task, args, kwargs):
        from google.cloud import tasks_v2
        from google.protobuf import timestamp_pb2

        self.validate_task(task)
        execution = TaskExecution.objects.create(
            priority=task.priority,
            module_path=task.module_path,
            backend=self.alias,
            queue_name=task.queue_name,
            run_after=task.run_after,
            takes_context=task.takes_context,
            args=list(args),
            kwargs=dict(kwargs),
        )
        client = tasks_v2.CloudTasksClient()
        payload = {"backend": self.alias, "task_execution_id": execution.pk}
        cloud_task_definition = tasks_v2.Task(
            http_request=tasks_v2.HttpRequest(  # type: ignore
                http_method=tasks_v2.HttpMethod.POST,  # type: ignore
                url=self.target_url,
                headers={"Content-Type": "application/json"},
                body=json.dumps(payload).encode(),  # type: ignore
                oidc_token=tasks_v2.OidcToken(  # type: ignore
                    service_account_email=self.oidc_service_account,
                    audience=self.target_url,
                ),
            ),
        )

        if task.run_after:
            schedule_time = timestamp_pb2.Timestamp()
            schedule_time.FromDatetime(task.run_after)
            cloud_task_definition["schedule_time"] = schedule_time

        cloud_task = client.create_task(
            parent=f"projects/{self.project_id}/locations/{self.location}/queues/{task.queue_name}",
            task=cloud_task_definition,
        )
        execution.cloud_task_name = cloud_task.name
        execution.save(update_fields=["cloud_task_name"])
        task_result = execution.task_result
        task_enqueued.send(sender=type(self), task_result=task_result)
        return task_result


class CloudSchedulerBackend(BaseTaskBackend):
    supports_defer = False
    supports_async_task = True
    supports_get_result = False
    supports_priority = False

    def __init__(self, alias, params):
        super().__init__(alias, params)
        self.project_id = self.options.get("project_id")
        self.location = self.options.get("location")
        self.target_url = self.options.get("target_url")
        self.oidc_service_account = self.options.get("oidc_service_account")
        if not self.project_id:
            raise ImproperlyConfigured("project_id is required")
        if not self.location:
            raise ImproperlyConfigured("location is required")
        if not self.target_url:
            raise ImproperlyConfigured("target_url is required")
        if not self.oidc_service_account:
            raise ImproperlyConfigured("oidc_service_account is required")

    def enqueue(self, task, args, kwargs):
        raise NotImplementedError("This task my only be enqueued by Cloud Scheduler")
