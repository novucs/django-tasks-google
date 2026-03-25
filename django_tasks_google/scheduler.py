import json

from django.db import transaction
from django.tasks import Task, task_backends

from django_tasks_google.models import ScheduledTask


def schedule_task(
    task: Task,
    schedule: str,
    *,
    name: str = "",
    description: str = "",
    backend: str = "scheduler",
    takes_context: bool = False,
    args: list | None = None,
    kwargs: dict | None = None,
    time_zone: str = "UTC",
    enabled: bool = True,
) -> ScheduledTask:
    with transaction.atomic():
        scheduled_task = ScheduledTask.objects.create(
            name=name,
            description=description,
            module_path=task.module_path,
            backend=task_backends[backend].alias,
            takes_context=takes_context,
            args=args or [],
            kwargs=kwargs or {},
            schedule=schedule,
            time_zone=time_zone,
            state=ScheduledTask.State.ENABLED
            if enabled
            else ScheduledTask.State.DISABLED,
        )
        scheduled_task.sync()
    return scheduled_task


def sync_scheduled_tasks():
    for task in ScheduledTask.objects.all():
        sync_scheduled_task(task)


def sync_scheduled_task(task: ScheduledTask):
    from google.api_core.exceptions import NotFound
    from google.cloud import scheduler_v1

    client = scheduler_v1.CloudSchedulerClient()
    backend = task_backends[task.backend]
    parent = f"projects/{backend.project_id}/locations/{backend.location}"
    job_name = f"{parent}/jobs/{task.name}"
    payload = {"backend": task.backend}
    job = scheduler_v1.Job(
        name=job_name,  # type: ignore
        description=task.description,
        schedule=task.schedule,
        time_zone=task.time_zone,
        http_target=scheduler_v1.HttpTarget(  # type: ignore
            http_method=scheduler_v1.HttpMethod.POST,  # type: ignore
            uri=backend.target_url,
            headers={"Content-Type": "application/json"},
            body=json.dumps(payload).encode(),  # type: ignore
            oidc_token=scheduler_v1.OidcToken(  # type: ignore
                service_account_email=backend.oidc_service_account,
                audience=backend.target_url,
            ),
        ),
    )

    job_exists = False
    try:
        client.get_job(name=job_name)
        job_exists = True
    except NotFound:
        pass

    if job_exists:
        update_mask = {"paths": ["description", "schedule", "time_zone", "http_target"]}
        job = client.update_job(job=job, update_mask=update_mask)  # type: ignore
    else:
        job = client.create_job(parent=parent, job=job)

    if job_name != task.cloud_scheduler_job_name:
        task.cloud_scheduler_job_name = job_name
        task.save(update_fields=["cloud_scheduler_job_name"])
    if job.state != job.State.ENABLED and task.state == task.State.ENABLED:
        client.resume_job(name=job_name)
    if job.state != job.State.DISABLED and task.state == task.State.DISABLED:
        client.pause_job(name=job_name)


def delete_remote_scheduled_task(task: ScheduledTask):
    from google.api_core.exceptions import NotFound
    from google.cloud import scheduler_v1

    client = scheduler_v1.CloudSchedulerClient()
    if task.cloud_scheduler_job_name:
        try:
            client.delete_job(name=task.cloud_scheduler_job_name)
        except NotFound:
            pass
