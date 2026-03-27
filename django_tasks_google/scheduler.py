import uuid
from urllib.parse import urlencode

from django.db import transaction
from django.tasks import Task, task_backends

from django_tasks_google.models import ScheduledTask


def schedule_task(
    task: Task,
    schedule: str,
    *,
    name: str = "",
    description: str = "",
    time_zone: str = "UTC",
    enabled: bool = True,
    backend: str = "",
    queue_name: str = "",
    takes_context: bool | None = None,
    args: list | None = None,
    kwargs: dict | None = None,
) -> ScheduledTask:
    with transaction.atomic():
        scheduled_task = ScheduledTask.objects.create(
            name=name or task.name,
            description=description,
            schedule=schedule,
            time_zone=time_zone,
            state=(
                ScheduledTask.State.ENABLED if enabled else ScheduledTask.State.DISABLED
            ),
            module_path=task.module_path,
            backend_alias=task_backends[backend].alias if backend else "",
            queue_name=queue_name,
            takes_context=takes_context,
            args=args or [],
            kwargs=kwargs or {},
        )
        scheduled_task.sync()
    return scheduled_task


def sync_scheduled_tasks():
    for task in ScheduledTask.objects.all():
        sync_scheduled_task(task.pk)


@transaction.atomic
def sync_scheduled_task(task_id: int):
    from google.api_core.exceptions import NotFound
    from google.cloud import scheduler_v1

    task = ScheduledTask.objects.select_for_update().get(pk=task_id)
    client = scheduler_v1.CloudSchedulerClient()
    backend = task.backend
    parent = f"projects/{backend.project_id}/locations/{backend.location}"
    job_name = f"{parent}/jobs/{task.name}"
    payload = {
        "task_id": str(task_id),
        "backend": task.backend_alias,
        "idempotency_key": str(uuid.uuid4()),
    }
    job = scheduler_v1.Job(
        name=job_name,  # type: ignore
        description=task.description,
        schedule=task.schedule,
        time_zone=task.time_zone,
        http_target=scheduler_v1.HttpTarget(  # type: ignore
            http_method=scheduler_v1.HttpMethod.POST,  # type: ignore
            uri=backend.schedule_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body=urlencode(payload).encode(),  # type: ignore
            oidc_token=scheduler_v1.OidcToken(  # type: ignore
                service_account_email=backend.oidc_service_account,
                audience=backend.oidc_audience,
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


def delete_cloud_scheduler_job_if_exists(job_name: str | None = None):
    from google.api_core.exceptions import NotFound
    from google.cloud import scheduler_v1

    client = scheduler_v1.CloudSchedulerClient()
    if job_name:
        try:
            client.delete_job(name=job_name)
        except NotFound:
            pass
