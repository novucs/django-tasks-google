# django-tasks-google

`django-tasks-google` connects Django's Task Framework to Google Cloud execution surfaces:

- Cloud Tasks for async request-driven execution
- Cloud Run Jobs for longer-running workloads
- Cloud Scheduler for cron-style recurring task scheduling

It also includes task execution/scheduling HTTP views, admin tooling for
scheduled jobs, and persisted execution metadata in Django models.

## Installation

```bash
pip install django-tasks-google
```

## Quick Start

### 1) Add the app

```python
INSTALLED_APPS = [
    # ...
    "django_tasks_google",
]
```

### 2) Configure task backends

Use Django `TASKS` settings for Cloud Tasks and/or Cloud Run Jobs.

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks_google.backends.CloudTasksBackend",
        "QUEUES": ["default"],
        "OPTIONS": {
            "project_id": "your-project-id",
            "location": "us-central1",
            "base_url": "https://your-app.run.app/tasks/",
            "oidc_service_account": "task-invoker@your-project-id.iam.gserviceaccount.com",
        },
    },
    "jobs": {
        "BACKEND": "django_tasks_google.backends.CloudRunJobsBackend",
        "QUEUES": ["my-cloud-run-job-name"],
        "OPTIONS": {
            "project_id": "your-project-id",
            "location": "us-central1",
            "base_url": "https://your-app.run.app/tasks/",
            "oidc_service_account": "task-invoker@your-project-id.iam.gserviceaccount.com",
        },
    },
}
```

Notes:

- `base_url` is used to derive:
  - execute endpoint: `<base_url>/execute/`
  - schedule endpoint: `<base_url>/schedule/`
- request auth is verified with OIDC token audience and service account email.
- set `QUEUES` to `[]` when you want backend queue names to be validated lazily
  (for example, when queue/job names are created or managed outside Django).

### Queue and resource name mapping

- `CloudTasksBackend`: task `queue_name` maps to Cloud Tasks queue name.
- `CloudRunJobsBackend`: task `queue_name` maps to Cloud Run Job name.
- `ScheduledTask.name`: maps to Cloud Scheduler job name.

### 3) Mount URLs

```python
from django.urls import include, path

urlpatterns = [
    # ...
    path("tasks/", include("django_tasks_google.urls")),
]
```

### 4) Define tasks

```python
from django.tasks import task


@task(queue_name="default")
def send_notification(user_id: int):
    # ...
    return {"user_id": user_id, "status": "sent"}


@task(backend="jobs", queue_name="my-cloud-run-job-name")
def recompute_analytics():
    # ...
    return {"ok": True}
```

Task definitions must use JSON-serializable values for all `args`, `kwargs`, and
return values.

### 5) Enqueue and inspect results

```python
from django_tasks_google.models import TaskExecution

result = send_notification.enqueue(user_id=1)
result.refresh()

print(result.status)
print(result.return_value)
```

### 6) Cancel a running Cloud Run Job execution

```python
from django_tasks_google.models import TaskExecution

result = recompute_analytics.enqueue()
execution = TaskExecution.objects.get(pk=result.id)

# Only Cloud Run Job executions can be cancelled.
execution.cancel()
```

## Cloud Scheduler Support

Cloud Scheduler is model-driven in this package using `ScheduledTask` and
`schedule_task()`.

```python
from django_tasks_google.scheduler import schedule_task
from myapp.tasks import send_notification

schedule_task(
    send_notification,
    "0 */3 * * *",
    name="send-notification-every-3-hours",
    args=[1],
)
```

This creates/updates scheduler jobs and routes execution through
`/tasks/schedule/`.

You can also manage scheduled entries from Django admin.

## Data Model

- `TaskExecution`: persisted execution metadata and return/error history for
Cloud Tasks and Cloud Run Jobs.
- `ScheduledTask`: persisted schedule definitions for Cloud Scheduler backed
recurring tasks.

## Reliability and idempotency

- task execution uses a lease-based worker model with heartbeats to reduce
  duplicate processing during retries, crashes, or slow workers.
- scheduler-triggered tasks include idempotency keys to prevent duplicate
  enqueue/execute handling.

## Management Commands

- `execute_task <execution_id>`: executes a leased task execution.
- `sync_scheduled_tasks`: syncs all `ScheduledTask` rows to Cloud Scheduler.

## Development

Run checks locally:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

To verify migrations:

```bash
DJANGO_SETTINGS_MODULE=tests.settings PYTHONPATH=. uv run python -m django makemigrations django_tasks_google --check --dry-run
```

## References

- [Django Task Framework docs](https://docs.djangoproject.com/en/6.0/topics/tasks/)

