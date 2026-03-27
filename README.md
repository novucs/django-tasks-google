# django-tasks-google

Run Django tasks on Google Cloud using Cloud Tasks, Cloud Run Jobs, and Cloud Scheduler - without managing workers or
leaving Django.

> Built on Django 6.0's Task Framework (`django.tasks`)

## What this handles for you

* **Execution routing**
    * Cloud Tasks (async work)
    * Cloud Run Jobs (long-running / batch jobs)
    * Cloud Scheduler (cron)

* **Execution state**
    * Status, results, and errors persisted via `TaskExecution`

* **Idempotency & de-duplication**
    * Scheduler-triggered tasks include idempotency keys
    * Lease-based execution prevents duplicate work during retries

* **Failure handling**
    * Heartbeats detect stalled executions
    * Safe retry behavior across crashes and timeouts

* **Admin & visibility**
    * Manage scheduled tasks via Django admin

## Who this is for

This project is designed for teams who:

* Are already on Google Cloud
* Prefer fully managed infrastructure (no workers or brokers)
* Want to use Django's built-in task framework

## Install

```bash
pip install django-tasks-google
```

## The idea (30 seconds)

1. Define a Django task
2. Choose a backend (Cloud Tasks or Cloud Run Jobs)
3. Call `.enqueue()`
4. It runs on Google Cloud
5. Results are stored in your database

## Setup

### Prerequisites

* A Google Cloud project
* Cloud Tasks / Cloud Run / Cloud Scheduler enabled
* A service account with Cloud Run Invoker (`roles/run.invoker`) permissions

### 1. Add the app

```python
INSTALLED_APPS = [
    "django_tasks_google",
]
```

### 2. Configure backends

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
        "QUEUES": ["my-job"],
        "OPTIONS": {
            "project_id": "your-project-id",
            "location": "us-central1",
            "base_url": "https://your-app.run.app/tasks/",
            "oidc_service_account": "task-invoker@your-project-id.iam.gserviceaccount.com",
        },
    },
}
```

> `QUEUES` maps to Cloud Tasks queues or Cloud Run Job names.

#### Local development

For local development, you can run tasks synchronously without Google Cloud by using Django’s built-in backend:

```python
TASKS = {
    "default": {
        "BACKEND": "django.tasks.backends.ImmediateBackend",
    },
    "jobs": {
        "BACKEND": "django.tasks.backends.ImmediateBackend",
    },
}
```

Tasks will execute immediately in-process when calling `.enqueue()`, making it easy to test and debug without external
services.

### 3. Mount URLs

```python
from django.urls import include, path

urlpatterns = [
    path("tasks/", include("django_tasks_google.urls")),
]
```

## Usage

### Define a task

```python
from django.tasks import task


@task(queue_name="default")  # Cloud Tasks queue
def send_notification(user_id: int):
    return {"user_id": user_id, "status": "sent"}
```

### Enqueue

```python
result = send_notification.enqueue(user_id=1)
```

### Inspect result

```python
result.refresh()
print(result.status)
print(result.return_value)
print(result.errors)
```

## Cloud Run Jobs (long-running work)

```python
@task(backend="jobs", queue_name="my-job")  # Cloud Run Job
def recompute_analytics():
    return {"ok": True}
```

Cancel a running execution:

```python
from django_tasks_google.models import TaskExecution

execution = TaskExecution.objects.get(pk=result.id)
execution.cancel()
```

> Only Cloud Run Job executions can be cancelled.

## Scheduling (cron)

```python
from django_tasks_google.scheduler import schedule_task

scheduled_task = schedule_task(
    send_notification,
    "0 */3 * * *",
    name="send-every-3-hours",
    args=[1],
)
```

This creates a `ScheduledTask` and syncs it to Cloud Scheduler.

You can also manage scheduled tasks via Django admin.

⚠️ Deleting a ScheduledTask does not automatically remove the Cloud Scheduler job. Use:

```python
from django_tasks_google.scheduler import delete_cloud_scheduler_job_if_exists

delete_cloud_scheduler_job_if_exists(scheduled_task.cloud_scheduler_job_name)
scheduled_task.delete()
```

## How scheduling works

1. **Cloud Scheduler** calls your app (`/tasks/schedule/`)
2. Your app calls `task.enqueue()`
3. The task runs via the configured backend

All executions go through the same pipeline, so scheduling behaves the same as manual enqueueing.

## Data model

* `TaskExecution` – execution metadata, status, results/errors
* `ScheduledTask` – cron definitions synced with Cloud Scheduler

## Configuration

### Backend `OPTIONS`

| Option                                | Default                                   | Applies to          | Description                                     |
|---------------------------------------|-------------------------------------------|---------------------|-------------------------------------------------|
| `project_id` **(Required)**           | -                                         | All                 | GCP project ID                                  |
| `location` **(Required)**             | -                                         | All                 | GCP region                                      |
| `base_url` **(Required)**             | -                                         | All                 | Base URL for task endpoints                     |
| `oidc_service_account` **(Required)** | -                                         | All                 | Service account used for authenticated requests |
| `oidc_audience`                       | Derived from `base_url`                   | All                 | OIDC audience override                          |
| `execute_url`                         | `<base_url>/execute/`                     | All                 | Override execute endpoint                       |
| `schedule_url`                        | `<base_url>/schedule/`                    | All                 | Override schedule endpoint                      |
| `heartbeat_enabled`                   | `True`                                    | All                 | Enable heartbeat tracking                       |
| `heartbeat_interval_seconds`          | 10                                        | All                 | Interval between heartbeats                     |
| `heartbeat_timeout_seconds`           | 30                                        | All                 | Time before execution is considered stalled     |
| `heartbeat_join_timeout_seconds`      | 5                                         | All                 | Time to wait for heartbeat shutdown             |
| `command`                             | `["python", "manage.py", "execute_task"]` | Cloud Run Jobs only | Command executed by the job                     |

**Constraint:**
`heartbeat_interval_seconds` must be ≤ `heartbeat_timeout_seconds`.

## Management Commands

### `sync_scheduled_tasks`

**Ensures your Django `ScheduledTask` models exist in Google Cloud Scheduler.**

While the Django admin and `schedule_task` function handle syncing automatically, run this command to force a
resync - ideal for initial deployments or after manual database edits.

```bash
python manage.py sync_scheduled_tasks
```

* **Creates:** Adds any missing tasks to GCP.
* **Updates:** Syncs cron expressions and arguments for existing tasks.
* **Note:** This command **does not delete** jobs from GCP that are missing from your database, preventing accidental
  data loss in shared GCP projects.

### `execute_task`

**The execution engine for Cloud Run Jobs.**

You won't typically run this manually; it is the command Google Cloud invokes to process long-running work.

```bash
python manage.py execute_task <execution_id>
```

* **Internal logic:** Manages the heartbeat, runs the task, and records the result.
* **Debugging:** Use this to re-run a specific `execution_id` locally for troubleshooting.

## Development

```bash
uv run pre-commit install
uv run pre-commit run --all-files
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## References

* [https://docs.djangoproject.com/en/6.0/topics/tasks/](https://docs.djangoproject.com/en/6.0/topics/tasks/)
