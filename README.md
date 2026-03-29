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


@task(backend="jobs", queue_name="my-job")  # Cloud Run Job
def recompute_analytics():
    return {"ok": True}
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

## Schedule tasks (cron)

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

To delete a `ScheduledTask` from both the database and Cloud Scheduler:

```python
from django_tasks_google.scheduler import delete_scheduled_task

delete_scheduled_task(scheduled_task.pk)
```

### How scheduling works

1. **Cloud Scheduler** calls your app (`/tasks/schedule/`)
2. Your app calls `task.enqueue()`
3. The task runs via the configured backend

All executions go through the same pipeline, so scheduling behaves the same as manual enqueueing.

## Cancelling Tasks

### Graceful Cancellation

To support graceful cancellation, your task should periodically check whether it has been cancelled:

```python
from django.tasks import task, TaskContext
from django_tasks_google.base import is_task_cancelled


@task(queue_name="my-queue", takes_context=True)
def batch_process(context: TaskContext):
    while not is_task_cancelled(context):
        ...  # Perform work
```

To cancel the task:

```python
from django_tasks_google.base import cancel_task

result = batch_process.enqueue()
cancel_task(result.id)
```

> **Note:** Cancellation is not immediate. Tasks become aware of cancellation during the heartbeat check, so there may
> be a short delay before `is_task_cancelled(context)` returns `True`.
> Passing `is_task_cancelled(context, refresh=True)` will immediately check the database.

### Forceful Cancellation (Cloud Run Jobs only)

Forceful cancellation is only supported with the `CloudRunJobsBackend`.

This sends a `SIGTERM` to the container, causing a `TaskCancelledError` to be raised inside the task. Use this to handle
cleanup:

```python
from django.tasks import task
from django_tasks_google.base import TaskCancelledError


@task(backend="jobs", queue_name="my-job")
def batch_process():
    try:
        ...  # Perform work
    except TaskCancelledError:
        ...  # Cleanup logic
```

To forcefully cancel the task:

```python
from django_tasks_google.base import cancel_task

result = batch_process.enqueue()
cancel_task(result.id, force=True)
```

## Data model

* `TaskExecution` – execution metadata, status, results/errors
* `ScheduledTask` – cron definitions synced with Cloud Scheduler

## Configuration

### Required settings

| Option                 | Description                                                                                                                               |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `project_id`           | Your Google Cloud project ID. Used to locate Cloud Tasks queues, Cloud Run Jobs, and Scheduler resources.                                 |
| `location`             | GCP region where your resources are deployed (e.g. `us-central1`). Must match your Cloud Tasks / Cloud Run configuration.                 |
| `base_url`             | Public URL where your Django app receives task requests. Must be reachable by Google Cloud services.                                      |
| `oidc_service_account` | Service account used by GCP to authenticate requests to your app. Must have permission to invoke your service (e.g. `roles/run.invoker`). |

### Request & routing

| Option          | Default                 | Description                                                                                                                                                                                                                                      |
|-----------------|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `oidc_audience` | Derived from `base_url` | Audience value expected in the OIDC token sent by GCP. Defaults to the **origin of `base_url` (scheme + host, no path)**, matching Cloud Run’s default auth behavior. Change only if your service validates tokens against a different audience. |
| `execute_url`   | `<base_url>/execute/`   | Endpoint that receives task execution requests. Change if you mount task URLs at a different path.                                                                                                                                               |
| `schedule_url`  | `<base_url>/schedule/`  | Endpoint used by Cloud Scheduler to trigger tasks. Change if your scheduling endpoint lives elsewhere.                                                                                                                                           |

> Example:
> `base_url = "https://my-app.run.app/tasks/"`
> → `oidc_audience = "https://my-app.run.app"`

### Execution behavior

| Option                            | Default                                   | Description                                                                                                                                                       |
|-----------------------------------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `run_once`                        | `False`                                   | If `True`, the task runs only on the first attempt and will not retry on failure or redelivery. Use for non-idempotent tasks where duplicate execution is unsafe. |
| `command` *(Cloud Run Jobs only)* | `["python", "manage.py", "execute_task"]` | Command executed inside the Cloud Run Job container. Change if your task runner entrypoint differs.                                                               |

### Heartbeat & reliability

These settings help detect and recover from stalled or crashed tasks.

| Option                           | Default | Description                                                                                                                                               |
|----------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `heartbeat_enabled`              | `True`  | Enables periodic “I’m alive” updates during execution. Heartbeats run in a separate thread and are not affected by task execution or blocking operations. |
| `heartbeat_interval_seconds`     | `10`    | How often the heartbeat is recorded. Lower values detect failures faster but increase database writes.                                                    |
| `heartbeat_timeout_seconds`      | `30`    | Time without a heartbeat before a task is considered stalled and its lease is released.                                                                   |
| `heartbeat_join_timeout_seconds` | `5`     | Time to wait for the heartbeat thread to shut down cleanly when the task exits.                                                                           |

> ⚠️ **Important:** If `heartbeat_enabled=False`, you must ensure
> `heartbeat_timeout_seconds` is **longer than your longest-running task**.
>
> If the timeout is exceeded, the task is considered stalled and **its lease is released**.
> This means the running task instance may lose ownership and **must not write results or update state**, as another
> worker may take over execution.

### Storage & limits

| Option                | Default | Description                                                                                                 |
|-----------------------|---------|-------------------------------------------------------------------------------------------------------------|
| `max_history_entries` | `100`   | Maximum number of error entries and worker attempts stored per task execution. Older entries are discarded. |

### Caching (GCP metadata)

These options reduce calls to Google Cloud APIs by caching queue/job configuration.

| Option                   | Default                 | Description                                                                        |
|--------------------------|-------------------------|------------------------------------------------------------------------------------|
| `cache_alias`            | `"default"`             | Django cache used to store GCP metadata (e.g. retry limits).                       |
| `cache_prefix`           | `"django-tasks-google"` | Prefix applied to cache keys to avoid collisions with other application data.      |
| `cache_ttl_max_attempts` | `600`                   | Time (in seconds) to cache `max_attempts` from GCP. Set to `0` to disable caching. |

## Development

```bash
uv run pre-commit install
uv run pre-commit run --all-files
uv run ruff check .
uv run ruff format --check .
uv run pytest
DJANGO_SETTINGS_MODULE=tests.settings uv run python -m django makemigrations --check --dry-run
```

## References

* [https://docs.djangoproject.com/en/6.0/topics/tasks/](https://docs.djangoproject.com/en/6.0/topics/tasks/)
