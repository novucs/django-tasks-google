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
            "queue_aliases": {"default": "my-service-queue-name-prod"},
        },
    },
    "jobs": {
        "BACKEND": "django_tasks_google.backends.CloudRunJobsBackend",
        "QUEUES": ["default"],
        "OPTIONS": {
            "project_id": "your-project-id",
            "location": "us-central1",
            "base_url": "https://your-app.run.app/tasks/",
            "oidc_service_account": "task-invoker@your-project-id.iam.gserviceaccount.com",
            "queue_aliases": {"default": "my-service-job-name-prod"},
        },
    },
}
```

> `QUEUES` lists the logical queue names used in `@task` and `.using()`. By default these are the literal Cloud Tasks
> queue / Cloud Run Job names; the optional `queue_aliases` option maps each to a different (often verbose or
> environment-specific) real resource name, so task code can keep using short, stable names. Unmapped names are used
> as-is.

#### Local development

`ProcessBackend` runs tasks locally without Google Cloud. Tasks are persisted as `TaskExecution` rows and executed in
background processes (one per task, mirroring Cloud Run Jobs) by a polling worker (`manage.py run_tasks`), so
long-running tasks don't block the request that enqueued them. Each backend's `mode` mocks either Cloud Tasks or
Cloud Run Jobs - they differ in whether deferred `run_after` tasks (Cloud Tasks) and forceful cancellation
(Cloud Run Jobs) are supported. The worker also polls scheduled (cron) tasks locally in place of Cloud Scheduler.

Install the local extra (pulls in `croniter`, used for cron schedules):

```bash
pip install 'django-tasks-google[local]'
```

Mirror your production aliases, swapping each backend for `ProcessBackend` with the matching `mode`:

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks_google.backends.ProcessBackend",
        "QUEUES": ["default"],
        "OPTIONS": {"mode": "cloud_tasks"},
    },
    "jobs": {
        "BACKEND": "django_tasks_google.backends.ProcessBackend",
        "QUEUES": ["my-job"],
        "OPTIONS": {"mode": "cloud_run_jobs"},
    },
}
```

Run the worker (leave it running alongside `runserver`):

```bash
python manage.py run_tasks
```

Useful flags: `--backend <alias>` (repeatable; defaults to every `ProcessBackend` in `TASKS`), `--queue <name>`
(repeatable), `--max-workers`, `--poll-interval`, `--batch-size`, `--once` (single poll then exit), and `--catch-up`.

Scheduled tasks work locally with no extra setup - `schedule_task(...)` creates the `ScheduledTask` (skipping the Cloud
Scheduler sync) and the same worker fires it on schedule, using each task's `time_zone`. Cron slots are evaluated with
`croniter`; matching cron semantics, a task is **not** fired for a slot that elapsed before the worker started unless
you pass `--catch-up`.

Cancellation works locally too: graceful cancellation is cooperative (as on Google Cloud), and forceful cancellation
(`cloud_run_jobs` mode) SIGTERMs the task's subprocess - raising `TaskCancelledError` inside the task just like
Cloud Run Jobs.

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

### Forceful Cancellation (Cloud Run Jobs)

Forceful cancellation is supported with the `CloudRunJobsBackend` (and locally with `ProcessBackend` in
`cloud_run_jobs` mode).

This sends a `SIGTERM` to the container (or, locally, to the task's subprocess), causing a `TaskCancelledError` to be
raised inside the task. Use this to handle cleanup:

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

| Option          | Default                 | Description                                                                                                                                                                                                                                                            |
|-----------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `oidc_audience` | Derived from `base_url` | Audience value expected in the OIDC token sent by GCP. Defaults to the **origin of `base_url` (scheme + host, no path)**, matching Cloud Run’s default auth behavior. Change only if your service validates tokens against a different audience.                       |
| `execute_url`   | `<base_url>/execute/`   | Endpoint that receives task execution requests. Change if you mount task URLs at a different path.                                                                                                                                                                     |
| `schedule_url`  | `<base_url>/schedule/`  | Endpoint used by Cloud Scheduler to trigger tasks. Change if your scheduling endpoint lives elsewhere.                                                                                                                                                                 |
| `queue_aliases` | `{}`                    | Maps a logical queue/job name (used in `@task` and `QUEUES`) to the real Cloud Tasks queue / Cloud Run Job name, e.g. `{"default": "my-service-queue-name-prod"}`. Lets task code use stable short names across environments. Names not in the mapping are used as-is. |

> Example:
> `base_url = "https://my-app.run.app/tasks/"`
> → `oidc_audience = "https://my-app.run.app"`

### Execution behavior

| Option                            | Default                                   | Description                                                                                                                                                       |
|-----------------------------------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `run_once`                        | `False`                                   | If `True`, the task runs only on the first attempt and will not retry on failure or redelivery. Use for non-idempotent tasks where duplicate execution is unsafe. |
| `command` *(Cloud Run Jobs only)* | `["python", "manage.py", "execute_task"]` | Command executed inside the Cloud Run Job container. Change if your task runner entrypoint differs.                                                               |

### ProcessBackend (local development)

`ProcessBackend` needs none of the GCP options above. It accepts the common options (`run_once`, heartbeat, storage,
caching) plus the following. Tasks run via the `run_tasks` polling worker.

| Option                  | Default         | Description                                                                                                                                                |
|-------------------------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mode`                  | `"cloud_tasks"` | `"cloud_tasks"` supports deferred (`run_after`) tasks; `"cloud_run_jobs"` supports forceful cancellation instead. Mocks the chosen backend's capabilities. |
| `max_attempts`          | `1`             | Total attempts per task before it is marked failed (retries are driven by the worker, not an external queue).                                              |
| `max_workers`           | `4`             | Maximum number of concurrent task subprocesses.                                                                                                            |
| `poll_interval_seconds` | `1.0`           | How often the worker polls for ready and scheduled tasks.                                                                                                  |
| `batch_size`            | `max_workers`   | Maximum number of ready tasks dispatched per poll iteration.                                                                                               |

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
