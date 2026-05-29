# Configuration

All options live under `OPTIONS` for each backend in your `TASKS` setting.

[Back to README](../README.md)

## Configuring backends

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
            "queue_aliases": {"default": "your-cloud-task-queue-name"},
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
            "queue_aliases": {"default": "your-cloud-run-job-name"},
        },
    },
}
```

Select a backend per task with `@task(backend="jobs")`. The two GCP backends differ in
what they support:

|                          | `CloudTasksBackend`                  | `CloudRunJobsBackend`                          |
|--------------------------|--------------------------------------|------------------------------------------------|
| Best for                 | Short async work                     | Long-running and batch jobs                    |
| Runs your code via       | POST to your app's execute endpoint  | A Cloud Run Job execution of your app's image  |
| Deferred (`run_after`)   | Yes                                  | No                                             |
| Forceful cancellation    | No                                   | Yes (SIGTERM, see [Cancelling tasks](cancellation.md)) |

The Cloud Tasks queue and Cloud Run Job you reference (after applying `queue_aliases`) must
already exist in your project. Retry policy (how many attempts) is read from that resource:
the queue's max attempts for Cloud Tasks, or the job's max retries for Cloud Run Jobs. Set
`run_once=True` to opt a backend out of retries entirely.

> `QUEUES` lists the logical queue names used in `@task` and `.using()`. By default these
> are the literal Cloud Tasks queue / Cloud Run Job names; the optional `queue_aliases`
> option maps each to a different (often verbose or environment-specific) real resource
> name, so task code can keep using short, stable names. Unmapped names are used as-is.

## Required settings

| Option                 | Description                                                                                                                               |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `project_id`           | Your Google Cloud project ID. Used to locate Cloud Tasks queues, Cloud Run Jobs, and Scheduler resources.                                 |
| `location`             | GCP region where your resources are deployed (e.g. `us-central1`). Must match your Cloud Tasks / Cloud Run configuration.                 |
| `base_url`             | Public URL where your Django app receives task requests. Must be reachable by Google Cloud services.                                      |
| `oidc_service_account` | Service account used by GCP to authenticate requests to your app. Must have permission to invoke your service (e.g. `roles/run.invoker`). |

## Request and routing

| Option          | Default                 | Description                                                                                                                                                                                                                      |
|-----------------|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `oidc_audience` | Derived from `base_url` | Audience value expected in the OIDC token sent by GCP. Defaults to the **origin of `base_url` (scheme + host, no path)**, matching Cloud Run's default auth behavior. Change only if your service validates tokens against a different audience. |
| `execute_url`   | `<base_url>/execute/`   | Endpoint that receives task execution requests. Change if you mount task URLs at a different path.                                                                                                                              |
| `schedule_url`  | `<base_url>/schedule/`  | Endpoint used by Cloud Scheduler to trigger tasks. Change if your scheduling endpoint lives elsewhere.                                                                                                                          |
| `queue_aliases` | `{}`                    | Maps a logical queue/job name (used in `@task` and `QUEUES`) to the real Cloud Tasks queue / Cloud Run Job name, e.g. `{"default": "your-cloud-task-queue-name"}`. Lets task code use stable short names across environments. Names not in the mapping are used as-is. |

> Example:
> `base_url = "https://my-app.run.app/tasks/"`
> -> `oidc_audience = "https://my-app.run.app"`

## Execution behavior

| Option                            | Default                                   | Description                                                                                                                                                       |
|-----------------------------------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `run_once`                        | `False`                                   | If `True`, the task runs only on the first attempt and will not retry on failure or redelivery. Use for non-idempotent tasks where duplicate execution is unsafe. |
| `command` *(Cloud Run Jobs only)* | `["python", "manage.py", "execute_task"]` | Command executed inside the Cloud Run Job container. Change if your task runner entrypoint differs.                                                               |

## Heartbeat and reliability

These settings help detect and recover from stalled or crashed tasks.

| Option                           | Default | Description                                                                                                                                               |
|----------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `heartbeat_enabled`              | `True`  | Enables periodic "I'm alive" updates during execution. Heartbeats run in a separate thread and are not affected by task execution or blocking operations. |
| `heartbeat_interval_seconds`     | `10`    | How often the heartbeat is recorded. Lower values detect failures faster but increase database writes.                                                    |
| `heartbeat_timeout_seconds`      | `30`    | Time without a heartbeat before a task is considered stalled and its lease is released.                                                                   |
| `heartbeat_join_timeout_seconds` | `5`     | Time to wait for the heartbeat thread to shut down cleanly when the task exits.                                                                           |

> **Important:** If `heartbeat_enabled=False`, you must ensure `heartbeat_timeout_seconds`
> is **longer than your longest-running task**.
>
> If the timeout is exceeded, the task is considered stalled and **its lease is released**.
> This means the running task instance may lose ownership and **must not write results or
> update state**, as another worker may take over execution.

## Storage and limits

| Option                | Default | Description                                                                                                 |
|-----------------------|---------|-------------------------------------------------------------------------------------------------------------|
| `max_history_entries` | `100`   | Maximum number of error entries and worker attempts stored per task execution. Older entries are discarded. |

## Caching (GCP metadata)

These options reduce calls to Google Cloud APIs by caching queue/job configuration.

| Option                   | Default                 | Description                                                                        |
|--------------------------|-------------------------|------------------------------------------------------------------------------------|
| `cache_alias`            | `"default"`             | Django cache used to store GCP metadata (e.g. retry limits).                       |
| `cache_prefix`           | `"django-tasks-google"` | Prefix applied to cache keys to avoid collisions with other application data.      |
| `cache_ttl_max_attempts` | `600`                   | Time (in seconds) to cache `max_attempts` from GCP. Set to `0` to disable caching. |

Local development uses `ProcessBackend`, which has its own options. See
[Local development](local-development.md).
