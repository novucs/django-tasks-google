# Local development

`ProcessBackend` runs tasks locally, without Google Cloud.

Tasks are persisted as `TaskExecution` rows and executed in background processes (one per
task, mirroring Cloud Run Jobs) by a polling worker (`manage.py run_tasks`), so long-running
tasks don't block the request that enqueued them. The worker also runs scheduled (cron) tasks
locally, in place of Cloud Scheduler.

Each backend's `mode` mocks either Cloud Tasks or Cloud Run Jobs: they differ in whether
deferred `run_after` tasks (Cloud Tasks) or forceful cancellation (Cloud Run Jobs) are
supported.

[Back to README](../README.md)

## Install

Install the local extra (pulls in `croniter`, used for cron schedules):

```bash
pip install 'django-tasks-google[local]'
```

## Configure

Mirror your production aliases, swapping each backend for `ProcessBackend` with the
matching `mode`:

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks_google.backends.ProcessBackend",
        "QUEUES": ["default"],
        "OPTIONS": {"mode": "cloud_tasks"},
    },
    "jobs": {
        "BACKEND": "django_tasks_google.backends.ProcessBackend",
        "QUEUES": ["default"],
        "OPTIONS": {"mode": "cloud_run_jobs"},
    },
}
```

## Run the worker

Leave it running alongside `runserver`:

```bash
python manage.py run_tasks
```

Useful flags: `--backend <alias>` (repeatable; defaults to every `ProcessBackend` in
`TASKS`), `--queue <name>` (repeatable), `--max-workers`, `--poll-interval`,
`--batch-size`, `--once` (single poll then exit), and `--catch-up`.

## Options

`ProcessBackend` needs none of the GCP options. It accepts the common options (`run_once`,
heartbeat, storage, caching - see [Configuration](configuration.md)) plus the following.

| Option                  | Default         | Description                                                                                                                                                |
|-------------------------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mode`                  | `"cloud_tasks"` | `"cloud_tasks"` supports deferred (`run_after`) tasks; `"cloud_run_jobs"` supports forceful cancellation instead. Mocks the chosen backend's capabilities. |
| `max_attempts`          | `1`             | Total attempts per task before it is marked failed (retries are driven by the worker, not an external queue).                                              |
| `max_workers`           | `4`             | Maximum number of concurrent task subprocesses.                                                                                                            |
| `poll_interval_seconds` | `1.0`           | How often the worker polls for ready and scheduled tasks.                                                                                                  |
| `batch_size`            | `max_workers`   | Maximum number of ready tasks dispatched per poll iteration.                                                                                                |

## Scheduling and cancellation locally

Scheduled tasks work locally with no extra setup - `schedule_task(...)` creates the
`ScheduledTask` (skipping the Cloud Scheduler sync) and the same worker fires it on
schedule, using each task's `time_zone`. Cron slots are evaluated with `croniter`;
matching cron semantics, a task is **not** fired for a slot that elapsed before the worker
started unless you pass `--catch-up`.

Cancellation works locally too: graceful cancellation is cooperative (as on Google Cloud),
and forceful cancellation (`cloud_run_jobs` mode) SIGTERMs the task's subprocess - raising
`TaskCancelledError` inside the task just like Cloud Run Jobs. See
[Cancelling tasks](cancellation.md).
