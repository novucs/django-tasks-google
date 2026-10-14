# Cancelling tasks

[Back to README](../README.md)

## Graceful cancellation

To support graceful cancellation, your task should periodically check whether it has been
cancelled:

```python
from django.tasks import task, TaskContext
from django_tasks_google.base import is_task_cancelled


@task(takes_context=True)
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

> **Note:** Cancellation is not immediate. Tasks become aware of cancellation during the
> heartbeat check, so there may be a short delay before `is_task_cancelled(context)`
> returns `True`. Passing `is_task_cancelled(context, refresh=True)` will immediately check
> the database.

You can also cancel executions in bulk from the Django admin: select them in the
`TaskExecution` list and run the "Cancel selected executions" action.

## Forceful cancellation (Cloud Run Jobs)

Forceful cancellation is supported with the `CloudRunJobsBackend` (and locally with
`ProcessBackend` in `cloud_run_jobs` mode).

This sends a `SIGTERM` to the container (or, locally, to the task's subprocess), causing a
`TaskCancelledError` to be raised inside the task. Use this to handle cleanup:

```python
from django.tasks import task
from django_tasks_google.base import TaskCancelledError


@task(backend="jobs")
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

## Timeouts

Set a timeout to limit how long each attempt of a task may run:

```python
from datetime import timedelta

from django.tasks import task


@task(backend="jobs", timeout=timedelta(hours=1))
def batch_process():
    ...
```

The timeout applies per attempt, so a retry gets the full timeout again. It is enforced by
the platform the backend runs on:

- **Cloud Run Jobs** uses it as the execution's task timeout. When it runs out, Cloud Run
  sends a `SIGTERM`, raising `TaskCancelledError` inside the task as with forceful
  cancellation, and the attempt is recorded with a `TaskTimeout` error. The task is then
  retried, or marked failed on its last attempt. Allowed range: 1 second to 168 hours.
- **Cloud Tasks** uses it as the task's dispatch deadline: how long Cloud Tasks waits for
  your app to respond (10 minutes by default). Cloud Tasks cannot stop the request, so the
  attempt runs to completion and records its own result. Allowed range: 15 seconds to 30
  minutes.

A timeout outside the backend's range raises `InvalidTask` when the task is defined.

A cancelled execution has `cancelled_at` set on its `TaskExecution`, while a timed-out
attempt adds a `django_tasks_google.base.TaskTimeout` entry to the result's `errors`.

Locally, `ProcessBackend` in `cloud_run_jobs` mode SIGTERMs a task's subprocess once it
outlives its timeout. Stopping the worker SIGTERMs its subprocesses too, and those attempts
are recorded the same way.
