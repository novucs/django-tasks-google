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
