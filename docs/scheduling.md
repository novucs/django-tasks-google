# Scheduling (cron)

Run a task on a recurring schedule via Cloud Scheduler.

[Back to README](../README.md)

```python
from django_tasks_google.scheduler import schedule_task

scheduled_task = schedule_task(
    send_notification,
    "0 */3 * * *",
    name="send-every-3-hours",
    args=[1],
)
```

This creates a `ScheduledTask` and syncs it to Cloud Scheduler. You can also manage
scheduled tasks from the Django admin.

`name` is unique, so `schedule_task` creates rather than upserts: calling it again with the
same name raises an error. Call it once, from a one-off management command or the admin,
rather than on every deploy or request. To change a schedule, edit the `ScheduledTask` in the
admin (saving re-syncs to Cloud Scheduler) or delete and recreate it.

To delete a `ScheduledTask` from both the database and Cloud Scheduler:

```python
from django_tasks_google.scheduler import delete_scheduled_task

delete_scheduled_task(scheduled_task.pk)
```

## How scheduling works

1. **Cloud Scheduler** calls your app (`/tasks/schedule/`).
2. Your app calls `task.enqueue()`.
3. The task runs via the configured backend.

All executions go through the same pipeline, so scheduling behaves the same as manual
enqueueing.

Scheduled tasks also run under local development with no Cloud Scheduler. See
[Local development](local-development.md).

## Running a task once, later (deferred)

For a one-off task that should run at a specific time rather than on a recurring schedule,
use Django's `run_after` instead of a `ScheduledTask`:

```python
from datetime import datetime, timedelta, timezone

send_notification.using(
    run_after=datetime.now(timezone.utc) + timedelta(hours=1),
).enqueue(user_id=1)
```

Deferred execution is supported by `CloudTasksBackend` (and `ProcessBackend` in
`cloud_tasks` mode), not by `CloudRunJobsBackend`. See the backend comparison in
[Configuration](configuration.md).
