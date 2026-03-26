# django-tasks-google

**django-tasks-google** provides seamless integration between Django's Task Framework and
Google Cloud's serverless infrastructure.

### Supported Backends

* **Cloud Tasks:** For asynchronous task execution with retries and rate limiting.
* **Cloud Scheduler:** For cron-style scheduled jobs and recurring tasks.
* **Cloud Run Jobs:** For long-running or resource-intensive background processing.

## Installation

Install the package via `pip`:

```bash
pip install django-tasks-google
```

## Quick Start

### 1. Register the Application

Add `django_tasks_google` to your `INSTALLED_APPS` in `settings.py`:

```python
# settings.py
INSTALLED_APPS = [
    # ...
    "django_tasks_google",
]
```

### 2. Configure Task Backends

Define your task execution strategy within the `TASKS` setting. You can mix and match backends based on your
architectural needs.

```python
# settings.py
TASKS = {
    "default": {
        "BACKEND": "django_tasks_google.backends.CloudTasksBackend",
        "QUEUES": [],
        "OPTIONS": {
            "project_id": "YOUR_PROJECT_ID",
            "location": "us-central1",
            "target_url": "https://your-app.run.app/tasks/execute/",
            "oidc_service_account": "task-invoker@YOUR_PROJECT_ID.iam.gserviceaccount.com",
        },
    },
    "jobs": {
        "BACKEND": "django_tasks_google.backends.CloudRunJobsBackend",
        "QUEUES": [],
        "OPTIONS": {
            "project_id": "YOUR_PROJECT_ID",
            "location": "us-central1",
        },
    },
    "scheduler": {
        "BACKEND": "django_tasks_google.backends.CloudSchedulerBackend",
        "QUEUES": [],
        "OPTIONS": {
            "project_id": "YOUR_PROJECT_ID",
            "location": "us-central1",
            "target_url": "https://your-app.run.app/tasks/execute/",
            "oidc_service_account": "task-invoker@YOUR_PROJECT_ID.iam.gserviceaccount.com",
        },
    },
}
```

No IAM roles are needed for the `oidc_service_account`. The backend is designed
to work with public Cloud Run services by performing manual OIDC token
verification. This ensures that even though the endpoint is publicly accessible,
only requests signed by your specific service account can trigger task execution.

### 3. Register the task execution URLs

```python
# urls.py
from django.urls import path, include

urlpatterns = [
    # ...
    path('tasks/', include('django_tasks_google.urls')),
]
```

### 4. Define tasks

```python
# tasks.py
from django.tasks import task


@task(queue_name="your-cloud-task-queue")
def send_notification(user_id):
    user = User.objects.get(id=user_id)
    # ...
    return f"Notification sent to {user.email}"


@task(backend="jobs", queue_name="your-cloud-run-job")
def compute_meaning_of_life():
    # ... long running process ...
    return 42


@task(backend="scheduler")  # "queue_name" will be populated with the scheduled task job name
def send_daily_newsletter(user_id):
    user = User.objects.get(id=user_id)
    # ...
    return f"Newsletter sent to {user.email}"


```

### 5. Enqueue tasks

Enqueuing tasks e.g. `compute_meaning_of_life.enqueue()` works for both Cloud Tasks and Cloud Run Jobs.
Cloud Scheduler tasks may only be enqueued by Cloud Scheduler itself. An admin dashboard exists for
managing Cloud Scheduler tasks within Django, or you may programmatically manage them using the
`ScheduledTask` model.

```python
from django_tasks_google.scheduler import schedule_task
from django_tasks_google.models import TaskExecution, ScheduledTask

# Cloud Tasks and Cloud Run Jobs are enqueued using the builtin Django tasks API.
send_notification.enqueue(user_id=1)
task_result = compute_meaning_of_life.enqueue()

# Task results for all jobs are accessible and are saved to the `TaskExecution` model.
task_result.refresh()
task_result.return_value  # 42 (if job successfully completed)

# To access the `TaskExecution` model.
execution = TaskExecution.objects.get(pk=task_result.id)

# Only Cloud Run Job tasks can be canceled mid-run.
execution.cancel()

# Creates an instance of ScheduledTask and a job on Google Cloud Scheduler.
scheduled_task: ScheduledTask = schedule_task(send_daily_newsletter, "0 */3 * * *", args=[1])
```

Task args, kwargs, and return values must all be JSON serializable.

## More Info

Check out the [Official Django Docs](https://docs.djangoproject.com/en/6.0/topics/tasks/) for more on Django tasks.
