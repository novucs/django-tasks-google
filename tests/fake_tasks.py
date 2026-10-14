import signal
import time
from datetime import timedelta

from django.tasks import task

from django_tasks_google.base import cancel_task


@task(backend="default", queue_name="default")
def sample_task(*args, **kwargs):
    return {"args": args, "kwargs": kwargs}


@task(backend="process", queue_name="default")
def process_task(*args, **kwargs):
    return {"args": list(args), "kwargs": kwargs}


@task(backend="process", queue_name="default")
def always_fails():
    raise ValueError("boom")


# Long-running task used by the force-cancel smoke test. It does NOT check for
# cooperative cancellation - it only stops when SIGTERM raises TaskCancelledError
# inside the child process, writing a marker file from its cleanup handler so the
# test can prove the forceful path ran.
@task(backend="process", queue_name="default")
def sleepy_task(marker_path=None):
    try:
        time.sleep(30)
        return "completed"
    except BaseException:
        if marker_path:
            with open(marker_path, "w") as fh:
                fh.write("cancelled")
        raise


@task(backend="process_jobs", queue_name="default", timeout=timedelta(minutes=5))
def timed_task():
    return "completed"


@task(backend="process", queue_name="default", timeout=timedelta(minutes=5))
def timed_cloud_tasks_task():
    return "completed"


# Stands in for the platform's timeout: SIGTERMs the attempt it runs in.
@task(backend="process_jobs", queue_name="default")
def terminated_task():
    signal.raise_signal(signal.SIGTERM)


@task(backend="process_jobs", queue_name="default", takes_context=True)
def cancelled_then_terminated_task(context):
    cancel_task(context.task_result.id, force=True)
    signal.raise_signal(signal.SIGTERM)
