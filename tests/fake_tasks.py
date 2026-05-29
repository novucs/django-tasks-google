import time

from django.tasks import task


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
