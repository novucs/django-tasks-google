from django.tasks import task


@task(backend="default", queue_name="default")
def sample_task(*args, **kwargs):
    return {"args": args, "kwargs": kwargs}
