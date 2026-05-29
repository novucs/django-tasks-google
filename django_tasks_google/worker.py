"""Child-process entrypoint for the local ``run_tasks`` worker.

Kept free of top-level Django imports: under the "spawn" start method the child
imports this module to resolve the target function *before* ``django.setup()``
runs, so importing models here would raise ``AppRegistryNotReady``.
"""


def child_entrypoint(execution_id):
    """Run a single task attempt in a freshly spawned child process.

    The task runs in this process's main thread, so the executor's SIGTERM
    handler registers - letting the worker forcibly cancel by SIGTERM'ing the
    child. Runs exactly one attempt; retries happen by the poller re-claiming the
    still-READY row and spawning again.
    """
    import django

    django.setup()  # idempotent; required under the spawn start method

    from django.db import connections

    # Drop any connection state inherited/copied from the parent; open fresh.
    connections.close_all()

    from django_tasks_google.executor import execute_task
    from django_tasks_google.models import TaskExecution

    try:
        execution = TaskExecution.objects.get(pk=execution_id)
    except TaskExecution.DoesNotExist:
        return
    attempt = len(execution.worker_ids) + 1
    execute_task(execution_id, attempt, backend=execution.backend)
