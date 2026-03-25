import logging
from traceback import format_exception

from django.db import close_old_connections
from django.tasks import task_backends
from django.tasks.base import TaskContext, TaskResultStatus
from django.tasks.signals import task_finished, task_started
from django.utils import timezone

from django_tasks_google.models import TaskExecution

logger = logging.getLogger("django_tasks_google")


def execute_task(execution: TaskExecution):
    worker_id = timezone.now().isoformat()

    logger.info(f"Running {execution}")
    sender = type(task_backends[execution.backend])
    execution.last_attempted_at = timezone.now()
    execution.status = TaskResultStatus.RUNNING
    execution.worker_ids.append(worker_id)

    if not execution.started_at:
        execution.started_at = execution.last_attempted_at
        execution.save()
        task_started.send(sender=sender, task_result=execution.task_result)
    else:
        execution.save()

    try:
        args = execution.args
        if execution.takes_context:
            args = (TaskContext(task_result=execution.task_result), *args)
        return_value = execution.task.call(*args, **execution.kwargs)

        close_old_connections()
        execution.finished_at = timezone.now()
        execution.status = TaskResultStatus.SUCCESSFUL
        execution.return_value = return_value
        execution.save()
        logger.info(f"Completed {execution}")
        task_finished.send(sender=sender, task_result=execution.task_result)
        return True

    except Exception as e:
        close_old_connections()
        execution.finished_at = timezone.now()
        execution.status = TaskResultStatus.FAILED
        exception_type = type(e)
        execution.errors.append(
            {
                "exception_class_path": f"{exception_type.__module__}.{exception_type.__qualname__}",
                "traceback": "".join(format_exception(e)),
            }
        )
        execution.save()
        logger.exception(f"An error occurred during {execution}")
        task_finished.send(sender=sender, task_result=execution.task_result)
        return False
