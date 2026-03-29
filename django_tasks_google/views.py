import logging

from django.db import transaction
from django.http import (
    HttpResponse,
    HttpResponseBadRequest,
    HttpResponseNotFound,
    HttpResponseServerError,
)
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from django_tasks_google.executor import execute_task
from django_tasks_google.forms import ExecuteTaskForm, ScheduleTaskForm
from django_tasks_google.models import ScheduledTask

logger = logging.getLogger("django_tasks_google")


@require_POST
@csrf_exempt
@transaction.non_atomic_requests  # avoid holding a DB transaction open during execution
def execute_task_view(request):
    from django_tasks_google.auth import handle_oidc_auth

    form = ExecuteTaskForm(request.POST)
    if not form.is_valid():
        logger.warning("Invalid ExecuteTaskForm submission: %s", form.errors.as_json())
        return HttpResponseBadRequest()

    execution_id = form.cleaned_data["execution_id"]
    backend = form.cleaned_data["backend"]
    authenticated, auth_status, auth_error = handle_oidc_auth(
        request, backend.oidc_audience, backend.oidc_service_account
    )
    if not authenticated:
        logger.warning("Failed to auth execution of %s: %s", execution_id, auth_error)
        return HttpResponse(status=auth_status)

    try:
        # X-CloudTasks-TaskRetryCount is 0-indexed.
        attempt = int(request.headers.get("X-CloudTasks-TaskRetryCount")) + 1
    except (TypeError, ValueError):
        logger.exception("X-CloudTasks-TaskRetryCount must be an integer")
        return HttpResponseBadRequest()

    should_retry = execute_task(execution_id, attempt, backend=backend)
    if should_retry:
        logger.info("Task %s requested retry", execution_id)
        return HttpResponseServerError()

    return HttpResponse(status=204)


@require_POST
@csrf_exempt
@transaction.atomic
def schedule_task_view(request):
    from django_tasks_google.auth import handle_oidc_auth

    form = ScheduleTaskForm(request.POST)
    if not form.is_valid():
        logger.warning("Invalid ScheduleTaskForm submission: %s", form.errors.as_json())
        return HttpResponseBadRequest()

    task_id = form.cleaned_data["task_id"]
    backend = form.cleaned_data["backend"]
    authenticated, auth_status, auth_error = handle_oidc_auth(
        request, backend.oidc_audience, backend.oidc_service_account
    )
    if not authenticated:
        logger.warning("Failed to auth scheduling of %s: %s", task_id, auth_error)
        return HttpResponse(status=auth_status)

    job_name = request.headers.get("X-CloudScheduler-JobName")
    schedule_time = request.headers.get("X-CloudScheduler-ScheduleTime")
    if not job_name:
        logger.warning("X-CloudScheduler-JobName header was not set")
        return HttpResponseBadRequest()
    if not schedule_time:
        logger.warning("X-CloudScheduler-ScheduleTime header was not set")
        return HttpResponseBadRequest()
    idempotency_key = f"{job_name}:{schedule_time}"

    try:
        task = ScheduledTask.objects.select_for_update().get(pk=task_id)
    except ScheduledTask.DoesNotExist:
        logger.warning("Could not find scheduled task: %s", task_id)
        return HttpResponseNotFound()

    if task.backend.alias != backend.alias:
        logger.warning(
            "Requested wrong backend alias task=%s requested=%s expected=%s",
            task_id,
            task.backend.alias,
            backend.alias,
        )
        return HttpResponseNotFound()

    if task.idempotency_key == idempotency_key:
        logger.warning("Prevented duplicate task execution for task: %s", task_id)
        return HttpResponse(status=204)

    task.idempotency_key = idempotency_key
    task.save(update_fields=["idempotency_key"])
    task.enqueue()
    return HttpResponse(status=204)
