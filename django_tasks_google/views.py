import json
import logging

from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.http import (
    HttpResponse,
    HttpResponseBadRequest,
    HttpResponseNotFound,
    HttpResponseServerError,
)
from django.tasks import InvalidTaskBackend, task_backends
from django.utils.dateparse import parse_datetime
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from django_tasks_google.executor import execute_task
from django_tasks_google.models import ScheduledTask

logger = logging.getLogger("django_tasks_google")


def handle_oidc_auth(request, backend):
    from google.auth.exceptions import GoogleAuthError
    from google.auth.transport import requests as google_requests
    from google.oauth2 import id_token

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise PermissionDenied("Missing or invalid Authorization header")

    token = auth_header[7:]
    try:
        # verify_oauth2_token checks the issuer for us
        claims = id_token.verify_oauth2_token(
            token, google_requests.Request(), audience=backend.oidc_audience
        )
        if claims.get("email") != backend.oidc_service_account:
            raise PermissionDenied(f"Unexpected caller email: {claims.get('email')}")
        if not claims.get("email_verified"):
            raise PermissionDenied("Caller email is not verified")
    except (ValueError, GoogleAuthError) as e:
        logger.warning("OIDC token verification failed: %s", e)
        raise PermissionDenied("OIDC token verification failed")


@require_POST
@csrf_exempt
@transaction.non_atomic_requests  # avoid holding a DB transaction open during execution
def execute_task_view(request):
    try:
        data = json.loads(request.body)
        backend = task_backends[data["backend_alias"]]
        execution_id = data["execution_id"]
    except (json.JSONDecodeError, InvalidTaskBackend, KeyError):
        return HttpResponseBadRequest()
    handle_oidc_auth(request, backend)
    retry = execute_task(execution_id)
    if retry:
        logger.info("Task %s requested retry", execution_id)
        return HttpResponseServerError()
    return HttpResponse(status=204)


@require_POST
@csrf_exempt
@transaction.atomic
def schedule_task_view(request):
    try:
        data = json.loads(request.body)
        backend = task_backends[data["backend_alias"]]
        task_id = data["task_id"]
        schedule_time = parse_datetime(request.headers["X-CloudScheduler-ScheduleTime"])
    except (json.JSONDecodeError, InvalidTaskBackend, KeyError, ValueError, TypeError):
        return HttpResponseBadRequest()

    if schedule_time is None:
        return HttpResponseBadRequest()

    handle_oidc_auth(request, backend)

    try:
        task = ScheduledTask.objects.select_for_update().get(pk=task_id)
    except ScheduledTask.DoesNotExist:
        return HttpResponseNotFound()

    if task.last_scheduled_at == schedule_time:
        logger.warning("Prevented duplicate task execution for task %s", task)
        return HttpResponse(status=204)

    task.last_scheduled_at = schedule_time
    task.save(update_fields=["last_scheduled_at"])
    task.enqueue()
    return HttpResponse(status=204)
