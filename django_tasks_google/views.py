import json
import logging
from json import JSONDecodeError

from django.db import transaction
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.tasks import InvalidTaskBackend, task_backends
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from django_tasks_google.executor import execute_task
from django_tasks_google.models import ScheduledTask, TaskExecution

logger = logging.getLogger("django_tasks_google")


def handle_oidc_auth(request, audience, email):
    from google.auth.transport import requests as google_requests
    from google.oauth2 import id_token

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return False, "Missing or invalid Authorization header"

    token = auth_header[7:]
    try:
        claims = id_token.verify_oauth2_token(
            token, google_requests.Request(), audience=audience
        )
        if claims.get("email") != email:
            return False, f"Unexpected caller email: {claims.get('email')}"
        if claims.get("email_verified") is not True:
            return False, "Caller email is not verified"
        return True, None
    except Exception as e:
        logger.error(f"OIDC token verification failed: {e}")
        return False, str(e)


@require_POST
@csrf_exempt
@transaction.non_atomic_requests
def execute_task_view(request):
    try:
        data = json.loads(request.body)
        backend = task_backends[data["backend"]]
    except (JSONDecodeError, KeyError, InvalidTaskBackend) as e:
        logger.warning(f"Cannot verify backend: {e}")
        return JsonResponse(
            {
                "error": "Unauthorized",
                "detail": f"Cannot verify backend: {request.body}",
            },
            status=401,
        )
    is_valid, error_message = handle_oidc_auth(
        request,
        audience=backend.target_url,
        email=backend.oidc_service_account,
    )
    if not is_valid:
        logger.warning(f"Authentication failed: {error_message}")
        return JsonResponse(
            {"error": "Unauthorized", "detail": error_message},
            status=401,
        )

    if request.headers.get("X-CloudScheduler"):
        job_name = request.headers["X-CloudScheduler-JobName"]
        schedule_time = request.headers["X-CloudScheduler-ScheduleTime"]
        cloud_scheduler_idempotency_key = f"{job_name}:{schedule_time}"
        execution = TaskExecution.objects.filter(
            cloud_scheduler_idempotency_key=cloud_scheduler_idempotency_key,
        ).first()
        if not execution:
            task = get_object_or_404(
                ScheduledTask, name=job_name, backend=backend.alias
            )
            execution = TaskExecution.objects.create(
                module_path=task.module_path,
                backend=task.backend,
                queue_name=task.name,
                takes_context=task.takes_context,
                args=task.args,
                kwargs=task.kwargs,
                cloud_scheduler_idempotency_key=cloud_scheduler_idempotency_key,
            )
    else:
        execution = get_object_or_404(
            TaskExecution, pk=data["task_execution_id"], backend=backend.alias
        )

    ok = execute_task(execution.pk)
    return JsonResponse({"ok": ok}, status=200 if ok else 500)
