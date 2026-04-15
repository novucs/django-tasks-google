from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import yaml
from django.db import transaction
from django.utils import timezone

from django_tasks_google.models import WorkflowDefinition

if TYPE_CHECKING:
    from django.tasks import Task


def schedule_workflow(
    name: str,
    definition: str,
    *,
    description: str = "",
    project_id: str,
    location: str = "us-central1",
    service_account: str = "",
) -> WorkflowDefinition:
    workflow_definition = WorkflowDefinition.objects.create(
        name=name,
        description=description,
        definition=definition,
        project_id=project_id,
        location=location,
        service_account=service_account,
    )
    workflow_definition.sync()
    return workflow_definition


@transaction.atomic
def sync_workflow_definition(workflow_definition_id):
    from google.api_core.exceptions import NotFound
    from google.cloud import workflows_v1
    from google.protobuf import field_mask_pb2

    workflow_definition = WorkflowDefinition.objects.select_for_update().get(
        pk=workflow_definition_id
    )
    client = workflows_v1.WorkflowsClient()
    parent = (
        f"projects/{workflow_definition.project_id}"
        f"/locations/{workflow_definition.location}"
    )
    resource_name = f"{parent}/workflows/{workflow_definition.name}"

    workflow = workflows_v1.Workflow(  # type: ignore
        name=resource_name,
        description=workflow_definition.description,
        source_contents=workflow_definition.definition,
        service_account=workflow_definition.service_account or None,
    )

    workflow_exists = False
    try:
        client.get_workflow(name=resource_name)
        workflow_exists = True
    except NotFound:
        pass

    if workflow_exists:
        update_mask = field_mask_pb2.FieldMask(
            paths=["source_contents", "description", "service_account"]
        )
        operation = client.update_workflow(workflow=workflow, update_mask=update_mask)
    else:
        operation = client.create_workflow(
            parent=parent,
            workflow=workflow,
            workflow_id=workflow_definition.name,
        )
    operation.result()

    workflow_definition.synced_at = timezone.now()
    update_fields = ["synced_at"]
    if workflow_definition.cloud_workflow_resource_name != resource_name:
        workflow_definition.cloud_workflow_resource_name = resource_name
        update_fields.append("cloud_workflow_resource_name")
    workflow_definition.save(update_fields=update_fields)


def delete_workflow(workflow_definition_id):
    with transaction.atomic():
        workflow_definition = WorkflowDefinition.objects.select_for_update().get(
            pk=workflow_definition_id
        )
        resource_name = workflow_definition.cloud_workflow_resource_name
        workflow_definition.delete()
    delete_cloud_workflow_if_exists(resource_name)


def delete_cloud_workflow_if_exists(resource_name: str | None = None):
    from google.api_core.exceptions import NotFound
    from google.cloud import workflows_v1

    if not resource_name:
        return
    client = workflows_v1.WorkflowsClient()
    try:
        operation = client.delete_workflow(name=resource_name)
        operation.result()
    except NotFound:
        pass


_SUBWORKFLOW_NAME_RE = re.compile(r"[^a-zA-Z0-9_]")


def task_subworkflow_yaml(
    task: Task,
    *,
    enqueue_url: str,
    oidc_audience: str | None = None,
    subworkflow_name: str | None = None,
    timeout_seconds: int = 3600,
) -> str:
    """Generate a Cloud Workflows definition that runs a single Django task.

    The returned YAML has:
      * A ``main`` entrypoint that takes ``{task_args, task_kwargs}`` and calls
        the subworkflow.
      * A subworkflow that creates a callback endpoint, POSTs to the Django
        ``/tasks/enqueue/`` endpoint with the task path + args/kwargs +
        callback URL, waits on the callback, and returns the task's
        ``return_value``.

    ``task_path``, ``enqueue_url``, and ``oidc_audience`` are interpolated as
    literals. ``task_args``/``task_kwargs`` are passed through as Cloud
    Workflows expressions so they survive JSON serialisation.
    """
    if oidc_audience is None:
        parsed = urlparse(enqueue_url)
        oidc_audience = f"{parsed.scheme}://{parsed.netloc}"

    if subworkflow_name is None:
        leaf = task.module_path.rsplit(".", 1)[-1]
        subworkflow_name = _SUBWORKFLOW_NAME_RE.sub("_", leaf) or "run_task"

    doc = {
        "main": {
            "params": ["args"],
            "steps": [
                {
                    "run_task_step": {
                        "call": subworkflow_name,
                        "args": {
                            "task_args": '${default(map.get(args, "task_args"), [])}',
                            "task_kwargs": (
                                '${default(map.get(args, "task_kwargs"), {})}'
                            ),
                        },
                        "result": "task_outcome",
                    }
                },
                {"return_outcome": {"return": "${task_outcome}"}},
            ],
        },
        subworkflow_name: {
            "params": ["task_args", "task_kwargs"],
            "steps": [
                {
                    "create_callback": {
                        "call": "events.create_callback_endpoint",
                        "args": {"http_callback_method": "POST"},
                        "result": "callback_details",
                    }
                },
                {
                    "enqueue_task": {
                        "call": "http.post",
                        "args": {
                            "url": enqueue_url,
                            "auth": {"type": "OIDC", "audience": oidc_audience},
                            "headers": {"Content-Type": "application/json"},
                            "body": {
                                "task_path": task.module_path,
                                "args": "${task_args}",
                                "kwargs": "${task_kwargs}",
                                "callback_url": "${callback_details.url}",
                            },
                        },
                        "result": "enqueue_response",
                    }
                },
                {
                    "await_callback": {
                        "call": "events.await_callback",
                        "args": {
                            "callback": "${callback_details}",
                            "timeout": timeout_seconds,
                        },
                        "result": "task_outcome",
                    }
                },
                {
                    "check_result": {
                        "switch": [
                            {
                                "condition": (
                                    "${task_outcome.http_request.body.status"
                                    ' == "failed"}'
                                ),
                                "raise": "${task_outcome.http_request.body}",
                            }
                        ]
                    }
                },
                {
                    "return_result": {
                        "return": "${task_outcome.http_request.body.return_value}"
                    }
                },
            ],
        },
    }
    return yaml.safe_dump(doc, sort_keys=False)


def chained_workflow_yaml(
    steps: list[dict],
    *,
    workflow_name: str = "chained-tasks",
) -> str:
    """Generate a Cloud Workflows definition that runs subworkflows in sequence.

    Each ``step`` is a dict of ``{"name": str, "subworkflow": str}``. The
    result of each step is passed as ``previous_result`` to the next.
    """
    del workflow_name  # currently only used as a convention hint for callers
    rendered_steps = []
    for i, step in enumerate(steps):
        result_var = f"result_{i}"
        previous_expr = f"${{result_{i - 1}}}" if i > 0 else "${null}"
        rendered_steps.append(
            {
                step["name"]: {
                    "call": step["subworkflow"],
                    "args": {
                        "task_args": "${args.task_args}",
                        "task_kwargs": "${args.task_kwargs}",
                        "previous_result": previous_expr,
                    },
                    "result": result_var,
                }
            }
        )
    last_result = f"${{result_{len(steps) - 1}}}" if steps else "${null}"
    rendered_steps.append({"return_final": {"return": last_result}})

    doc = {
        "main": {
            "params": ["args"],
            "steps": rendered_steps,
        },
    }
    return yaml.safe_dump(doc, sort_keys=False)
