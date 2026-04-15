from unittest.mock import MagicMock, patch

import pytest
import yaml
from google.api_core.exceptions import NotFound

from django_tasks_google.models import WorkflowDefinition
from django_tasks_google.workflow import (
    chained_workflow_yaml,
    delete_cloud_workflow_if_exists,
    delete_workflow,
    schedule_workflow,
    sync_workflow_definition,
    task_subworkflow_yaml,
)
from tests.fake_tasks import sample_task


@pytest.mark.django_db
def test_schedule_workflow_creates_model_and_syncs():
    with patch.object(WorkflowDefinition, "sync", autospec=True) as sync_mock:
        workflow_definition = schedule_workflow(
            name="notify-workflow",
            definition="main:\n  steps:\n    - noop:\n        return: ok\n",
            description="test",
            project_id="test-project",
            location="us-central1",
            service_account="sa@test.iam.gserviceaccount.com",
        )

    workflow_definition.refresh_from_db()
    assert workflow_definition.name == "notify-workflow"
    assert workflow_definition.description == "test"
    assert workflow_definition.project_id == "test-project"
    assert workflow_definition.location == "us-central1"
    assert workflow_definition.service_account == "sa@test.iam.gserviceaccount.com"
    sync_mock.assert_called_once_with(workflow_definition)


@pytest.mark.django_db
def test_workflow_definition_sync_delegates_to_sync_workflow_definition():
    workflow_definition = WorkflowDefinition.objects.create(
        name="w1",
        definition="main:\n  steps: []",
        project_id="test-project",
    )
    with patch(
        "django_tasks_google.workflow.sync_workflow_definition"
    ) as sync_one_mock:
        workflow_definition.sync()
    sync_one_mock.assert_called_once_with(workflow_definition.pk)


@pytest.mark.django_db
def test_sync_workflow_definition_creates_workflow_when_not_found():
    workflow_definition = WorkflowDefinition.objects.create(
        name="new-workflow",
        definition="main:\n  steps: []",
        project_id="test-project",
        location="us-central1",
        service_account="sa@test.iam.gserviceaccount.com",
    )

    with patch("google.cloud.workflows_v1.WorkflowsClient") as client_cls:
        client = client_cls.return_value
        client.get_workflow.side_effect = NotFound("missing")
        operation = MagicMock()
        client.create_workflow.return_value = operation

        sync_workflow_definition(workflow_definition.pk)

    client.create_workflow.assert_called_once()
    call_kwargs = client.create_workflow.call_args.kwargs
    assert call_kwargs["parent"] == "projects/test-project/locations/us-central1"
    assert call_kwargs["workflow_id"] == "new-workflow"
    assert (
        call_kwargs["workflow"].name
        == "projects/test-project/locations/us-central1/workflows/new-workflow"
    )
    assert call_kwargs["workflow"].source_contents == workflow_definition.definition
    client.update_workflow.assert_not_called()
    operation.result.assert_called_once()

    workflow_definition.refresh_from_db()
    assert (
        workflow_definition.cloud_workflow_resource_name
        == "projects/test-project/locations/us-central1/workflows/new-workflow"
    )
    assert workflow_definition.synced_at is not None


@pytest.mark.django_db
def test_sync_workflow_definition_updates_existing_workflow():
    workflow_definition = WorkflowDefinition.objects.create(
        name="existing-workflow",
        definition="main:\n  steps: []",
        project_id="test-project",
        location="us-central1",
    )

    with patch("google.cloud.workflows_v1.WorkflowsClient") as client_cls:
        client = client_cls.return_value
        client.get_workflow.return_value = object()
        operation = MagicMock()
        client.update_workflow.return_value = operation

        sync_workflow_definition(workflow_definition.pk)

    client.create_workflow.assert_not_called()
    client.update_workflow.assert_called_once()
    call_kwargs = client.update_workflow.call_args.kwargs
    assert set(call_kwargs["update_mask"].paths) == {
        "source_contents",
        "description",
        "service_account",
    }
    operation.result.assert_called_once()


@pytest.mark.django_db
def test_delete_workflow_deletes_db_and_cloud_resource():
    workflow_definition = WorkflowDefinition.objects.create(
        name="to-delete",
        definition="main:\n  steps: []",
        project_id="test-project",
        location="us-central1",
        cloud_workflow_resource_name=(
            "projects/test-project/locations/us-central1/workflows/to-delete"
        ),
    )

    with patch("google.cloud.workflows_v1.WorkflowsClient") as client_cls:
        client = client_cls.return_value
        operation = MagicMock()
        client.delete_workflow.return_value = operation

        delete_workflow(workflow_definition.pk)

    assert not WorkflowDefinition.objects.filter(pk=workflow_definition.pk).exists()
    client.delete_workflow.assert_called_once_with(
        name="projects/test-project/locations/us-central1/workflows/to-delete"
    )
    operation.result.assert_called_once()


def test_delete_cloud_workflow_if_exists_ignores_missing_name():
    with patch("google.cloud.workflows_v1.WorkflowsClient") as client_cls:
        delete_cloud_workflow_if_exists(None)
    client_cls.assert_not_called()


def test_delete_cloud_workflow_if_exists_swallows_not_found():
    with patch("google.cloud.workflows_v1.WorkflowsClient") as client_cls:
        client = client_cls.return_value
        client.delete_workflow.side_effect = NotFound("missing")
        delete_cloud_workflow_if_exists("projects/p/locations/l/workflows/w")
    client.delete_workflow.assert_called_once()


def test_task_subworkflow_yaml_targets_enqueue_endpoint():
    output = task_subworkflow_yaml(
        sample_task,
        enqueue_url="https://app.run.app/tasks/enqueue/",
    )
    assert "enqueue/" in output
    assert "execute/" not in output


def test_task_subworkflow_yaml_uses_literal_task_path_and_expression_args():
    output = task_subworkflow_yaml(
        sample_task,
        enqueue_url="https://app.run.app/tasks/enqueue/",
    )
    doc = yaml.safe_load(output)
    subworkflow = doc["sample_task"]
    enqueue_step = next(
        step["enqueue_task"] for step in subworkflow["steps"] if "enqueue_task" in step
    )
    body = enqueue_step["args"]["body"]
    assert body["task_path"] == "tests.fake_tasks.sample_task"
    assert body["args"] == "${task_args}"
    assert body["kwargs"] == "${task_kwargs}"
    assert body["callback_url"] == "${callback_details.url}"


def test_task_subworkflow_yaml_defaults_audience_to_origin():
    output = task_subworkflow_yaml(
        sample_task,
        enqueue_url="https://app.run.app/tasks/enqueue/",
    )
    doc = yaml.safe_load(output)
    subworkflow = doc["sample_task"]
    enqueue_step = next(
        step["enqueue_task"] for step in subworkflow["steps"] if "enqueue_task" in step
    )
    assert enqueue_step["args"]["auth"] == {
        "type": "OIDC",
        "audience": "https://app.run.app",
    }


def test_task_subworkflow_yaml_parses_as_valid_yaml_with_main_and_sub():
    output = task_subworkflow_yaml(
        sample_task,
        enqueue_url="https://app.run.app/tasks/enqueue/",
    )
    doc = yaml.safe_load(output)
    assert "main" in doc
    assert "sample_task" in doc


def test_task_subworkflow_yaml_applies_custom_timeout():
    output = task_subworkflow_yaml(
        sample_task,
        enqueue_url="https://app.run.app/tasks/enqueue/",
        timeout_seconds=7200,
    )
    doc = yaml.safe_load(output)
    await_step = next(
        step["await_callback"]
        for step in doc["sample_task"]["steps"]
        if "await_callback" in step
    )
    assert await_step["args"]["timeout"] == 7200


def test_chained_workflow_yaml_single_step_renders_previous_result_null():
    output = chained_workflow_yaml([{"name": "step1", "subworkflow": "task_a"}])
    doc = yaml.safe_load(output)
    steps = doc["main"]["steps"]
    first = steps[0]["step1"]
    assert first["args"]["previous_result"] == "${null}"


def test_chained_workflow_yaml_multi_step_passes_previous_result():
    output = chained_workflow_yaml(
        [
            {"name": "step1", "subworkflow": "task_a"},
            {"name": "step2", "subworkflow": "task_b"},
        ]
    )
    doc = yaml.safe_load(output)
    steps = doc["main"]["steps"]
    second = steps[1]["step2"]
    assert second["args"]["previous_result"] == "${result_0}"
    final = steps[2]["return_final"]
    assert final["return"] == "${result_1}"
