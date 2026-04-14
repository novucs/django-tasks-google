import os
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import requests
from django.core.management import call_command
from django.core.management.base import CommandError
from django.tasks.base import TaskResultStatus
from django.utils import timezone

from django_tasks_google.models import TaskExecution


def test_execute_task_command_raises_when_execution_returns_true():
    with patch.dict(os.environ, {"CLOUD_RUN_TASK_ATTEMPT": "0"}):
        with patch(
            "django_tasks_google.management.commands.execute_task.execute_task"
        ) as run_mock:
            run_mock.return_value = True
            with pytest.raises(CommandError, match=r"Task id=123 retry requested"):
                call_command("execute_task", "123")


def test_execute_task_command_succeeds_when_execution_returns_false():
    with patch.dict(os.environ, {"CLOUD_RUN_TASK_ATTEMPT": "0"}):
        with patch(
            "django_tasks_google.management.commands.execute_task.execute_task"
        ) as run_mock:
            run_mock.return_value = False
            call_command("execute_task", "123")
    run_mock.assert_called_once_with("123", 1)


def _create_finished_execution(
    *, callback_url, status=TaskResultStatus.SUCCESSFUL, finished_at=None, **kwargs
):
    return TaskExecution.objects.create(
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        args=[],
        kwargs={},
        status=status,
        finished_at=finished_at or timezone.now(),
        callback_url=callback_url,
        **kwargs,
    )


@pytest.mark.django_db
def test_retry_failed_workflow_callbacks_delivers_pending_callback():
    execution = _create_finished_execution(
        callback_url="https://workflowexecutions.googleapis.com/v1/cb/1",
    )

    response = SimpleNamespace(raise_for_status=lambda: None)
    with patch(
        "django_tasks_google.auth.post_with_oidc",
        return_value=response,
    ) as post_mock:
        call_command("retry_failed_workflow_callbacks")

    post_mock.assert_called_once()
    execution.refresh_from_db()
    assert execution.callback_delivered_at is not None


@pytest.mark.django_db
def test_retry_failed_workflow_callbacks_skips_already_delivered():
    _create_finished_execution(
        callback_url="https://workflowexecutions.googleapis.com/v1/cb/2",
        callback_delivered_at=timezone.now(),
    )

    with patch("django_tasks_google.auth.post_with_oidc") as post_mock:
        call_command("retry_failed_workflow_callbacks")

    post_mock.assert_not_called()


@pytest.mark.django_db
def test_retry_failed_workflow_callbacks_skips_non_terminal_executions():
    TaskExecution.objects.create(
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        args=[],
        kwargs={},
        status=TaskResultStatus.RUNNING,
        callback_url="https://workflowexecutions.googleapis.com/v1/cb/3",
    )

    with patch("django_tasks_google.auth.post_with_oidc") as post_mock:
        call_command("retry_failed_workflow_callbacks")

    post_mock.assert_not_called()


@pytest.mark.django_db
def test_retry_failed_workflow_callbacks_respects_max_age_hours():
    _create_finished_execution(
        callback_url="https://workflowexecutions.googleapis.com/v1/cb/4",
        finished_at=timezone.now() - timedelta(hours=48),
    )

    with patch("django_tasks_google.auth.post_with_oidc") as post_mock:
        call_command("retry_failed_workflow_callbacks", "--max-age-hours=24")

    post_mock.assert_not_called()


@pytest.mark.django_db
def test_retry_failed_workflow_callbacks_respects_limit():
    for i in range(5):
        _create_finished_execution(
            callback_url=f"https://workflowexecutions.googleapis.com/v1/cb/limit-{i}",
        )

    response = SimpleNamespace(raise_for_status=lambda: None)
    with patch(
        "django_tasks_google.auth.post_with_oidc",
        return_value=response,
    ) as post_mock:
        call_command("retry_failed_workflow_callbacks", "--limit=2")

    assert post_mock.call_count == 2


@pytest.mark.django_db
def test_retry_failed_workflow_callbacks_dry_run():
    execution = _create_finished_execution(
        callback_url="https://workflowexecutions.googleapis.com/v1/cb/dry",
    )

    with patch("django_tasks_google.auth.post_with_oidc") as post_mock:
        call_command("retry_failed_workflow_callbacks", "--dry-run")

    post_mock.assert_not_called()
    execution.refresh_from_db()
    assert execution.callback_delivered_at is None


@pytest.mark.django_db
def test_retry_failed_workflow_callbacks_handles_delivery_failure():
    execution = _create_finished_execution(
        callback_url="https://workflowexecutions.googleapis.com/v1/cb/fail",
    )

    with patch(
        "django_tasks_google.auth.post_with_oidc",
        side_effect=requests.RequestException("boom"),
    ):
        call_command("retry_failed_workflow_callbacks")

    execution.refresh_from_db()
    assert execution.callback_delivered_at is None
