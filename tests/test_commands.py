import os
from unittest.mock import patch

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError


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
