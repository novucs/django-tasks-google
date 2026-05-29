from types import SimpleNamespace
from unittest.mock import patch

import pytest
from django.contrib import admin, messages
from django.tasks import TaskResultStatus
from django.test import RequestFactory

from django_tasks_google.admin import ScheduledTaskAdmin, TaskExecutionAdmin
from django_tasks_google.models import ScheduledTask, TaskExecution


@pytest.fixture
def http_request():
    return RequestFactory().get("/admin/")


@pytest.fixture
def admin_site():
    return admin.sites.AdminSite()


@pytest.fixture
def scheduled_task():
    return ScheduledTask.objects.create(
        name="daily-job",
        schedule="0 * * * *",
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
    )


@pytest.fixture
def task_execution():
    return TaskExecution.objects.create(
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        status=TaskResultStatus.READY.value,
    )


@pytest.mark.django_db
def test_sync_tasks_success_message(admin_site, http_request, scheduled_task):
    model_admin = ScheduledTaskAdmin(ScheduledTask, admin_site)

    with (
        patch.object(ScheduledTask, "sync", autospec=True) as sync_mock,
        patch.object(model_admin, "message_user", autospec=True) as message_user_mock,
    ):
        model_admin.sync_tasks(
            http_request, ScheduledTask.objects.filter(pk=scheduled_task.pk)
        )

    sync_mock.assert_called_once()
    message_user_mock.assert_called_once()
    call_args = message_user_mock.call_args.args
    message = call_args[-2]
    level = call_args[-1]
    assert "Successfully synced" in message
    assert level == messages.SUCCESS


@pytest.mark.django_db
def test_sync_tasks_failure_message(admin_site, http_request, scheduled_task):
    model_admin = ScheduledTaskAdmin(ScheduledTask, admin_site)

    with (
        patch.object(
            ScheduledTask, "sync", autospec=True, side_effect=RuntimeError("boom")
        ),
        patch("django_tasks_google.admin.logger.exception") as log_exception_mock,
        patch.object(model_admin, "message_user", autospec=True) as message_user_mock,
    ):
        model_admin.sync_tasks(
            http_request, ScheduledTask.objects.filter(pk=scheduled_task.pk)
        )

    log_exception_mock.assert_called_once()
    message_user_mock.assert_called_once()
    call_args = message_user_mock.call_args.args
    message = call_args[-2]
    level = call_args[-1]
    assert "Failed to sync" in message
    assert level == messages.ERROR


@pytest.mark.django_db
def test_save_model_warns_when_sync_fails(admin_site, http_request):
    model_admin = ScheduledTaskAdmin(ScheduledTask, admin_site)
    task = ScheduledTask(
        name="weekly-job",
        schedule="0 0 * * 0",
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
    )
    form = SimpleNamespace()

    with (
        patch.object(
            ScheduledTask, "sync", autospec=True, side_effect=ValueError("nope")
        ),
        patch("django_tasks_google.admin.logger.exception") as log_exception_mock,
        patch.object(model_admin, "message_user", autospec=True) as message_user_mock,
    ):
        model_admin.save_model(http_request, task, form, change=False)

    task.refresh_from_db()
    log_exception_mock.assert_called_once()
    message_user_mock.assert_called_once()
    call_args = message_user_mock.call_args.args
    message = call_args[-2]
    level = call_args[-1]
    assert "Model saved but sync failed" in message
    assert level == messages.WARNING


@pytest.mark.django_db
def test_delete_model_attempts_cloud_scheduler_cleanup(
    admin_site, http_request, scheduled_task
):
    model_admin = ScheduledTaskAdmin(ScheduledTask, admin_site)
    scheduled_task.cloud_scheduler_job_name = "projects/p/locations/l/jobs/j1"
    scheduled_task.save(update_fields=["cloud_scheduler_job_name"])
    task_id = scheduled_task.pk

    with patch(
        "django_tasks_google.admin.delete_cloud_scheduler_job_if_exists"
    ) as cleanup_mock:
        model_admin.delete_model(http_request, scheduled_task)

    cleanup_mock.assert_called_once_with("projects/p/locations/l/jobs/j1")
    assert not ScheduledTask.objects.filter(pk=task_id).exists()


@pytest.mark.django_db
def test_delete_model_warns_when_cleanup_fails(
    admin_site, http_request, scheduled_task
):
    model_admin = ScheduledTaskAdmin(ScheduledTask, admin_site)
    scheduled_task.cloud_scheduler_job_name = "projects/p/locations/l/jobs/j1"
    scheduled_task.save(update_fields=["cloud_scheduler_job_name"])

    with (
        patch(
            "django_tasks_google.admin.delete_cloud_scheduler_job_if_exists",
            side_effect=RuntimeError("cannot delete"),
        ),
        patch("django_tasks_google.admin.logger.exception") as log_exception_mock,
        patch.object(model_admin, "message_user", autospec=True) as message_user_mock,
    ):
        model_admin.delete_model(http_request, scheduled_task)

    log_exception_mock.assert_called_once()
    message_user_mock.assert_called_once()
    call_args = message_user_mock.call_args.args
    message = call_args[-2]
    level = call_args[-1]
    assert "Cloud Scheduler deletion failed" in message
    assert level == messages.WARNING


def test_task_execution_admin_is_read_only(admin_site):
    model_admin = TaskExecutionAdmin(TaskExecution, admin_site)
    request = RequestFactory().get("/admin/")

    assert model_admin.has_add_permission(request) is False

    readonly = set(model_admin.get_readonly_fields(request))
    field_names = {f.name for f in TaskExecution._meta.fields}
    assert field_names <= readonly


@pytest.mark.django_db
def test_cancel_executions_success_message(admin_site, http_request, task_execution):
    model_admin = TaskExecutionAdmin(TaskExecution, admin_site)

    with (
        patch.object(TaskExecution, "cancel", autospec=True) as cancel_mock,
        patch.object(model_admin, "message_user", autospec=True) as message_user_mock,
    ):
        model_admin.cancel_executions(
            http_request, TaskExecution.objects.filter(pk=task_execution.pk)
        )

    cancel_mock.assert_called_once()
    message_user_mock.assert_called_once()
    call_args = message_user_mock.call_args.args
    message = call_args[-2]
    level = call_args[-1]
    assert "Cancelled 1 execution(s)" in message
    assert level == messages.SUCCESS


@pytest.mark.django_db
def test_cancel_executions_skips_finished(admin_site, http_request, task_execution):
    model_admin = TaskExecutionAdmin(TaskExecution, admin_site)
    task_execution.status = TaskResultStatus.SUCCESSFUL.value
    task_execution.save(update_fields=["status"])

    with (
        patch.object(TaskExecution, "cancel", autospec=True) as cancel_mock,
        patch.object(model_admin, "message_user", autospec=True) as message_user_mock,
    ):
        model_admin.cancel_executions(
            http_request, TaskExecution.objects.filter(pk=task_execution.pk)
        )

    cancel_mock.assert_not_called()
    message_user_mock.assert_called_once()
    call_args = message_user_mock.call_args.args
    message = call_args[-2]
    level = call_args[-1]
    assert "already-finished" in message
    assert level == messages.WARNING


@pytest.mark.django_db
def test_cancel_executions_failure_message(admin_site, http_request, task_execution):
    model_admin = TaskExecutionAdmin(TaskExecution, admin_site)

    with (
        patch.object(
            TaskExecution, "cancel", autospec=True, side_effect=RuntimeError("boom")
        ),
        patch("django_tasks_google.admin.logger.exception") as log_exception_mock,
        patch.object(model_admin, "message_user", autospec=True) as message_user_mock,
    ):
        model_admin.cancel_executions(
            http_request, TaskExecution.objects.filter(pk=task_execution.pk)
        )

    log_exception_mock.assert_called_once()
    message_user_mock.assert_called_once()
    call_args = message_user_mock.call_args.args
    message = call_args[-2]
    level = call_args[-1]
    assert "Failed to cancel" in message
    assert level == messages.ERROR
