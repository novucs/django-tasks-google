from unittest.mock import patch

import pytest
from google.api_core.exceptions import NotFound

from django_tasks_google.models import ScheduledTask
from django_tasks_google.scheduler import (
    delete_cloud_scheduler_job_if_exists,
    schedule_task,
    sync_scheduled_task,
)
from tests.fake_tasks import sample_task


class _FakeJob:
    class State:
        ENABLED = "ENABLED"
        DISABLED = "DISABLED"

    def __init__(self, state):
        self.state = state


@pytest.mark.django_db
def test_schedule_task_creates_model_and_syncs():
    with patch.object(ScheduledTask, "sync", autospec=True) as sync_mock:
        scheduled = schedule_task(
            sample_task,
            "*/5 * * * *",
            name="my-scheduled-task",
            description="desc",
            backend="default",
            queue_name="default",
            args=[1],
            kwargs={"a": 2},
        )

    scheduled.refresh_from_db()
    assert scheduled.name == "my-scheduled-task"
    assert scheduled.description == "desc"
    assert scheduled.schedule == "*/5 * * * *"
    assert scheduled.backend_alias == "default"
    assert scheduled.queue_name == "default"
    assert scheduled.args == [1]
    assert scheduled.kwargs == {"a": 2}
    sync_mock.assert_called_once_with(scheduled)


@pytest.mark.django_db
def test_schedule_task_leaves_backend_alias_empty_when_not_provided():
    with patch.object(ScheduledTask, "sync", autospec=True):
        scheduled = schedule_task(
            sample_task,
            "*/10 * * * *",
        )

    scheduled.refresh_from_db()
    assert scheduled.backend_alias == ""


@pytest.mark.django_db
def test_scheduled_task_sync_delegates_to_sync_scheduled_task():
    task = ScheduledTask.objects.create(
        name="task-a",
        schedule="0 * * * *",
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
    )

    with patch("django_tasks_google.scheduler.sync_scheduled_task") as sync_one_mock:
        task.sync()

    sync_one_mock.assert_called_once_with(task.pk)


@pytest.mark.django_db
def test_sync_scheduled_task_creates_job_and_resumes_when_enabled():
    task = ScheduledTask.objects.create(
        name="task-c",
        schedule="0 * * * *",
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        state=ScheduledTask.State.ENABLED,
    )

    with patch("google.cloud.scheduler_v1.CloudSchedulerClient") as client_cls:
        client = client_cls.return_value
        client.get_job.side_effect = NotFound("missing")
        client.create_job.return_value = _FakeJob(_FakeJob.State.DISABLED)

        sync_scheduled_task(task.pk)

    task.refresh_from_db()
    expected_name = "projects/test-project/locations/us-central1/jobs/task-c"
    assert task.cloud_scheduler_job_name == expected_name
    client.create_job.assert_called_once()
    client.update_job.assert_not_called()
    client.resume_job.assert_called_once_with(name=expected_name)
    client.pause_job.assert_not_called()


@pytest.mark.django_db
def test_sync_scheduled_task_builds_expected_scheduler_http_target():
    task = ScheduledTask.objects.create(
        name="task-http",
        schedule="*/5 * * * *",
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        state=ScheduledTask.State.ENABLED,
    )

    with patch("google.cloud.scheduler_v1.CloudSchedulerClient") as client_cls:
        client = client_cls.return_value
        client.get_job.side_effect = NotFound("missing")
        client.create_job.return_value = _FakeJob(_FakeJob.State.ENABLED)

        sync_scheduled_task(task.pk)

    call_kwargs = client.create_job.call_args.kwargs
    assert call_kwargs["parent"] == "projects/test-project/locations/us-central1"
    job = call_kwargs["job"]
    assert job.name == "projects/test-project/locations/us-central1/jobs/task-http"
    assert job.http_target.uri == "https://example.com/tasks/schedule/"
    assert job.http_target.headers["Content-Type"] == (
        "application/x-www-form-urlencoded"
    )
    body = job.http_target.body.decode()
    assert f"task_id={task.pk}" in body
    assert "backend=default" in body
    assert (
        job.http_target.oidc_token.service_account_email
        == "worker@example.iam.gserviceaccount.com"
    )
    assert job.http_target.oidc_token.audience == "https://example.com"


@pytest.mark.django_db
def test_sync_scheduled_task_updates_job_and_pauses_when_disabled():
    task = ScheduledTask.objects.create(
        name="task-d",
        schedule="0 * * * *",
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        state=ScheduledTask.State.DISABLED,
    )

    with patch("google.cloud.scheduler_v1.CloudSchedulerClient") as client_cls:
        client = client_cls.return_value
        client.get_job.return_value = object()
        client.update_job.return_value = _FakeJob(_FakeJob.State.ENABLED)

        sync_scheduled_task(task.pk)

    expected_name = "projects/test-project/locations/us-central1/jobs/task-d"
    client.create_job.assert_not_called()
    client.update_job.assert_called_once()
    client.pause_job.assert_called_once_with(name=expected_name)
    client.resume_job.assert_not_called()


def test_delete_cloud_scheduler_job_if_exists_deletes_when_present():
    with patch("google.cloud.scheduler_v1.CloudSchedulerClient") as client_cls:
        client = client_cls.return_value
        delete_cloud_scheduler_job_if_exists("projects/p/locations/l/jobs/j")
    client.delete_job.assert_called_once_with(name="projects/p/locations/l/jobs/j")


def test_delete_cloud_scheduler_job_if_exists_ignores_not_found():
    with patch("google.cloud.scheduler_v1.CloudSchedulerClient") as client_cls:
        client = client_cls.return_value
        client.delete_job.side_effect = NotFound("missing")
        delete_cloud_scheduler_job_if_exists("projects/p/locations/l/jobs/j")
    client.delete_job.assert_called_once()
