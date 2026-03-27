from datetime import timedelta
from unittest.mock import patch

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.tasks import Task, task_backends

from django_tasks_google.backends import (
    CloudRunJobsBackend,
    CloudTasksBackend,
    get_oidc_audience,
)
from django_tasks_google.models import TaskExecution
from tests.fake_tasks import sample_task


@pytest.mark.django_db
def test_get_oidc_audience_extracts_scheme_and_host():
    audience = get_oidc_audience("https://example.com/tasks/execute/")
    assert audience == "https://example.com"


def test_backend_requires_project_id():
    with pytest.raises(ImproperlyConfigured, match="project_id is required"):
        CloudTasksBackend(
            "x",
            {
                "OPTIONS": {
                    "location": "us-central1",
                    "base_url": "https://example.com/tasks/",
                    "oidc_service_account": "svc@example.com",
                }
            },
        )


def test_backend_rejects_heartbeat_interval_greater_than_timeout():
    with pytest.raises(
        ImproperlyConfigured,
        match=(
            "heartbeat_interval_seconds cannot be greater than "
            "heartbeat_timeout_seconds"
        ),
    ):
        CloudTasksBackend(
            "x",
            {
                "OPTIONS": {
                    "project_id": "p1",
                    "location": "us-central1",
                    "base_url": "https://example.com/tasks/",
                    "oidc_service_account": "svc@example.com",
                    "heartbeat_interval_seconds": 10,
                    "heartbeat_timeout_seconds": 5,
                }
            },
        )


@pytest.mark.django_db
def test_enqueue_creates_task_execution_and_calls_enqueue_gcp(
    django_capture_on_commit_callbacks,
):
    backend = task_backends["default"]
    task = Task(
        priority=0,
        func=sample_task.func,
        backend="default",
        queue_name="default",
        run_after=None,
        takes_context=False,
    )

    with (
        patch.object(backend, "enqueue_gcp", autospec=True) as enqueue_gcp_mock,
        django_capture_on_commit_callbacks(execute=True),
    ):
        task_result = backend.enqueue(task, args=[1, "a"], kwargs={"k": "v"})

    execution = TaskExecution.objects.get(pk=task_result.id)
    enqueue_gcp_mock.assert_called_once_with(execution.pk)
    assert execution.priority == 0
    assert execution.backend_alias == "default"
    assert execution.queue_name == "default"
    assert execution.args == [1, "a"]
    assert execution.kwargs == {"k": "v"}
    assert task_result.id == str(execution.pk)


@pytest.mark.django_db
def test_get_result_returns_none_for_missing_result_id():
    backend = task_backends["default"]
    assert backend.get_result("999999") is None


@pytest.mark.django_db
def test_get_result_returns_task_result_for_existing_execution():
    backend = task_backends["default"]
    execution = TaskExecution.objects.create(
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        args=[],
        kwargs={},
    )

    result = backend.get_result(execution.pk)
    assert result is not None
    assert result.id == str(execution.pk)
    assert result.backend == "default"
    assert result.status is not None


def test_backend_sets_default_urls_and_timedeltas():
    backend = CloudTasksBackend(
        "x",
        {
            "OPTIONS": {
                "project_id": "p1",
                "location": "us-central1",
                "base_url": "https://example.com/tasks",
                "oidc_service_account": "svc@example.com",
            }
        },
    )

    assert backend.base_url == "https://example.com/tasks/"
    assert backend.execute_url == "https://example.com/tasks/execute/"
    assert backend.schedule_url == "https://example.com/tasks/schedule/"
    assert backend.heartbeat_interval == timedelta(seconds=10)
    assert backend.heartbeat_timeout == timedelta(seconds=30)


@pytest.mark.django_db
def test_cloud_run_enqueue_gcp_sets_execution_name_on_success(
    django_capture_on_commit_callbacks,
):
    backend = CloudRunJobsBackend(
        "jobs",
        {
            "OPTIONS": {
                "project_id": "p1",
                "location": "us-central1",
                "base_url": "https://example.com/tasks",
                "oidc_service_account": "svc@example.com",
            }
        },
    )
    execution = TaskExecution.objects.create(
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        args=[],
        kwargs={},
    )

    fake_operation = type(
        "Operation",
        (),
        {
            "metadata": type(
                "Metadata", (), {"name": "projects/p/locations/l/executions/e1"}
            )()
        },
    )()

    with (
        patch("google.cloud.run_v2.JobsClient", autospec=True) as jobs_client_cls,
        patch("django_tasks_google.backends.task_enqueued.send") as send_mock,
        django_capture_on_commit_callbacks(execute=True),
    ):
        jobs_client = jobs_client_cls.return_value
        jobs_client.run_job.return_value = fake_operation

        backend.enqueue_gcp(execution.pk)

    execution.refresh_from_db()
    assert (
        execution.cloud_run_job_execution_name == "projects/p/locations/l/executions/e1"
    )
    assert execution.status != "FAILED"
    assert execution.errors == []

    jobs_client_cls.assert_called_once()
    jobs_client.run_job.assert_called_once()
    request = jobs_client.run_job.call_args.kwargs["request"]
    assert request.name == "projects/p1/locations/us-central1/jobs/default"
    assert request.overrides.container_overrides[0].args == [
        "python",
        "manage.py",
        "execute_task",
        str(execution.pk),
    ]
    send_mock.assert_called_once()


@pytest.mark.django_db
def test_cloud_run_enqueue_gcp_marks_failed_on_client_error():
    backend = CloudRunJobsBackend(
        "jobs",
        {
            "OPTIONS": {
                "project_id": "p1",
                "location": "us-central1",
                "base_url": "https://example.com/tasks",
                "oidc_service_account": "svc@example.com",
            }
        },
    )
    execution = TaskExecution.objects.create(
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        args=[],
        kwargs={},
    )

    with (
        patch("google.cloud.run_v2.JobsClient", autospec=True) as jobs_client_cls,
        patch(
            "django_tasks_google.backends.task_enqueued.send", autospec=True
        ) as send_mock,
    ):
        jobs_client = jobs_client_cls.return_value
        jobs_client.run_job.side_effect = RuntimeError("gcp-failure")
        backend.enqueue_gcp(execution.pk)

    execution.refresh_from_db()
    assert execution.status == "FAILED"
    assert execution.cloud_run_job_execution_name is None
    assert len(execution.errors) == 1
    assert execution.errors[0]["exception_class_path"].endswith("RuntimeError")
    send_mock.assert_not_called()
