from datetime import timedelta
from unittest.mock import patch

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.tasks import Task, task_backends
from django.tasks.exceptions import InvalidTask
from django.utils import timezone

from django_tasks_google.backends import (
    CloudRunJobsBackend,
    CloudTasksBackend,
    ProcessBackend,
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
        patch.object(backend, "get_max_attempts_with_cache", return_value=3),
        patch("google.cloud.run_v2.JobsClient", autospec=True) as jobs_client_cls,
        patch("django_tasks_google.backends.task_enqueued.send_robust") as send_mock,
        django_capture_on_commit_callbacks(execute=True),
    ):
        jobs_client = jobs_client_cls.return_value
        jobs_client.run_job.return_value = fake_operation

        backend.enqueue_gcp(execution.pk)

    execution.refresh_from_db()
    assert (
        execution.cloud_run_job_execution_name == "projects/p/locations/l/executions/e1"
    )
    assert execution.max_attempts == 3
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
        patch.object(backend, "get_max_attempts_with_cache", return_value=3),
        patch("google.cloud.run_v2.JobsClient", autospec=True) as jobs_client_cls,
        patch("django_tasks_google.backends.logger.exception") as log_exception_mock,
        patch(
            "django_tasks_google.backends.task_enqueued.send_robust", autospec=True
        ) as send_mock,
    ):
        jobs_client = jobs_client_cls.return_value
        jobs_client.run_job.side_effect = RuntimeError("gcp-failure")
        backend.enqueue_gcp(execution.pk)

    execution.refresh_from_db()
    log_exception_mock.assert_called_once()
    assert execution.status == "FAILED"
    assert execution.cloud_run_job_execution_name is None
    assert len(execution.errors) == 1
    assert execution.errors[0]["exception_class_path"].endswith("RuntimeError")
    send_mock.assert_not_called()


@pytest.mark.django_db
def test_cloud_tasks_enqueue_gcp_builds_expected_http_request(
    django_capture_on_commit_callbacks,
):
    backend = task_backends["default"]
    execution = TaskExecution.objects.create(
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        args=[],
        kwargs={},
    )

    fake_cloud_task = type("CloudTask", (), {"name": "projects/p/tasks/t1"})()

    with (
        patch.object(backend, "get_max_attempts_with_cache", return_value=5),
        patch("google.cloud.tasks_v2.CloudTasksClient", autospec=True) as client_cls,
        patch("django_tasks_google.backends.task_enqueued.send_robust") as send_mock,
        django_capture_on_commit_callbacks(execute=True),
    ):
        client = client_cls.return_value
        client.create_task.return_value = fake_cloud_task

        backend.enqueue_gcp(execution.pk)

    execution.refresh_from_db()
    assert execution.cloud_task_name == "projects/p/tasks/t1"
    assert execution.max_attempts == 5

    call_kwargs = client.create_task.call_args.kwargs
    assert (
        call_kwargs["parent"]
        == "projects/test-project/locations/us-central1/queues/default"
    )
    task_proto = call_kwargs["task"]
    assert task_proto.http_request.url == "https://example.com/tasks/execute/"
    assert task_proto.http_request.headers["Content-Type"] == (
        "application/x-www-form-urlencoded"
    )
    body = task_proto.http_request.body.decode()
    assert f"execution_id={execution.pk}" in body
    assert "backend=default" in body
    assert (
        task_proto.http_request.oidc_token.service_account_email
        == "worker@example.iam.gserviceaccount.com"
    )
    assert task_proto.http_request.oidc_token.audience == "https://example.com"
    send_mock.assert_called_once()


@pytest.mark.django_db
def test_cloud_tasks_enqueue_gcp_sets_schedule_time_for_run_after():
    backend = task_backends["default"]
    run_after = timezone.now() + timedelta(minutes=5)
    execution = TaskExecution.objects.create(
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        run_after=run_after,
        args=[],
        kwargs={},
    )

    fake_cloud_task = type("CloudTask", (), {"name": "projects/p/tasks/t2"})()

    with (
        patch.object(backend, "get_max_attempts_with_cache", return_value=5),
        patch("google.cloud.tasks_v2.CloudTasksClient", autospec=True) as client_cls,
    ):
        client = client_cls.return_value
        client.create_task.return_value = fake_cloud_task
        backend.enqueue_gcp(execution.pk)

    task_proto = client.create_task.call_args.kwargs["task"]
    schedule_dt = task_proto.schedule_time
    assert int(schedule_dt.timestamp()) == int(run_after.timestamp())


def test_process_backend_does_not_require_gcp_config():
    backend = ProcessBackend("x", {"OPTIONS": {}})
    assert backend.project_id is None
    assert backend.execute_url is None
    assert backend.schedule_url is None
    assert backend.oidc_audience is None
    assert backend.requires_gcp_config is False
    assert backend.uses_cloud_scheduler is False


def test_process_backend_mode_cloud_tasks_supports_defer():
    backend = task_backends["process"]
    assert backend.supports_defer is True

    # Constructing a deferred task against the process backend validates fine.
    task = Task(
        priority=0,
        func=sample_task.func,
        backend="process",
        queue_name="default",
        run_after=timezone.now() + timedelta(minutes=5),
        takes_context=False,
    )
    backend.validate_task(task)


def test_process_backend_mode_cloud_run_jobs_rejects_defer():
    backend = ProcessBackend("x", {"OPTIONS": {"mode": "cloud_run_jobs"}})
    assert backend.supports_defer is False

    # Built against the (defer-capable) process alias, then validated against a
    # cloud_run_jobs-mode backend, which must reject the run_after.
    task = Task(
        priority=0,
        func=sample_task.func,
        backend="process",
        queue_name="default",
        run_after=timezone.now() + timedelta(minutes=5),
        takes_context=False,
    )
    with pytest.raises(InvalidTask):
        backend.validate_task(task)


def test_process_backend_mode_sets_force_cancel_capability():
    cloud_tasks = ProcessBackend("x", {"OPTIONS": {"mode": "cloud_tasks"}})
    cloud_run = ProcessBackend("x", {"OPTIONS": {"mode": "cloud_run_jobs"}})
    assert cloud_tasks.supports_force_cancel is False
    assert cloud_run.supports_force_cancel is True


def test_process_backend_invalid_mode_raises():
    with pytest.raises(ImproperlyConfigured, match="mode must be"):
        ProcessBackend("x", {"OPTIONS": {"mode": "nonsense"}})


def test_process_backend_get_max_attempts_returns_configured():
    backend = ProcessBackend("x", {"OPTIONS": {"max_attempts": 7}})
    assert backend.get_max_attempts("default") == 7
    assert backend.get_max_attempts_with_cache("default") == 7


def test_force_cancel_capability_flags():
    assert (
        CloudRunJobsBackend(
            "jobs",
            {
                "OPTIONS": {
                    "project_id": "p1",
                    "location": "us-central1",
                    "base_url": "https://example.com/tasks",
                    "oidc_service_account": "svc@example.com",
                }
            },
        ).supports_force_cancel
        is True
    )
    assert task_backends["default"].supports_force_cancel is False  # CloudTasks


@pytest.mark.django_db
def test_process_backend_enqueue_sets_max_attempts_and_fires_signal(
    django_capture_on_commit_callbacks,
):
    backend = task_backends["process"]
    task = Task(
        priority=0,
        func=sample_task.func,
        backend="process",
        queue_name="default",
        run_after=None,
        takes_context=False,
    )

    with (
        patch("django_tasks_google.backends.task_enqueued.send_robust") as send_mock,
        django_capture_on_commit_callbacks(execute=True),
    ):
        task_result = backend.enqueue(task, args=[1], kwargs={})

    execution = TaskExecution.objects.get(pk=task_result.id)
    assert execution.status == "READY"
    assert execution.max_attempts == 3
    send_mock.assert_called_once()


@pytest.mark.django_db
def test_cancel_force_on_process_cloud_run_jobs_mode_marks_force_cancel():
    execution = TaskExecution.objects.create(
        module_path="tests.fake_tasks.process_task",
        backend_alias="process_jobs",  # cloud_run_jobs mode
        queue_name="default",
        args=[],
        kwargs={},
        status="RUNNING",
    )

    execution.cancel(force=True)

    execution.refresh_from_db()
    assert execution.force_cancel is True
    assert execution.cancelled_at is not None
    assert execution.status == "FAILED"


@pytest.mark.django_db
def test_cancel_force_on_process_cloud_tasks_mode_raises():
    execution = TaskExecution.objects.create(
        module_path="tests.fake_tasks.process_task",
        backend_alias="process",  # settings default = cloud_tasks mode
        queue_name="default",
        args=[],
        kwargs={},
    )
    with pytest.raises(NotImplementedError):
        execution.cancel(force=True)


@pytest.mark.django_db
def test_cancel_force_on_cloud_tasks_backend_raises():
    execution = TaskExecution.objects.create(
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        args=[],
        kwargs={},
    )
    with pytest.raises(NotImplementedError):
        execution.cancel(force=True)


def test_cloud_run_jobs_force_cancel_calls_api():
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
    execution = TaskExecution(cloud_run_job_execution_name="projects/p/executions/e1")

    with patch("google.cloud.run_v2.ExecutionsClient", autospec=True) as client_cls:
        backend.force_cancel(execution)

    client_cls.return_value.cancel_execution.assert_called_once_with(
        name="projects/p/executions/e1"
    )


def test_cloud_run_jobs_force_cancel_noop_without_execution_name():
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
    execution = TaskExecution(cloud_run_job_execution_name=None)

    with patch("google.cloud.run_v2.ExecutionsClient", autospec=True) as client_cls:
        backend.force_cancel(execution)

    client_cls.assert_not_called()
