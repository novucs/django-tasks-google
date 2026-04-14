import json
from unittest.mock import ANY, patch

import pytest

from django_tasks_google.models import ScheduledTask, TaskExecution


@pytest.mark.django_db
def test_execute_task_view_returns_400_for_invalid_form(client):
    response = client.post("/execute/", data={})
    assert response.status_code == 400


@pytest.mark.django_db
def test_execute_task_view_returns_auth_status_on_auth_failure(client):
    with patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock:
        auth_mock.return_value = (False, 401, "bad-token")
        response = client.post(
            "/execute/",
            data={"execution_id": "1", "backend": "default"},
            HTTP_AUTHORIZATION="Bearer token",
        )
    assert response.status_code == 401


@pytest.mark.django_db
def test_execute_task_view_returns_500_when_retry_requested(client):
    with (
        patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock,
        patch("django_tasks_google.views.execute_task") as execute_mock,
    ):
        auth_mock.return_value = (True, None, None)
        execute_mock.return_value = True
        response = client.post(
            "/execute/",
            data={"execution_id": "1", "backend": "default"},
            HTTP_AUTHORIZATION="Bearer token",
            HTTP_X_CLOUDTASKS_TASKRETRYCOUNT="0",
        )
    assert response.status_code == 500
    execute_mock.assert_called_once_with("1", 1, backend=ANY)


@pytest.mark.django_db
def test_execute_task_view_returns_204_when_done(client):
    with (
        patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock,
        patch("django_tasks_google.views.execute_task") as execute_mock,
    ):
        auth_mock.return_value = (True, None, None)
        execute_mock.return_value = False
        response = client.post(
            "/execute/",
            data={"execution_id": "1", "backend": "default"},
            HTTP_AUTHORIZATION="Bearer token",
            HTTP_X_CLOUDTASKS_TASKRETRYCOUNT="0",
        )
    assert response.status_code == 204


@pytest.mark.django_db
def test_schedule_task_view_returns_400_for_invalid_form(client):
    response = client.post("/schedule/", data={})
    assert response.status_code == 400


@pytest.mark.django_db
def test_schedule_task_view_returns_auth_status_on_auth_failure(client):
    with patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock:
        auth_mock.return_value = (False, 403, "bad-caller")
        response = client.post(
            "/schedule/",
            data={"task_id": "1", "backend": "default"},
            HTTP_AUTHORIZATION="Bearer token",
            HTTP_X_CLOUDSCHEDULER_JOBNAME="projects/test/locations/us-central1/jobs/test-job",
            HTTP_X_CLOUDSCHEDULER_SCHEDULETIME="2026-03-27T12:00:00Z",
        )
    assert response.status_code == 403


@pytest.mark.django_db
def test_schedule_task_view_returns_404_for_missing_task(client):
    with patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock:
        auth_mock.return_value = (True, None, None)
        response = client.post(
            "/schedule/",
            data={"task_id": "999", "backend": "default"},
            HTTP_AUTHORIZATION="Bearer token",
            HTTP_X_CLOUDSCHEDULER_JOBNAME="projects/test/locations/us-central1/jobs/test-job",
            HTTP_X_CLOUDSCHEDULER_SCHEDULETIME="2026-03-27T12:00:00Z",
        )
    assert response.status_code == 404


@pytest.mark.django_db
def test_schedule_task_view_skips_duplicate_idempotency(client):
    job_name = "projects/test/locations/us-central1/jobs/test-job"
    schedule_time = "2026-03-27T12:00:00Z"
    idempotency_key = f"{job_name}:{schedule_time}"
    task = ScheduledTask.objects.create(
        name="task-v1",
        schedule="0 * * * *",
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        idempotency_key=idempotency_key,
    )
    with (
        patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock,
        patch.object(ScheduledTask, "enqueue", autospec=True) as enqueue_mock,
    ):
        auth_mock.return_value = (True, None, None)
        response = client.post(
            "/schedule/",
            data={
                "task_id": str(task.pk),
                "backend": "default",
            },
            HTTP_AUTHORIZATION="Bearer token",
            HTTP_X_CLOUDSCHEDULER_JOBNAME=job_name,
            HTTP_X_CLOUDSCHEDULER_SCHEDULETIME=schedule_time,
        )
    assert response.status_code == 204
    enqueue_mock.assert_not_called()


@pytest.mark.django_db
def test_schedule_task_view_sets_idempotency_and_enqueues(client):
    job_name = "projects/test/locations/us-central1/jobs/test-job"
    schedule_time = "2026-03-27T12:00:00Z"
    expected_idempotency_key = f"{job_name}:{schedule_time}"
    task = ScheduledTask.objects.create(
        name="task-v2",
        schedule="0 * * * *",
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
    )
    with (
        patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock,
        patch.object(ScheduledTask, "enqueue", autospec=True) as enqueue_mock,
    ):
        auth_mock.return_value = (True, None, None)
        response = client.post(
            "/schedule/",
            data={
                "task_id": str(task.pk),
                "backend": "default",
            },
            HTTP_AUTHORIZATION="Bearer token",
            HTTP_X_CLOUDSCHEDULER_JOBNAME=job_name,
            HTTP_X_CLOUDSCHEDULER_SCHEDULETIME=schedule_time,
        )
    task.refresh_from_db()
    assert response.status_code == 204
    assert task.idempotency_key == expected_idempotency_key
    enqueue_mock.assert_called_once_with(task)


@pytest.mark.django_db
def test_enqueue_task_view_deduplicates_on_callback_url(client):
    callback_url = "https://workflowexecutions.googleapis.com/v1/callbacks/dedup"
    with (
        patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock,
        patch(
            "django_tasks_google.backends.CloudTasksBackend.enqueue_gcp",
        ) as enqueue_gcp_mock,
    ):
        auth_mock.return_value = (True, None, None)
        enqueue_gcp_mock.return_value = None

        first = client.post(
            "/enqueue/",
            data=json.dumps(
                {
                    "task_path": "tests.fake_tasks.sample_task",
                    "callback_url": callback_url,
                }
            ),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token",
        )
        second = client.post(
            "/enqueue/",
            data=json.dumps(
                {
                    "task_path": "tests.fake_tasks.sample_task",
                    "callback_url": callback_url,
                }
            ),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token",
        )

    assert first.status_code == 202
    assert second.status_code == 200
    assert first.json()["execution_id"] == second.json()["execution_id"]
    assert TaskExecution.objects.filter(callback_url=callback_url).count() == 1


@pytest.mark.django_db
def test_enqueue_task_view_allows_duplicate_without_callback_url(client):
    with (
        patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock,
        patch(
            "django_tasks_google.backends.CloudTasksBackend.enqueue_gcp",
        ) as enqueue_gcp_mock,
    ):
        auth_mock.return_value = (True, None, None)
        enqueue_gcp_mock.return_value = None

        first = client.post(
            "/enqueue/",
            data=json.dumps({"task_path": "tests.fake_tasks.sample_task"}),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token",
        )
        second = client.post(
            "/enqueue/",
            data=json.dumps({"task_path": "tests.fake_tasks.sample_task"}),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token",
        )

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["execution_id"] != second.json()["execution_id"]


@pytest.mark.django_db
def test_enqueue_task_view_allows_different_callback_urls(client):
    with (
        patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock,
        patch(
            "django_tasks_google.backends.CloudTasksBackend.enqueue_gcp",
        ) as enqueue_gcp_mock,
    ):
        auth_mock.return_value = (True, None, None)
        enqueue_gcp_mock.return_value = None

        first = client.post(
            "/enqueue/",
            data=json.dumps(
                {
                    "task_path": "tests.fake_tasks.sample_task",
                    "callback_url": "https://workflowexecutions.googleapis.com/v1/callbacks/aaa",
                }
            ),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token",
        )
        second = client.post(
            "/enqueue/",
            data=json.dumps(
                {
                    "task_path": "tests.fake_tasks.sample_task",
                    "callback_url": "https://workflowexecutions.googleapis.com/v1/callbacks/bbb",
                }
            ),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token",
        )

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["execution_id"] != second.json()["execution_id"]


MALFORMED_ENQUEUE_REQUESTS = [
    pytest.param("not-json", id="invalid_json"),
    pytest.param(json.dumps([1, 2, 3]), id="non_object_body"),
    pytest.param(
        json.dumps({"task_path": "tests.does_not_exist"}),
        id="unknown_task_path",
    ),
    # _is_task is false for plain callables like json.loads
    pytest.param(json.dumps({"task_path": "json.loads"}), id="non_task_path"),
]


@pytest.mark.django_db
@pytest.mark.parametrize("body", MALFORMED_ENQUEUE_REQUESTS)
def test_enqueue_task_view_returns_400_for_malformed_request(client, body):
    response = client.post(
        "/enqueue/",
        data=body,
        content_type="application/json",
    )
    assert response.status_code == 400


@pytest.mark.django_db
def test_enqueue_task_view_returns_400_when_backend_is_not_google_backend(client):
    from django.tasks.backends.immediate import ImmediateBackend

    fake_backend = ImmediateBackend(
        "default", {"BACKEND": "django.tasks.backends.immediate.ImmediateBackend"}
    )
    # Force both the form's validate_backend lookup AND the task.backend
    # fallback to return a non-DjangoTasksGoogleBackend instance.
    with patch(
        "django_tasks_google.forms.task_backends",
        new={"default": fake_backend},
    ):
        response = client.post(
            "/enqueue/",
            data=json.dumps(
                {
                    "task_path": "tests.fake_tasks.sample_task",
                    "backend": "default",
                }
            ),
            content_type="application/json",
        )
    assert response.status_code == 400


@pytest.mark.django_db
def test_enqueue_task_view_returns_auth_status_on_auth_failure(client):
    with patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock:
        auth_mock.return_value = (False, 401, "bad-token")
        response = client.post(
            "/enqueue/",
            data=json.dumps({"task_path": "tests.fake_tasks.sample_task"}),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token",
        )
    assert response.status_code == 401


@pytest.mark.django_db
def test_enqueue_task_view_creates_execution_with_callback_url(client):
    with (
        patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock,
        patch(
            "django_tasks_google.backends.CloudTasksBackend.enqueue_gcp",
        ) as enqueue_gcp_mock,
    ):
        auth_mock.return_value = (True, None, None)
        enqueue_gcp_mock.return_value = None
        response = client.post(
            "/enqueue/",
            data=json.dumps(
                {
                    "task_path": "tests.fake_tasks.sample_task",
                    "backend": "default",
                    "args": [1, 2],
                    "kwargs": {"a": "b"},
                    "callback_url": "https://workflowexecutions.googleapis.com/v1/callbacks/abc",
                }
            ),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token",
        )

    assert response.status_code == 202
    body = response.json()
    assert "execution_id" in body
    execution = TaskExecution.objects.get(pk=body["execution_id"])
    assert (
        execution.callback_url
        == "https://workflowexecutions.googleapis.com/v1/callbacks/abc"
    )
    assert execution.args == [1, 2]
    assert execution.kwargs == {"a": "b"}
    assert execution.backend_alias == "default"


@pytest.mark.django_db
def test_enqueue_task_view_applies_queue_override(client):
    with (
        patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock,
        patch(
            "django_tasks_google.backends.CloudTasksBackend.enqueue_gcp",
        ) as enqueue_gcp_mock,
    ):
        auth_mock.return_value = (True, None, None)
        enqueue_gcp_mock.return_value = None
        response = client.post(
            "/enqueue/",
            data=json.dumps(
                {
                    "task_path": "tests.fake_tasks.sample_task",
                    "queue_name": "high-priority",
                }
            ),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer token",
        )

    assert response.status_code == 202
    execution = TaskExecution.objects.get(pk=response.json()["execution_id"])
    assert execution.queue_name == "high-priority"
