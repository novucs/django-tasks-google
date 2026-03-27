from unittest.mock import patch

import pytest

from django_tasks_google.models import ScheduledTask


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
        )
    assert response.status_code == 500
    execute_mock.assert_called_once_with("1")


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
            data={"task_id": "1", "backend": "default", "idempotency_key": "k1"},
            HTTP_AUTHORIZATION="Bearer token",
        )
    assert response.status_code == 403


@pytest.mark.django_db
def test_schedule_task_view_returns_404_for_missing_task(client):
    with patch("django_tasks_google.auth.handle_oidc_auth") as auth_mock:
        auth_mock.return_value = (True, None, None)
        response = client.post(
            "/schedule/",
            data={"task_id": "999", "backend": "default", "idempotency_key": "k1"},
            HTTP_AUTHORIZATION="Bearer token",
        )
    assert response.status_code == 404


@pytest.mark.django_db
def test_schedule_task_view_skips_duplicate_idempotency(client):
    task = ScheduledTask.objects.create(
        name="task-v1",
        schedule="0 * * * *",
        module_path="tests.fake_tasks.sample_task",
        backend_alias="default",
        queue_name="default",
        idempotency_key="k1",
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
                "idempotency_key": "k1",
            },
            HTTP_AUTHORIZATION="Bearer token",
        )
    assert response.status_code == 204
    enqueue_mock.assert_not_called()


@pytest.mark.django_db
def test_schedule_task_view_sets_idempotency_and_enqueues(client):
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
                "idempotency_key": "k-new",
            },
            HTTP_AUTHORIZATION="Bearer token",
        )
    task.refresh_from_db()
    assert response.status_code == 204
    assert task.idempotency_key == "k-new"
    enqueue_mock.assert_called_once_with(task)
