from types import SimpleNamespace
from unittest.mock import patch

import pytest
import requests
from django.http import HttpRequest
from django.test import RequestFactory
from google.auth.exceptions import GoogleAuthError

from django_tasks_google.auth import handle_oidc_auth, post_with_oidc


def _request(auth_header: str = "") -> HttpRequest:
    return RequestFactory().get("/", HTTP_AUTHORIZATION=auth_header)


def test_handle_oidc_auth_rejects_missing_bearer_header():
    ok, status, error = handle_oidc_auth(_request(""), "aud", "svc@example.com")
    assert ok is False
    assert status == 401
    assert isinstance(error, str) and "Authorization header" in error


def test_handle_oidc_auth_rejects_invalid_token():
    with patch("django_tasks_google.auth.id_token.verify_oauth2_token") as verify_mock:
        verify_mock.side_effect = ValueError("bad token")
        ok, status, error = handle_oidc_auth(
            _request("Bearer abc"), "aud", "svc@example.com"
        )
    assert ok is False
    assert status == 401
    assert error == "Invalid OIDC token"


def test_handle_oidc_auth_rejects_google_auth_error():
    with patch("django_tasks_google.auth.id_token.verify_oauth2_token") as verify_mock:
        verify_mock.side_effect = GoogleAuthError("broken")
        ok, status, error = handle_oidc_auth(
            _request("Bearer abc"), "aud", "svc@example.com"
        )
    assert ok is False
    assert status == 401
    assert error == "Invalid OIDC token"


def test_handle_oidc_auth_rejects_unexpected_email():
    with patch("django_tasks_google.auth.id_token.verify_oauth2_token") as verify_mock:
        verify_mock.return_value = {
            "email": "other@example.com",
            "email_verified": True,
        }
        ok, status, error = handle_oidc_auth(
            _request("Bearer abc"), "aud", "svc@example.com"
        )
    assert ok is False
    assert status == 403
    assert isinstance(error, str) and "Unexpected caller email" in error


def test_handle_oidc_auth_rejects_unverified_email():
    with patch("django_tasks_google.auth.id_token.verify_oauth2_token") as verify_mock:
        verify_mock.return_value = {"email": "svc@example.com", "email_verified": False}
        ok, status, error = handle_oidc_auth(
            _request("Bearer abc"), "aud", "svc@example.com"
        )
    assert ok is False
    assert status == 403
    assert isinstance(error, str) and "not verified" in error


def test_handle_oidc_auth_returns_success_for_verified_expected_email():
    with patch("django_tasks_google.auth.id_token.verify_oauth2_token") as verify_mock:
        verify_mock.return_value = {"email": "svc@example.com", "email_verified": True}
        ok, status, error = handle_oidc_auth(
            _request("Bearer abc"), "aud", "svc@example.com"
        )
    assert ok is True
    assert status is None
    assert error is None


def test_post_with_oidc_fetches_token_and_posts():
    response_obj = SimpleNamespace()
    with (
        patch(
            "django_tasks_google.auth.id_token.fetch_id_token",
            return_value="minted-token",
        ) as fetch_mock,
        patch("django_tasks_google.auth._HTTP_CLIENT.post") as post_mock,
    ):
        post_mock.return_value = response_obj
        result = post_with_oidc(
            "https://workflows.googleapis.com/v1/callbacks/abc",
            audience="https://workflows.googleapis.com/v1/callbacks/abc",
            json_body={"status": "succeeded"},
        )

    assert result is response_obj
    fetch_mock.assert_called_once()
    # fetch_id_token(request, audience)
    assert fetch_mock.call_args.args[1] == (
        "https://workflows.googleapis.com/v1/callbacks/abc"
    )
    post_mock.assert_called_once()
    post_kwargs = post_mock.call_args.kwargs
    assert post_kwargs["json"] == {"status": "succeeded"}
    assert post_kwargs["headers"]["Authorization"] == "Bearer minted-token"
    assert post_kwargs["headers"]["Content-Type"] == "application/json"
    assert post_kwargs["timeout"] == 30.0


def test_post_with_oidc_propagates_http_errors():
    with (
        patch(
            "django_tasks_google.auth.id_token.fetch_id_token",
            return_value="minted-token",
        ),
        patch(
            "django_tasks_google.auth._HTTP_CLIENT.post",
            side_effect=requests.RequestException("boom"),
        ),
    ):
        with pytest.raises(requests.RequestException):
            post_with_oidc(
                "https://example.com/cb",
                audience="https://example.com/cb",
                json_body={},
            )
