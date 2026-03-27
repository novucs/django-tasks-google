from types import SimpleNamespace
from unittest.mock import patch

from google.auth.exceptions import GoogleAuthError

from django_tasks_google.auth import handle_oidc_auth


def _request(auth_header=""):
    return SimpleNamespace(headers={"Authorization": auth_header})


def test_handle_oidc_auth_rejects_missing_bearer_header():
    ok, status, error = handle_oidc_auth(_request(""), "aud", "svc@example.com")
    assert ok is False
    assert status == 401
    assert "Authorization header" in error


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
    assert "Unexpected caller email" in error


def test_handle_oidc_auth_rejects_unverified_email():
    with patch("django_tasks_google.auth.id_token.verify_oauth2_token") as verify_mock:
        verify_mock.return_value = {"email": "svc@example.com", "email_verified": False}
        ok, status, error = handle_oidc_auth(
            _request("Bearer abc"), "aud", "svc@example.com"
        )
    assert ok is False
    assert status == 403
    assert "not verified" in error


def test_handle_oidc_auth_returns_success_for_verified_expected_email():
    with patch("django_tasks_google.auth.id_token.verify_oauth2_token") as verify_mock:
        verify_mock.return_value = {"email": "svc@example.com", "email_verified": True}
        ok, status = handle_oidc_auth(_request("Bearer abc"), "aud", "svc@example.com")
    assert ok is True
    assert status is None
