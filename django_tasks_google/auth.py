import requests
from cachecontrol import CacheControl
from django.http import HttpRequest
from google.auth.exceptions import GoogleAuthError
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

_HTTP_CLIENT = CacheControl(requests.Session())
_CACHED_REQUEST = google_requests.Request(session=_HTTP_CLIENT)


def handle_oidc_auth(
    request: HttpRequest, audience: str, email: str
) -> tuple[bool, int | None, str | None]:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return False, 401, "Missing or invalid Authorization header"

    token = auth_header[7:]
    try:
        # verify_oauth2_token checks the issuer for us
        claims = id_token.verify_oauth2_token(token, _CACHED_REQUEST, audience=audience)
        if claims.get("email") != email:
            return False, 403, f"Unexpected caller email: {claims.get('email')}"
        if not claims.get("email_verified"):
            return False, 403, "Caller email is not verified"
    except (GoogleAuthError, ValueError):
        return False, 401, "Invalid OIDC token"

    return True, None, None
