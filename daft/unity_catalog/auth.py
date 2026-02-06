from __future__ import annotations

import base64
import dataclasses
import json
import socket
import urllib.parse
import urllib.request
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError

from daft.retries import retry_with_backoff


def decode_b64url(segment: str) -> bytes:
    # JWT strips padding; add it back
    padding = "=" * (-len(segment) % 4)
    return base64.urlsafe_b64decode(segment + padding)


def decode_jwt(jwt: str) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    header_b64, payload_b64, sig_b64 = jwt.split(".")
    header = json.loads(decode_b64url(header_b64))
    payload = json.loads(decode_b64url(payload_b64))
    signature = decode_b64url(sig_b64)  # raw signature bytes
    return header, payload, signature


def jwt_expiration(jwt: str) -> int:
    try:
        _, payload, _ = decode_jwt(jwt)
        return int(payload["exp"])
    except Exception as ex:
        raise ValueError("JWT payload missing or invalid exp") from ex


@dataclasses.dataclass(frozen=True)
class OAuth2Credentials:
    client_id: str
    client_secret: str


class TokenProvider(ABC):
    @abstractmethod
    def get_token(self) -> str:
        """Return a token string."""
        raise NotImplementedError


class StaticTokenProvider(TokenProvider):
    def __init__(self, token: str | None):
        self._token = token

    def get_token(self) -> str:
        if self._token is None:
            return ""
        return self._token


class OAuth2TokenProvider(TokenProvider):
    def __init__(self, workspace_url: str, credentials: OAuth2Credentials):
        self._workspace_url = workspace_url
        self._credentials = credentials
        self._token: str | None = None
        self._expiration: int = 0

    def get_token(self) -> str:
        if self._token_expired():
            self._refresh()
        if self._token is None:
            raise RuntimeError("Token refresh failed to set token")
        return self._token

    def _refresh(self) -> None:
        self._token = _generate_workspace_token(
            self._workspace_url,
            self._credentials,
        )
        self._expiration = jwt_expiration(self._token)

    def _token_expired(self) -> bool:
        if self._token is None or self._expiration == 0:
            return True
        return is_expired(self._expiration)


def is_expired(exp: int, skew_seconds: int = 300) -> bool:
    exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc)
    return exp_dt - timedelta(seconds=skew_seconds) <= datetime.now(timezone.utc)


def _is_retryable_auth_error(e: Exception) -> bool:
    if isinstance(e, HTTPError):
        return 500 <= e.code < 600 or e.code == 429
    return isinstance(e, (URLError, TimeoutError, socket.timeout))


def _generate_workspace_token(workspace_url: str, oauth: OAuth2Credentials) -> str:
    # Generate a workspace level access token using client credentials.
    # Token lifetime is typically 1 hour (Databricks default).
    # Databricks doc: https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m#generate-a-workspace-level-access-token
    scope = "all-apis"
    token_url = workspace_url.rstrip("/") + "/oidc/v1/token"
    # Build HTTP Basic Auth header
    credentials = f"{oauth.client_id}:{oauth.client_secret}".encode()
    auth_header = base64.b64encode(credentials).decode("ascii")
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "scope": scope,
        }
    ).encode()

    max_retries = 3
    timeout = 10

    request = urllib.request.Request(
        token_url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )

    @retry_with_backoff(
        max_retries=max_retries,
        jitter_ms=500,
        max_backoff_ms=10_000,
        should_retry=_is_retryable_auth_error,
    )
    def _request_token() -> str:
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                response_body = response.read().decode()
                data = json.loads(response_body)
                if "access_token" not in data:
                    raise RuntimeError("UnityCatalog token response missing expected field 'access_token'")
                token = data["access_token"]
                if not token:
                    raise RuntimeError("UnityCatalog token response contains empty or None 'access_token'")
                return token
        except HTTPError as e:
            error_body = e.read().decode(errors="replace")
            if 500 <= e.code < 600 or e.code == 429:
                raise
            raise RuntimeError(
                f"UnityCatalog token request to {token_url} failed with HTTP {e.code}: {error_body}"
            ) from e

    try:
        return _request_token()
    except Exception as e:
        if _is_retryable_auth_error(e):
            raise RuntimeError(
                f"UnityCatalog token request to {token_url} failed after {max_retries} attempts: {e}"
            ) from e
        raise
