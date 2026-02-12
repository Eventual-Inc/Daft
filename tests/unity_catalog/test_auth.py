from __future__ import annotations

from datetime import datetime, timedelta, timezone
from http.client import HTTPMessage
from io import BytesIO
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

import pytest

from daft.unity_catalog.auth import OAuth2Credentials, _generate_workspace_token, is_expired


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tenacity.nap.time.sleep", lambda _: None)


def _http_error(url: str, code: int, body: bytes) -> HTTPError:
    headers = HTTPMessage()
    return HTTPError(url=url, code=code, msg="error", hdrs=headers, fp=BytesIO(body))


@patch("daft.unity_catalog.auth.urllib.request.urlopen")
def test_generate_token_retries_on_429(mock_urlopen) -> None:
    error_429 = _http_error(
        url="https://workspace.example/oidc/v1/token",
        code=429,
        body=b'{"error": "rate_limit_exceeded", "message": ""}',
    )
    mock_urlopen.side_effect = error_429

    credentials = OAuth2Credentials(client_id="test_id", client_secret="test_secret")

    with pytest.raises(RuntimeError) as excinfo:
        _generate_workspace_token("https://workspace.example", credentials)

    assert "failed after 3 attempts" in str(excinfo.value)
    assert mock_urlopen.call_count == 3


@patch("daft.unity_catalog.auth.urllib.request.urlopen")
def test_generate_token_fails_instantly_on_401(mock_urlopen) -> None:
    error_401 = _http_error(
        url="https://workspace.example/oidc/v1/token",
        code=401,
        body=b'{"error": "invalid_client", "message": "unauthorized"}',
    )
    mock_urlopen.side_effect = error_401

    credentials = OAuth2Credentials(client_id="id", client_secret="wrong_secret")

    with pytest.raises(RuntimeError) as excinfo:
        _generate_workspace_token("https://workspace.example", credentials)

    assert mock_urlopen.call_count == 1
    assert "failed with HTTP 401" in str(excinfo.value)
    assert "unauthorized" in str(excinfo.value)


@patch("daft.unity_catalog.auth.urllib.request.urlopen")
def test_generate_token_retries_on_socket_timeout(mock_urlopen) -> None:
    mock_urlopen.side_effect = TimeoutError("timed out")

    credentials = OAuth2Credentials(client_id="test_id", client_secret="test_secret")

    with pytest.raises(RuntimeError) as excinfo:
        _generate_workspace_token("https://workspace.example", credentials)

    assert mock_urlopen.call_count == 3
    assert "timed out" in str(excinfo.value)


@patch("daft.unity_catalog.auth.urllib.request.urlopen")
def test_generate_token_missing_access_token(mock_urlopen) -> None:
    response_body = b'{"token_type": "bearer"}'
    mock_response = MagicMock()
    mock_response.read.return_value = response_body
    mock_response.__enter__.return_value = mock_response
    mock_response.__exit__.return_value = None
    mock_urlopen.return_value = mock_response

    credentials = OAuth2Credentials(client_id="test_id", client_secret="test_secret")

    with pytest.raises(RuntimeError, match="missing expected field 'access_token'"):
        _generate_workspace_token("https://workspace.example", credentials)


def test_is_expired_within_skew_window():
    now = datetime.now(timezone.utc)
    exp = int((now + timedelta(seconds=200)).timestamp())
    assert is_expired(exp, skew_seconds=300) is True


def test_is_not_expired_outside_skew_window():
    now = datetime.now(timezone.utc)
    exp = int((now + timedelta(seconds=400)).timestamp())
    assert is_expired(exp, skew_seconds=300) is False
