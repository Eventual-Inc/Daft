from __future__ import annotations

from unittest.mock import patch

import pytest

from daft.unity_catalog.auth import OAuth2Credentials, OAuth2TokenProvider


@pytest.fixture
def credentials() -> OAuth2Credentials:
    return OAuth2Credentials(client_id="client_id", client_secret="client_secret")


@pytest.fixture
def provider(credentials: OAuth2Credentials) -> OAuth2TokenProvider:
    return OAuth2TokenProvider("https://workspace.example", credentials)


def test_get_token_caching_logic(provider: OAuth2TokenProvider) -> None:
    with patch("daft.unity_catalog.auth._generate_workspace_token") as mock_generate:
        with patch("daft.unity_catalog.auth.jwt_expiration", return_value=9999999999):
            with patch("daft.unity_catalog.auth.is_expired", return_value=False):
                mock_generate.return_value = "token_1"
                first = provider.get_token()
                second = provider.get_token()

    assert first == "token_1"
    assert second == "token_1"
    assert mock_generate.call_count == 1


def test_get_token_refreshes_when_expired(provider: OAuth2TokenProvider) -> None:
    with patch("daft.unity_catalog.auth._generate_workspace_token") as mock_generate:
        with patch("daft.unity_catalog.auth.jwt_expiration", return_value=1000):
            mock_generate.return_value = "token_1"
            provider.get_token()

        with patch("daft.unity_catalog.auth.is_expired", return_value=True):
            with patch("daft.unity_catalog.auth.jwt_expiration", return_value=9999999999):
                mock_generate.return_value = "token_2"
                second = provider.get_token()

    assert second == "token_2"
    assert mock_generate.call_count == 2


def test_get_token_failure_after_retries(provider: OAuth2TokenProvider) -> None:
    error_msg = "failed after 3 attempts: HTTP Error 500"
    with patch("daft.unity_catalog.auth._generate_workspace_token", side_effect=RuntimeError(error_msg)):
        with pytest.raises(RuntimeError, match="failed after 3 attempts"):
            provider.get_token()


def test_get_token_raises_if_token_is_none(provider: OAuth2TokenProvider) -> None:
    with patch("daft.unity_catalog.auth.OAuth2TokenProvider._token_expired", return_value=True):
        with patch("daft.unity_catalog.auth.OAuth2TokenProvider._refresh"):
            with pytest.raises(RuntimeError, match="Token refresh failed to set token"):
                provider.get_token()
