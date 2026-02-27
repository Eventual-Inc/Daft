from __future__ import annotations

from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from collections.abc import Generator

    from .auth import TokenProvider


class AuthProvider(httpx.Auth):  # type: ignore[misc]
    _token_provider: TokenProvider

    def __init__(self, provider: TokenProvider) -> None:
        self._token_provider = provider

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, None, None]:
        token = self._token_provider.get_token()
        request.headers["Authorization"] = f"Bearer {token}"
        yield request
