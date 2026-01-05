from __future__ import annotations

import time
from typing import Generator

import httpx

from .auth import TokenProvider


class AuthProvider(httpx.Auth):  # type: ignore[misc]
    _token_provider: TokenProvider

    def __init__(self, provider: TokenProvider) -> None:
        self._token_provider = provider

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, None, None]:
        token = self._token_provider.get_token()
        request.headers["Authorization"] = f"Bearer {token}"
        print("auth_flow request:", request)
        yield request
