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


#class RefreshingTokenAuth(httpx.Auth):
#    requires_request_body = True  # if you need body for refresh; else omit

#    def __init__(self, token_url, client_id, client_secret):
#        self._token_url = token_url
#        self._client_id = client_id
#        self._client_secret = client_secret
#        self._token = None
#        self._expires_at = 0

#    def _refresh(self):
#        r = httpx.post(
#            self._token_url,
#            data={"client_id": self._client_id, "client_secret": self._client_secret},
#        )
#        r.raise_for_status()
#        data = r.json()
#        self._token = data["access_token"]
#        self._expires_at = time.time() + data.get("expires_in", 3600) - 30  # small skew

#    def auth_flow(self, request):
#        # if self._token is None or time.time() >= self._expires_at:
#        #    self._refresh()
#        # request.headers["Authorization"] = f"Bearer {self._token}"
#        print("auth_flow request:", request)
#        yield request


# client = httpx.Client(auth=RefreshingTokenAuth("https://auth/token", "id", "secret"))
