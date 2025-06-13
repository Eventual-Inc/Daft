from __future__ import annotations

from daft.daft import PyEchoClient


class EchoClient:

    _client: PyEchoClient

    def __init__(self, client: PyEchoClient):
        self._client = client

    @staticmethod
    def connect(endpoint: str) -> EchoClient:
        client = PyEchoClient.connect(endpoint)
        return EchoClient(client)

    
    def describe(self, df) -> str:
        builder = df._builder._builder
        return self._client.describe(builder)
