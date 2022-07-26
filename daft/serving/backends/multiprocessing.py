import dataclasses
import logging
import multiprocessing
import pickle
import socket
from typing import Any, Callable, Dict, List, Literal

import cloudpickle
import fastapi
import pydantic
import requests
import uvicorn
from requests.adapters import HTTPAdapter, Retry

from daft.serving.backend import AbstractEndpointBackend
from daft.serving.definitions import Endpoint

CONFIG_TYPE_ID = Literal["multiprocessing"]

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class MultiprocessingServerEndpoint:
    process: multiprocessing.Process
    endpoint_name: str
    endpoint_version: int
    port: int


def run_process(endpoint_pkl: bytes, port: int):
    app = fastapi.FastAPI()

    # Load cloudpickled function
    endpoint = pickle.loads(endpoint_pkl)
    app.get("/")(endpoint)

    def healthcheck():
        return {"status": "ok"}

    app.get("/healthz")(healthcheck)

    config = uvicorn.Config(app=app, port=port)
    server = uvicorn.Server(config)
    server.run()


class MultiprocessingBackendConfig(pydantic.BaseModel):
    """Configuration for the Docker backend"""

    type: CONFIG_TYPE_ID


class MultiprocessingEndpointBackend(AbstractEndpointBackend):
    """Manages Daft Serving endpoints for a Multiprocessing backend"""

    def __init__(self, config: MultiprocessingBackendConfig):
        self.multiprocessing_servers: Dict[str, MultiprocessingServerEndpoint] = {}

    @staticmethod
    def config_type_id() -> str:
        return "multiprocessing"

    @classmethod
    def from_config(cls, config: MultiprocessingBackendConfig) -> AbstractEndpointBackend:
        return cls(config)

    def _get_free_local_port(self) -> int:
        """Returns a free local port"""
        sock = socket.socket()
        sock.bind(("", 0))
        return int(sock.getsockname()[1])

    def list_endpoints(self) -> List[Endpoint]:
        """Lists all endpoints managed by this endpoint manager"""
        return [
            Endpoint(name=e.endpoint_name, version=e.endpoint_version, addr=f"http://localhost:{e.port}")
            for e in self.multiprocessing_servers.values()
        ]

    def deploy_endpoint(
        self, endpoint_name: str, endpoint: Callable[[Any], Any], pip_dependencies: List[str] = []
    ) -> Endpoint:
        endpoint_version = 1
        if endpoint_name in self.multiprocessing_servers:
            server = self.multiprocessing_servers[endpoint_name]
            endpoint_version = server.endpoint_version + 1
            server.process.kill()
            del self.multiprocessing_servers[endpoint_name]

        port = self._get_free_local_port()
        endpoint = cloudpickle.dumps(endpoint)
        process = multiprocessing.Process(target=run_process, args=(endpoint, port))
        process.daemon = True
        self.multiprocessing_servers[endpoint_name] = MultiprocessingServerEndpoint(
            process=process,
            endpoint_name=endpoint_name,
            endpoint_version=endpoint_version,
            port=port,
        )
        process.start()

        # Wait for process to start serving
        session = requests.Session()
        session.mount((f"http://"), HTTPAdapter(max_retries=Retry(total=5, backoff_factor=0.5)))
        response = session.get(f"http://localhost:{port}/healthz")
        if response.status_code != 200:
            raise RuntimeError(f"Failed to start endpoint {endpoint_name}: {response.text}")

        return Endpoint(
            name=endpoint_name,
            version=endpoint_version,
            addr=f"http://localhost:{port}",
        )
