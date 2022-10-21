from __future__ import annotations

import dataclasses
import logging
import os
import pathlib
import socket
import subprocess
import sys
import tempfile
from typing import Any, Callable

if sys.version_info < (3, 8):
    from typing_extensions import Literal
else:
    from typing import Literal

import cloudpickle
import pydantic
import requests
import yaml
from loguru import logger
from requests.adapters import HTTPAdapter, Retry

from daft.experimental.serving.backend import AbstractEndpointBackend
from daft.experimental.serving.definitions import Endpoint
from daft.experimental.serving.env import DaftEnv, get_conda_executable

CONFIG_TYPE_ID = Literal["multiprocessing"]

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class MultiprocessingServerEndpoint:
    process: subprocess.Popen | None
    endpoint_name: str
    endpoint_version: int
    port: int
    custom_env: DaftEnv | None
    conda_env_path: tempfile.TemporaryDirectory
    endpoint_pkl_file: tempfile._TemporaryFileWrapper

    def start(self):
        if self.custom_env is None:
            self.process = subprocess.Popen(
                [
                    sys.executable,
                    pathlib.Path(__file__).parent.parent / "static" / "multiprocessing-entrypoint.py",
                    "--endpoint-pkl-file",
                    self.endpoint_pkl_file.name,
                    "--port",
                    str(self.port),
                ]
            )
        else:
            conda_executable = get_conda_executable()
            with tempfile.TemporaryDirectory() as tmpdir:
                environment_yaml = pathlib.Path(tmpdir) / "conda_environment.yml"
                environment_yaml.write_text(yaml.dump(self.custom_env.get_conda_environment()))
                conda_create_env_proc = subprocess.run(
                    [
                        conda_executable,
                        "env",
                        "create",
                        "--prefix",
                        self.conda_env_path.name,
                        "-f",
                        str(environment_yaml),
                    ]
                )
                if conda_create_env_proc.returncode != 0:
                    raise RuntimeError(f"Error while creating Conda environment: {conda_create_env_proc.stderr}")
            self.process = subprocess.Popen(
                [
                    conda_executable,
                    "run",
                    "--prefix",
                    self.conda_env_path.name,
                    sys.executable,
                    pathlib.Path(__file__).parent.parent / "static" / "multiprocessing-entrypoint.py",
                    "--endpoint-pkl-file",
                    self.endpoint_pkl_file.name,
                    "--port",
                    str(self.port),
                ]
            )

        # Wait for process to start serving
        session = requests.Session()
        session.mount((f"http://"), HTTPAdapter(max_retries=Retry(total=5, backoff_factor=0.5)))
        response = session.get(f"http://localhost:{self.port}/healthz")
        if response.status_code != 200:
            raise RuntimeError(f"Failed to start endpoint {self.endpoint_name}: {response.text}")

    def __del__(self):
        self.conda_env_path.cleanup()
        os.remove(self.endpoint_pkl_file.name)
        if self.process is not None:
            self.process.kill()


class MultiprocessingBackendConfig(pydantic.BaseModel):
    """Configuration for the Docker backend"""

    type: CONFIG_TYPE_ID


class MultiprocessingEndpointBackend(AbstractEndpointBackend):
    """Manages Daft Serving endpoints for a Multiprocessing backend"""

    def __init__(self):
        self.multiprocessing_servers: dict[str, MultiprocessingServerEndpoint] = {}

    def __del__(self):
        # Best effort cleanup of all resources used
        for endpoint in self.multiprocessing_servers.values():
            del endpoint

    @staticmethod
    def config_type_id() -> str:
        return "multiprocessing"

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> AbstractEndpointBackend:
        assert config["type"] == cls.config_type_id()
        return cls()

    def _get_free_local_port(self) -> int:
        """Returns a free local port"""
        sock = socket.socket()
        sock.bind(("", 0))
        return int(sock.getsockname()[1])

    def list_endpoints(self) -> list[Endpoint]:
        """Lists all endpoints managed by this endpoint manager"""
        return [
            Endpoint(name=e.endpoint_name, version=e.endpoint_version, addr=f"http://localhost:{e.port}")
            for e in self.multiprocessing_servers.values()
        ]

    def deploy_endpoint(
        self,
        endpoint_name: str,
        endpoint: Callable[[Any], Any],
        custom_env: DaftEnv | None = None,
    ) -> Endpoint:

        # Get correct endpoint_version
        endpoint_version = 1
        if endpoint_name in self.multiprocessing_servers:
            server = self.multiprocessing_servers[endpoint_name]
            endpoint_version = server.endpoint_version + 1
            del self.multiprocessing_servers[endpoint_name]

        port = self._get_free_local_port()

        # Write the endpoint out as a pkl file
        endpoint_pkl_file = tempfile.NamedTemporaryFile()
        endpoint_pkl_file.write(cloudpickle.dumps(endpoint))
        endpoint_pkl_file.flush()

        self.multiprocessing_servers[endpoint_name] = MultiprocessingServerEndpoint(
            process=None,
            endpoint_name=endpoint_name,
            endpoint_version=endpoint_version,
            port=port,
            endpoint_pkl_file=endpoint_pkl_file,
            conda_env_path=tempfile.TemporaryDirectory(),
            custom_env=custom_env,
        )
        self.multiprocessing_servers[endpoint_name].start()

        return Endpoint(
            name=endpoint_name,
            version=endpoint_version,
            addr=f"http://localhost:{port}",
        )
