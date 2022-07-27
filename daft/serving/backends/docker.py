import pathlib
import socket
from typing import Any, Callable, Dict, List, Literal, Optional

import docker
import requests
from requests.adapters import HTTPAdapter, Retry

from daft.env import DaftEnv, build_serving_docker_image
from daft.serving.backend import AbstractEndpointBackend
from daft.serving.definitions import Endpoint

CONFIG_TYPE_ID = Literal["docker"]
REQUIREMENTS_TXT_FILENAME = "requirements.txt"
ENDPOINT_PKL_FILENAME = "endpoint.pkl"
WORKING_DIR_PATH = pathlib.Path("working_dir")
MODULES_PATH = pathlib.Path("site-packages")


class DockerEndpointBackend(AbstractEndpointBackend):
    """Manages Daft Serving endpoints for a Docker backend"""

    REQUIREMENTS_TXT_FILENAME = "requirements.txt"
    ENDPOINT_PKL_FILENAME = "endpoint.pkl"

    DAFT_ENDPOINT_VERSION_LABEL = "DAFT_ENDPOINT_VERSION"
    DAFT_ENDPOINT_NAME_LABEL = "DAFT_ENDPOINT_NAME"
    DAFT_ENDPOINT_PORT_LABEL = "DAFT_ENDPOINT_PORT"

    def __init__(self):
        self.docker_client = docker.from_env()

    @staticmethod
    def config_type_id() -> str:
        return "docker"

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> AbstractEndpointBackend:
        assert config["type"] == cls.config_type_id()
        return cls()

    def _run_container(self, endpoint_name: str, img: docker.models.images.Image) -> docker.models.containers.Container:
        """Runs a Docker container from the given image"""
        version = 0
        port = self._get_free_local_port()

        # Check and remove existing container with the same name
        containers = [
            c
            for c in self._get_daft_serving_containers(running_only=False)
            if c.labels[DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL] == endpoint_name
        ]
        containers = sorted(containers, key=lambda c: int(c.labels[DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL]))
        if containers:
            match = containers[-1]
            old_version = int(match.labels[DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL])
            version = old_version + 1
            port = match.labels[DockerEndpointBackend.DAFT_ENDPOINT_PORT_LABEL]
            for container in containers:
                if container.status == "running":
                    print(
                        f"Tearing down existing endpoint {endpoint_name}/v{container.labels[DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL]}"
                    )
                    container.stop()

        try:
            container = self.docker_client.containers.run(
                img,
                name=f"daft-endpoint-{endpoint_name}-v{version}",
                ports={f"8000/tcp": str(port)},
                detach=True,
                auto_remove=False,
                labels={
                    DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL: endpoint_name,
                    DockerEndpointBackend.DAFT_ENDPOINT_PORT_LABEL: str(port),
                    DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL: str(version),
                },
            )
            print(f"Started endpoint {endpoint_name}/v{version} at http://localhost:{port}")
            return container
        except docker.errors.APIError as e:
            raise RuntimeError(e)

    def _get_free_local_port(self) -> int:
        """Returns a free local port"""
        sock = socket.socket()
        sock.bind(("", 0))
        return int(sock.getsockname()[1])

    def _get_daft_serving_containers(self, running_only: bool = True) -> List[docker.models.containers.Container]:
        """Returns all Daft Serving containers"""
        return [
            container
            for container in self.docker_client.containers.list(all=not running_only)
            if container.labels.get(DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL)
        ]

    def list_endpoints(self) -> List[Endpoint]:
        """Lists all endpoints managed by this endpoint manager"""
        containers = self._get_daft_serving_containers(running_only=True)
        return [
            Endpoint(
                name=c.labels[DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL],
                addr=f"http://localhost:{c.labels['DAFT_ENDPOINT_PORT']}",
                version=int(c.labels[DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL]),
            )
            for c in containers
        ]

    def deploy_endpoint(
        self,
        endpoint_name: str,
        endpoint: Callable[[Any], Any],
        custom_env: Optional[DaftEnv] = None,
    ) -> Endpoint:
        img = build_serving_docker_image(custom_env if custom_env is not None else DaftEnv(), endpoint_name, endpoint)
        container = self._run_container(endpoint_name, img)
        addr = f"http://localhost:{container.labels['DAFT_ENDPOINT_PORT']}"

        # Wait for process to start serving
        session = requests.Session()
        session.mount((f"http://"), HTTPAdapter(max_retries=Retry(total=5, backoff_factor=0.5)))
        try:
            response = session.get(f"{addr}/healthz")
        except requests.exceptions.ConnectionError:
            raise RuntimeError(
                f"Container for endpoint {endpoint_name} unable to start:\n{container.logs().decode('utf-8')}"
            )
        if response.status_code != 200:
            raise RuntimeError(f"Failed to start endpoint {endpoint_name}: {response.text}")

        return Endpoint(
            name=endpoint_name,
            addr=addr,
            version=int(container.labels[DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL]),
        )
