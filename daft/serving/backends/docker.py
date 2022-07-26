import io
import logging
import pathlib
import socket
import tarfile
import tempfile
from typing import Any, Callable, List, Literal

import cloudpickle
import docker
import pydantic

from daft.serving.backend import AbstractEndpointBackend
from daft.serving.definitions import Endpoint

CONFIG_TYPE_ID = Literal["docker"]
REQUIREMENTS_TXT_FILENAME = "requirements.txt"
ENDPOINT_PKL_FILENAME = "endpoint.pkl"

logger = logging.getLogger(__name__)


class DockerBackendConfig(pydantic.BaseModel):
    """Configuration for the Docker backend"""

    type: CONFIG_TYPE_ID


def build_image(
    docker_client: docker.DockerClient,
    endpoint_name: str,
    endpoint: Callable[[Any], Any],
    pip_dependencies: List[str] = [],
) -> docker.models.images.Image:
    """Builds a Docker image from the endpoint function"""
    tarbytes = io.BytesIO()
    with tempfile.TemporaryDirectory() as td, tarfile.open(fileobj=tarbytes, mode="w") as tar:
        # Add static files into the Tarfile
        tar.add(pathlib.Path(__file__).parent.parent / "static" / "docker-entrypoint.py", arcname="entrypoint.py")
        tar.add(pathlib.Path(__file__).parent.parent / "static" / "Dockerfile", arcname="Dockerfile")

        # Add pip dependencies into the Tarfile as a requirements.txt
        requirements_txt_file = pathlib.Path(td) / REQUIREMENTS_TXT_FILENAME
        requirements_txt_file.write_text("\n".join(pip_dependencies))
        tar.add(requirements_txt_file, arcname=REQUIREMENTS_TXT_FILENAME)

        # Add the endpoint function to tarfile as a pickle
        pickle_file = pathlib.Path(td) / ENDPOINT_PKL_FILENAME
        with open(pickle_file, "wb") as f:
            f.write(cloudpickle.dumps(endpoint))
        tar.add(pickle_file, arcname=ENDPOINT_PKL_FILENAME)

        # Create a Docker image from the tarfile
        tarbytes.seek(0)
        print(f"DaFt is building your server")
        img, build_logs = docker_client.images.build(
            fileobj=tarbytes, tag=f"daft-serving:{endpoint_name}-latest", custom_context=True
        )
        for log in build_logs:
            logger.debug(log)
        print(f"Your server was built successfully!")
        return img


class DockerEndpointBackend(AbstractEndpointBackend):
    """Manages Daft Serving endpoints for a Docker backend"""

    REQUIREMENTS_TXT_FILENAME = "requirements.txt"
    ENDPOINT_PKL_FILENAME = "endpoint.pkl"

    DAFT_ENDPOINT_VERSION_LABEL = "DAFT_ENDPOINT_VERSION"
    DAFT_ENDPOINT_NAME_LABEL = "DAFT_ENDPOINT_NAME"
    DAFT_ENDPOINT_PORT_LABEL = "DAFT_ENDPOINT_PORT"

    def __init__(self, config: DockerBackendConfig):
        self.docker_client = docker.from_env()

    @staticmethod
    def config_type_id() -> str:
        return "docker"

    @classmethod
    def from_config(cls, config: DockerBackendConfig) -> AbstractEndpointBackend:
        return cls(config)

    def _run_container(self, endpoint_name: str, img: docker.models.images.Image) -> docker.models.containers.Container:
        """Runs a Docker container from the given image"""
        version = 0
        port = self._get_free_local_port()

        # Check and remove existing container with the same name
        containers = [
            c
            for c in self._get_daft_serving_containers()
            if c.labels[DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL] == endpoint_name
        ]
        if containers:
            assert len(containers) == 1, "Multiple endpoints with the same name are not supported"
            match = containers[0]
            old_version = int(match.labels[DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL])
            version = old_version + 1
            port = match.labels[DockerEndpointBackend.DAFT_ENDPOINT_PORT_LABEL]
            print(f"Tearing down existing endpoint {endpoint_name}/v{old_version}")
            match.stop()

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

    def _get_daft_serving_containers(self) -> List[docker.models.containers.Container]:
        """Returns all Daft Serving containers"""
        return [
            container
            for container in self.docker_client.containers.list()
            if container.labels.get(DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL)
        ]

    def list_endpoints(self) -> List[Endpoint]:
        """Lists all endpoints managed by this endpoint manager"""
        containers = self._get_daft_serving_containers()
        return [
            Endpoint(
                name=c.labels[DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL],
                addr=f"http://localhost:{c.labels['DAFT_ENDPOINT_PORT']}",
                version=int(c.labels[DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL]),
            )
            for c in containers
        ]

    def deploy_endpoint(
        self, endpoint_name: str, endpoint: Callable[[Any], Any], pip_dependencies: List[str] = []
    ) -> Endpoint:
        img = build_image(self.docker_client, endpoint_name, endpoint, pip_dependencies=pip_dependencies)
        container = self._run_container(endpoint_name, img)
        return Endpoint(
            name=endpoint_name,
            addr=f"http://localhost:{container.labels['DAFT_ENDPOINT_PORT']}",
            version=int(container.labels[DockerEndpointBackend.DAFT_ENDPOINT_VERSION_LABEL]),
        )
