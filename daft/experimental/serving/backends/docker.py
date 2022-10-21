from __future__ import annotations

import io
import pathlib
import re
import socket
import sys
import tarfile
import tempfile
from typing import Any, Callable

if sys.version_info < (3, 8):
    from typing_extensions import Literal
else:
    from typing import Literal

import cloudpickle
import docker
import requests
import yaml
from loguru import logger
from requests.adapters import HTTPAdapter, Retry

from daft.experimental.serving.backend import AbstractEndpointBackend
from daft.experimental.serving.definitions import Endpoint
from daft.experimental.serving.env import DaftEnv, get_docker_client

CONFIG_TYPE_ID = Literal["docker"]
ENDPOINT_PKL_FILENAME = "endpoint.pkl"
ENTRYPOINT_FILE_NAME = "entrypoint.py"
VENV_PATH = "/opt/venv"
CONDA_ENV_PATH = "conda_environment.yml"
SERVING_IMAGE_DOCKER_MANIFEST = f"""
FROM continuumio/miniconda3:4.12.0
WORKDIR /scratch
COPY {CONDA_ENV_PATH} {CONDA_ENV_PATH}
RUN conda env create --prefix {VENV_PATH} -f {CONDA_ENV_PATH} --quiet
WORKDIR /app
COPY {ENTRYPOINT_FILE_NAME} {ENTRYPOINT_FILE_NAME}
COPY {ENDPOINT_PKL_FILENAME} {ENDPOINT_PKL_FILENAME}
CMD ["conda", "run", "--prefix", "{VENV_PATH}", "python", "{ENTRYPOINT_FILE_NAME}", "--endpoint-pkl-file={ENDPOINT_PKL_FILENAME}"]
"""


class DockerEndpointBackend(AbstractEndpointBackend):
    """Manages Daft Serving endpoints for a Docker backend"""

    DAFT_ENDPOINT_VERSION_LABEL = "DAFT_ENDPOINT_VERSION"
    DAFT_ENDPOINT_NAME_LABEL = "DAFT_ENDPOINT_NAME"
    DAFT_ENDPOINT_PORT_LABEL = "DAFT_ENDPOINT_PORT"

    def __init__(self):
        self.docker_client = get_docker_client()

    @staticmethod
    def config_type_id() -> str:
        return "docker"

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> AbstractEndpointBackend:
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

    def _get_daft_serving_containers(self, running_only: bool = True) -> list[docker.models.containers.Container]:
        """Returns all Daft Serving containers"""
        return [
            container
            for container in self.docker_client.containers.list(all=not running_only)
            if container.labels.get(DockerEndpointBackend.DAFT_ENDPOINT_NAME_LABEL)
        ]

    def list_endpoints(self) -> list[Endpoint]:
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
        custom_env: DaftEnv | None = None,
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


def build_serving_docker_image(
    env: DaftEnv, endpoint_name: str, endpoint: Callable[[Any], Any]
) -> docker.models.images.Image:
    """Builds the image to be served locally"""
    docker_client = docker.from_env()

    # Extend the base conda environment with serving dependencies
    conda_env = env.get_conda_environment()
    conda_env["dependencies"].extend(["fastapi", "uvicorn", "cloudpickle"])

    tarbytes = io.BytesIO()
    with tarfile.open(fileobj=tarbytes, mode="w") as tar, tempfile.TemporaryDirectory() as td:
        tmpdir = pathlib.Path(td)

        # Add conda env as a YAML file
        conda_env_file = tmpdir / CONDA_ENV_PATH
        conda_env_file.write_text(yaml.dump(conda_env))
        tar.add(conda_env_file, arcname=CONDA_ENV_PATH)

        # Add the endpoint function to tarfile as a pickle
        pickle_file = tmpdir / ENDPOINT_PKL_FILENAME
        pickle_file.write_bytes(cloudpickle.dumps(endpoint))
        tar.add(pickle_file, arcname=ENDPOINT_PKL_FILENAME)

        # Add entrypoint file to tarfile
        tar.add(pathlib.Path(__file__).parent.parent / "static" / "docker-entrypoint.py", arcname=ENTRYPOINT_FILE_NAME)

        # Add Dockerfile to tarfile
        dockerfile = tmpdir / "Dockerfile"
        dockerfile.write_text(SERVING_IMAGE_DOCKER_MANIFEST)
        tar.add(dockerfile, arcname="Dockerfile")

        # Build the image
        tarbytes.seek(0)
        response = docker_client.api.build(
            fileobj=tarbytes,
            custom_context=True,
            tag=f"daft-serving:{endpoint_name}",
            decode=True,
        )
        for msg in response:
            if "stream" in msg:
                logger.debug(msg["stream"])
                match = re.search(r"(^Successfully built |sha256:)([0-9a-f]+)$", msg["stream"])
                if match:
                    image_id = match.group(2)
                    print(f"Built image: {image_id}")
                    return docker_client.images.get(image_id)
            if "aux" in msg:
                logger.debug(msg["aux"])
        raise RuntimeError("Failed to build base Docker image, check debug logs for more info")
