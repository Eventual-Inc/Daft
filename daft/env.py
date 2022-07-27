from __future__ import annotations

import io
import pathlib
import re
import sys
import tarfile
import tempfile
from typing import Any, Callable, Optional

import cloudpickle
import docker
from loguru import logger

ENDPOINT_PKL_FILENAME = "endpoint.pkl"


class DaftEnv:
    """Reproduces the user's current Python environment as a Docker image"""

    def __init__(self, docker_client: Optional[docker.DockerClient] = None):
        self.docker_client = docker_client if docker_client is not None else docker.from_env()
        self._manifest = [
            # Match the user's current Python version
            f"FROM python:{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}-slim-buster",
            # Install base python dependencies required to run the server
            "WORKDIR /scratch",
            "RUN pip install --upgrade pip",
            "RUN pip install fastapi uvicorn cloudpickle",
        ]
        self._tarbytes = io.BytesIO()
        self._tar = tarfile.open(fileobj=self._tarbytes, mode="w")

        # Add static files to the Docker context
        self._tar.add(
            pathlib.Path(__file__).parent / "serving" / "static" / "docker-entrypoint.py", arcname="entrypoint.py"
        )

    def with_requirements_txt(self, requirements_txt_path: str) -> DaftEnv:
        """Install requirements as specified in a valid requirements.txt file"""
        self._manifest.extend(
            [
                f"COPY requirements.txt requirements.txt",
                "RUN pip install -r requirements.txt",
            ]
        )
        self._tar.add(requirements_txt_path, arcname="requirements.txt")
        return self

    def with_local_package(self, package_path: str) -> DaftEnv:
        """Install a local package (folder with a setup.py or pyproject.toml)"""
        pkg_folder_name = pathlib.Path(package_path).name
        self._manifest.extend(
            [
                f"COPY {pkg_folder_name} {pkg_folder_name}",
                f"RUN pip install /scratch/{pkg_folder_name}",
            ]
        )
        self._tar.add(package_path, arcname=pkg_folder_name)
        return self

    def with_pip_package(self, pip_package: str) -> DaftEnv:
        """Install a pip package"""
        self._manifest.extend(
            [
                f"RUN pip install {pip_package}",
            ]
        )
        return self

    def build_image(
        self,
        endpoint_name: str,
        endpoint: Callable[[Any], Any],
        platform: str = "linux/amd64",
    ):
        """Builds the Docker image"""
        with tempfile.TemporaryDirectory() as td:
            # Add the endpoint function to tarfile as a pickle
            pickle_file = pathlib.Path(td) / ENDPOINT_PKL_FILENAME
            with open(pickle_file, "wb") as f:
                f.write(cloudpickle.dumps(endpoint))
            self._tar.add(pickle_file, arcname=ENDPOINT_PKL_FILENAME)
            self._manifest.extend(
                [
                    "WORKDIR /app",
                    "COPY entrypoint.py entrypoint.py",
                    "COPY endpoint.pkl /app/endpoint.pkl",
                    'CMD ["python", "entrypoint.py", "--endpoint-pkl-file=/app/endpoint.pkl"]',
                ]
            )

            # Add the Dockerfile to tarfile
            self._manifest.extend(
                [
                    "WORKDIR /app",
                ]
            )
            dockerfile = pathlib.Path(td) / "Dockerfile"
            with open(dockerfile, "w") as f:
                f.write("\n".join(self._manifest))
            self._tar.add(dockerfile, arcname="Dockerfile")

            # Create a Docker image from the tarfile
            self._tarbytes.seek(0)
            print(f"DaFt is building your server")

            response = self.docker_client.api.build(
                fileobj=self._tarbytes,
                custom_context=True,
                tag=f"daft-serving:{endpoint_name}",
                decode=True,
                platform=platform,
            )
            for msg in response:
                if "stream" in msg:
                    logger.debug(msg["stream"])
                    match = re.search(r"(^Successfully built |sha256:)([0-9a-f]+)$", msg["stream"])
                    if match:
                        image_id = match.group(2)
                        print(f"Built image: {image_id}")
                        return self.docker_client.images.get(image_id)
                if "aux" in msg:
                    logger.debug(msg["aux"])
            raise RuntimeError("Failed to build Docker image, check debug logs for more info")
