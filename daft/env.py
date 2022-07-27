from __future__ import annotations

import io
import pathlib
import re
import sys
import tarfile
import tempfile
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional

import cloudpickle
import docker
from loguru import logger

ENDPOINT_PKL_FILENAME = "endpoint.pkl"
ENTRYPOINT_FILE_NAME = "entrypoint.py"
VENV_PATH = "/opt/venv"
BASE_IMAGE_BUILD_TARGET = "daft_env_base"
SERVING_IMAGE_BUILD_TARGET = "daft_serving"


@dataclass(frozen=True)
class DaftEnv:
    python_version: str = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    requirements_txt: Optional[str] = None
    pip_packages: List[str] = field(default_factory=list)
    local_packages: List[str] = field(default_factory=list)


def daft_env_image_dockerfile(env: DaftEnv, tar: tarfile.TarFile) -> List[str]:
    manifest = [
        f"FROM python:{env.python_version}-slim-buster AS {BASE_IMAGE_BUILD_TARGET}",
        "WORKDIR /scratch",
        # Use a Virtual Environment for installing dependencies, and we can then copy all installed dependencies
        # in subsequent build stages from /opt/venv
        f"RUN python -m venv {VENV_PATH}",
        f'ENV PATH="{VENV_PATH}/bin:$PATH"',
    ]

    if env.requirements_txt is not None:
        manifest.extend(
            [
                f"COPY requirements.txt requirements.txt",
                "RUN pip install -r requirements.txt",
            ]
        )
        tar.add(env.requirements_txt, arcname="requirements.txt")
    if env.pip_packages:
        manifest.extend(
            [
                f"RUN pip install {' '.join(env.pip_packages)}",
            ]
        )
    if env.local_packages:
        for local_package_path in env.local_packages:
            pkg_folder_name = pathlib.Path(local_package_path).name
            manifest.extend(
                [
                    f"COPY {pkg_folder_name} {pkg_folder_name}",
                    f"RUN pip install /scratch/{pkg_folder_name}",
                ]
            )
            tar.add(local_package_path, arcname=pkg_folder_name)

    return manifest


def serving_image_dockerfile(env: DaftEnv, tar: tarfile.TarFile, endpoint: Callable[[Any], Any]) -> List[str]:
    with tempfile.TemporaryDirectory() as td:
        # Add the endpoint function to tarfile as a pickle
        pickle_file = pathlib.Path(td) / ENDPOINT_PKL_FILENAME
        with open(pickle_file, "wb") as f:
            f.write(cloudpickle.dumps(endpoint))
        tar.add(pickle_file, arcname=ENDPOINT_PKL_FILENAME)

        # Add entrypoint file to tarfile
        tar.add(
            pathlib.Path(__file__).parent / "serving" / "static" / "docker-entrypoint.py", arcname=ENTRYPOINT_FILE_NAME
        )

    manifest = [
        f"FROM python:{env.python_version}-slim-buster AS {SERVING_IMAGE_BUILD_TARGET}",
        f"COPY --from={BASE_IMAGE_BUILD_TARGET} {VENV_PATH} {VENV_PATH}",
        f'ENV PATH="{VENV_PATH}/bin:$PATH"',
        "RUN pip install fastapi uvicorn cloudpickle",
        "WORKDIR /app",
        f"COPY {ENTRYPOINT_FILE_NAME} {ENTRYPOINT_FILE_NAME}",
        f"COPY {ENDPOINT_PKL_FILENAME} {ENDPOINT_PKL_FILENAME}",
        f'CMD ["python", "{ENTRYPOINT_FILE_NAME}", "--endpoint-pkl-file={ENDPOINT_PKL_FILENAME}"]',
    ]

    return manifest


def build_serving_docker_image(
    env: DaftEnv, endpoint_name: str, endpoint: Callable[[Any], Any], platform: str = "linux/amd64"
) -> docker.models.images.Image:
    docker_client = docker.from_env()

    tarbytes = io.BytesIO()
    with tarfile.open(fileobj=tarbytes, mode="w") as tar, tempfile.TemporaryDirectory() as td:
        base_manifest = daft_env_image_dockerfile(env, tar)
        serving_manifest = serving_image_dockerfile(env, tar, endpoint)

        dockerfile = pathlib.Path(td) / "Dockerfile"
        with open(dockerfile, "w") as f:
            f.write("\n".join(base_manifest + serving_manifest))
        tar.add(dockerfile, arcname="Dockerfile")

        # Build the image
        tarbytes.seek(0)
        response = docker_client.api.build(
            fileobj=tarbytes,
            custom_context=True,
            tag=f"daft-serving:{endpoint_name}",
            decode=True,
            platform=platform,
            target=SERVING_IMAGE_BUILD_TARGET,
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
