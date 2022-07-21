import io
import logging
import pathlib
import tarfile
import tempfile
from typing import Any, Callable, List

import cloudpickle
import docker

REQUIREMENTS_TXT_FILENAME = "requirements.txt"
ENDPOINT_PKL_FILENAME = "endpoint.pkl"

logger = logging.getLogger(__name__)


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
        tar.add(pathlib.Path(__file__).parent / "static" / "docker-entrypoint.py", arcname="entrypoint.py")
        tar.add(pathlib.Path(__file__).parent / "static" / "Dockerfile", arcname="Dockerfile")

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
