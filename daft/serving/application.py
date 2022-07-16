import io
import logging
import os
import pathlib
import sys
import tarfile
import tempfile
from typing import Any, Callable, Optional

import docker
import fastapi

from daft.serving import pickle

logger = logging.getLogger(__name__)

ENDPOINT_PKL_FILENAME = "endpoint.pkl"


class App:
    def __init__(self):
        self.app = fastapi.FastAPI()
        self.registered_endpoint = None

        # TODO(jay): This breaks when not running as a Python script and needs to be extended for
        # running in an interactive environment
        self.main_filepath = os.path.realpath(sys.argv[0])

    def endpoint(self, func) -> Callable[[Any], Any]:
        """Decorate a function to be the application endpoint"""
        assert self.registered_endpoint is None, "Endpoint already defined"
        self.registered_endpoint = func
        return func

    def deploy(self) -> None:
        """Deploy the application on the configured backend and block on the server until it is interrupted"""
        assert self.registered_endpoint is not None, "Endpoint not defined"
        tarbytes = io.BytesIO()
        with tempfile.TemporaryDirectory() as td, tarfile.open(fileobj=tarbytes, mode="w") as tar:
            # Add static files into the Tarfile
            tar.add(pathlib.Path(__file__).parent / "entrypoint.py", arcname="entrypoint.py")
            tar.add(pathlib.Path(__file__).parent / "Dockerfile", arcname="Dockerfile")

            # Add the endpoint function to tarfile as a pickle
            pickle_file = pathlib.Path(td) / ENDPOINT_PKL_FILENAME
            with open(pickle_file, "wb") as f:
                pickle.dump_function(self.registered_endpoint, f)
            tar.add(pickle_file, arcname=ENDPOINT_PKL_FILENAME)

            # Create a Docker image from the tarfile
            client = docker.from_env()
            tarbytes.seek(0)
            print(f"DaFt is building your server at {__file__}:app")
            img, build_logs = client.images.build(fileobj=tarbytes, tag="daft-serving:latest", custom_context=True)
            for log in build_logs:
                logger.debug(log)
            print(f"Your server was built successfully!")
            print("Serving at configured runner: localhost:8000")
            print("Press Ctrl+C to exit your server")

            # Run the Docker container with uvicorn
            container: Optional[docker.models.containers.Container] = None
            try:
                container = client.containers.run(img, ports={"8000/tcp": "8000"}, detach=True, auto_remove=True)
                for line in container.logs(stream=True):
                    logger.error(line.strip().decode("utf-8"))
            except KeyboardInterrupt:
                assert container is not None, "Container should be running on KeyboardInterrupt"
                print(f"\nStopping your server...")
                container.kill()
                container = None
            except docker.errors.APIError as e:
                raise RuntimeError(e)
            finally:
                if container is not None:
                    try:
                        container.kill()
                    except docker.errors.APIError as e:
                        logger.debug(f"Docker API returned error when killing container: {e}")
