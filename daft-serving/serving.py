import argparse
import io
import logging
import pathlib
import sys
import tarfile
import tempfile
from typing import Optional

import docker
import fastapi
import uvicorn

HEADER = "\033[95m"
OKBLUE = "\033[94m"
OKCYAN = "\033[96m"
OKGREEN = "\033[92m"
WARNING = "\033[93m"
FAIL = "\033[91m"
ENDC = "\033[0m"
BOLD = "\033[1m"
UNDERLINE = "\033[4m"

colors = {
    "green": OKGREEN,
    "blue": OKBLUE,
}


def _get_color(color: Optional[str]) -> str:
    if color is None:
        return ""
    assert color in colors, f"Supplied color {color} not found"
    return colors[color]


def text(text: str, color: Optional[str] = None):
    return _get_color(color) + text + ENDC


TEMPLATE = """
FROM python:3.8-slim-buster

WORKDIR /app
RUN pip install fastapi uvicorn docker
COPY main.py main.py

CMD ["python", "main.py", "--serve"]
"""

logger = logging.getLogger(__name__)


class App:
    def __init__(self):
        self.app = fastapi.FastAPI()

    def route(self, f):
        self.app.get("/")(f)

    async def __call__(self, scope, receive, send):
        assert scope["type"] == "http"
        await self.app(scope, receive, send)

    def build_and_serve(self):
        # Get all the dependencies and package into a Docker image
        client = docker.from_env()
        tarbytes = io.BytesIO()

        with tempfile.TemporaryDirectory() as td, tarfile.open(fileobj=tarbytes, mode="w") as tar:
            tmpdir = pathlib.Path(td)
            dockerfile_file = tmpdir / "Dockerfile"
            dockerfile_file.touch()
            with open(dockerfile_file, "r+b") as f:
                f.write(TEMPLATE.encode("utf-8"))
                file_len = f.tell()
                f.seek(0)
                tarinfo = tarfile.TarInfo(name="Dockerfile")
                tarinfo.size = file_len
                tar.addfile(tarinfo, fileobj=f)

            with open(pathlib.Path(__file__), "rb") as f:
                f.seek(0, 2)
                file_len = f.tell()
                f.seek(0)
                tarinfo = tarfile.TarInfo(name="main.py")
                tarinfo.size = file_len
                tar.addfile(tarinfo, fileobj=f)

        tarbytes.seek(0)
        print(text(f"DaFt is building your server at {__file__}:app", color="blue"))
        img, build_logs = client.images.build(fileobj=tarbytes, tag="daft-serving:latest", custom_context=True)
        for log in build_logs:
            logger.debug(log)
        print(text(f"Your server was built successfully!", color="green"))
        print(text("Serving at configured runner: localhost:8000"))
        print(text("Press Ctrl+C to exit your server"))
        print()

        # Run the Docker container with uvicorn
        container = None
        try:
            container = client.containers.run(img, ports={"8000/tcp": "8000"}, detach=True, auto_remove=True)
            for line in container.logs(stream=True):
                logger.error(line.strip().decode("utf-8"))
        except KeyboardInterrupt:
            assert container is not None
            print(text(f"\nStopping your server...", color="blue"))
            container.kill()
            sys.exit(0)
        except docker.errors.APIError as e:
            raise ValueError(e)
        finally:
            try:
                if container is not None:
                    container.kill()
            except docker.errors.APIError as e:
                logger.debug(f"Docker API returned error when killing container: {e}")


app = App()


@app.route
def handler():
    return {"hello": "world"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true")
    args = parser.parse_args()

    if args.serve:
        config = uvicorn.Config("main:app", port=8000, host="0.0.0.0", log_level="info")
        server = uvicorn.Server(config)
        server.run()
    else:
        app.build_and_serve()
