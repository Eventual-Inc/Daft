import io
import pathlib
import tarfile
import tempfile

import docker

TEMPLATE = """
FROM python:3.8-slim-buster

WORKDIR /app
RUN pip install fastapi uvicorn
COPY main.py main.py

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
"""

APP_TEMPLATE = """
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}
"""


class App:
    def route(self, f):
        self.f = f

    def serve(self):
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
            main_file = tmpdir / "main.py"
            main_file.touch()
            with open(main_file, "r+b") as f:
                f.write(APP_TEMPLATE.encode("utf-8"))
                file_len = f.tell()
                f.seek(0)
                tarinfo = tarfile.TarInfo(name="main.py")
                tarinfo.size = file_len
                tar.addfile(tarinfo, fileobj=f)

        tarbytes.seek(0)
        img, build_logs = client.images.build(fileobj=tarbytes, tag="daft-serving:latest", custom_context=True)
        for log in build_logs:
            print(log)

        # Run the Docker container with uvicorn
        try:
            container = client.containers.run(img, ports={"8000/tcp": "8000"}, detach=True)
            for line in container.logs(stream=True):
                print(line.strip())
        finally:
            container.kill()


app = App()


@app.route
def handler(row):
    return row


if __name__ == "__main__":
    app.serve()
