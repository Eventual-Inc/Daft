from __future__ import annotations

import dataclasses

import boto3
import docker
import pytest


@dataclasses.dataclass(frozen=True)
class S3Config:
    endpoint: str
    key_id: str
    access_key: str

    def boto3_resource(self):
        return boto3.resource(
            "s3",
            endpoint_url=self.endpoint,
            # config=boto3.session.Config(signature_version='s3v4'),
            aws_access_key_id=self.key_id,
            aws_secret_access_key=self.access_key,
        )


@pytest.fixture(scope="session")
def minio_config(tmp_path_factory):
    KEY_ID = "daft-pytest"
    ACCESS_KEY = "daft-is-the-coolest"

    tmpdir = tmp_path_factory.mktemp("data")
    docker_client = docker.from_env()
    container = docker_client.containers.run(
        "quay.io/minio/minio:RELEASE.2023-06-19T19-52-50Z",
        'server /data --console-address ":9090"',
        ports={
            "9000/tcp": 9000,
            "9090/tcp": 9090,
        },
        name="pytest-minio",
        volumes={str(tmpdir): {"bind": "/data", "mode": "rw"}},
        environment={
            "MINIO_ROOT_USER": KEY_ID,
            "MINIO_ROOT_PASSWORD": ACCESS_KEY,
        },
        detach=True,
    )
    try:
        yield S3Config(
            endpoint="http://localhost:9000",
            key_id=KEY_ID,
            access_key=ACCESS_KEY,
        )
    finally:
        container.kill()
        container.remove()
