from __future__ import annotations

import dataclasses

import boto3
import docker
import pytest

import daft
from tests.remote_io.conftest import YieldFixture


@dataclasses.dataclass(frozen=True)
class S3Config:
    endpoint: str
    key_id: str
    access_key: str

    def boto3_resource(self):
        return boto3.resource(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.key_id,
            aws_secret_access_key=self.access_key,
        )


@pytest.fixture(scope="session")
def minio_config(tmp_path_factory) -> YieldFixture[S3Config]:
    """Provides a mock S3 implementation running locally with MinIO on Docker

    NOTE: This fixture will skip tests if it cannot find a local Docker daemon

    Yields an `S3Config` object which can be used to create an S3 connection to the running MinIO service
    """
    try:
        docker_client = docker.from_env()
    except docker.errors.DockerException:
        pytest.skip("MinIO tests need a running local Docker instance.")

    KEY_ID = "daft-pytest"
    ACCESS_KEY = "daft-is-the-coolest"

    tmpdir = tmp_path_factory.mktemp("data")
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


@pytest.fixture(scope="function")
def minio_image_data_fixture(minio_config, image_data) -> YieldFixture[tuple[S3Config, list[str]]]:
    """Populates the minio session with some fake data and yields (S3Config, paths)"""
    s3 = minio_config.boto3_resource()

    # Add some images into `s3://image-bucket`
    BUCKET = "image-bucket"
    bucket = s3.Bucket(BUCKET)
    bucket.create()
    urls = []
    for i in range(10):
        key = f"{i}.jpeg"
        bucket.put_object(Body=image_data, Key=key)
        urls.append(f"s3://{BUCKET}/{key}")

    yield (minio_config, urls)

    # Cleanup data
    bucket.objects.all().delete()
    bucket.delete()


def test_url_download_minio_custom_s3fs(minio_image_data_fixture, image_data):
    import s3fs

    s3_config, urls = minio_image_data_fixture
    fs = s3fs.S3FileSystem(
        endpoint_url=s3_config.endpoint,
        key=s3_config.key_id,
        password=s3_config.access_key,
    )

    data = {"urls": urls}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(fs=fs))

    assert df.to_pydict() == {**data, "data": [image_data for _ in range(len(urls))]}
