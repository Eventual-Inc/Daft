from __future__ import annotations

import io
import pathlib
from typing import Generator, TypeVar

import boto3
import numpy as np
import pytest
from PIL import Image

import daft

T = TypeVar("T")

YieldFixture = Generator[T, None, None]


@pytest.fixture(scope="session")
def minio_io_config() -> daft.io.IOConfig:
    return daft.io.IOConfig(
        s3=daft.io.S3Config(
            endpoint_url="http://127.0.0.1:9000",
            key_id="minioadmin",
            access_key="minioadmin",
        )
    )


@pytest.fixture(scope="session")
def aws_public_s3_config() -> daft.io.IOConfig:
    return daft.io.IOConfig(
        s3=daft.io.S3Config(
            # NOTE: no keys or endpoints specified for an AWS public s3 bucket
            region_name="us-west-2",
        )
    )


@pytest.fixture(scope="session")
def nginx_config() -> tuple[str, pathlib.Path]:
    """Returns the (nginx_server_url, static_files_tmpdir) as a tuple"""
    return (
        "http://127.0.0.1:8080",
        pathlib.Path("/tmp/daft-integration-testing/nginx"),
    )


@pytest.fixture(scope="session")
def image_data() -> YieldFixture[bytes]:
    """A small bit of fake image JPEG data"""
    bio = io.BytesIO()
    image = Image.fromarray(np.ones((3, 3)).astype(np.uint8))
    image.save(bio, format="JPEG")
    return bio.getvalue()


###
# NGINX-based fixtures
###


@pytest.fixture(scope="function")
def mock_http_image_urls(nginx_config, image_data) -> YieldFixture[str]:
    """Uses the docker-compose Nginx server to serve HTTP image URLs

    This fixture yields:
        list[str]: URLs of files available on the HTTP server
    """
    server_url, static_assets_tmpdir = nginx_config

    # Add image files to the tmpdir
    urls = []
    for i in range(10):
        image_filepath = static_assets_tmpdir / f"{i}.jpeg"
        image_filepath.write_bytes(image_data)
        urls.append(f"{server_url}/{image_filepath.relative_to(static_assets_tmpdir)}")

    try:
        yield urls
    # Remember to cleanup!
    finally:
        for child in static_assets_tmpdir.glob("*"):
            child.unlink()


###
# S3-based fixtures
###


@pytest.fixture(scope="function")
def minio_image_data_fixture(minio_io_config, image_data) -> YieldFixture[list[str]]:
    """Populates the minio session with some fake data and yields (S3Config, paths)"""
    s3 = boto3.resource(
        "s3",
        endpoint_url=minio_io_config.s3.endpoint_url,
        aws_access_key_id=minio_io_config.s3.key_id,
        aws_secret_access_key=minio_io_config.s3.access_key,
    )

    # Add some images into `s3://image-bucket`
    BUCKET = "image-bucket"
    bucket = s3.Bucket(BUCKET)
    bucket.create()
    urls = []
    for i in range(10):
        key = f"{i}.jpeg"
        bucket.put_object(Body=image_data, Key=key)
        urls.append(f"s3://{BUCKET}/{key}")

    try:
        yield urls
    # Remember to cleanup!
    finally:
        bucket.objects.all().delete()
        bucket.delete()
