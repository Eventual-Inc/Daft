from __future__ import annotations

import contextlib
import io
import os
import pathlib
import shutil
from typing import Generator, TypeVar

import numpy as np
import pytest
import s3fs
from PIL import Image

import daft

T = TypeVar("T")

YieldFixture = Generator[T, None, None]


###
# Config fixtures
###


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
def azure_storage_public_config() -> daft.io.IOConfig:
    return daft.io.IOConfig(
        azure=daft.io.AzureConfig(
            storage_account="dafttestdata",
            anonymous=True,
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
def retry_server_s3_config() -> daft.io.IOConfig:
    """Returns the URL to the local retry_server fixture"""
    return daft.io.IOConfig(
        s3=daft.io.S3Config(endpoint_url="http://127.0.0.1:8001", anonymous=True, num_tries=10),
    )


###
# Mounting utilities: mount data and perform cleanup at the end of each test
###


@contextlib.contextmanager
def mount_data_minio(
    minio_io_config: daft.io.IOConfig, folder: pathlib.Path, bucket_name: str = "my-minio-bucket"
) -> YieldFixture[list[str]]:
    """Mounts data in `folder` into files in minio

    Yields a list of S3 URLs
    """
    fs = s3fs.S3FileSystem(
        key=minio_io_config.s3.key_id,
        password=minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": minio_io_config.s3.endpoint_url},
    )
    fs.mkdir(bucket_name)

    urls = []
    for p in folder.glob("**/*"):
        if not p.is_file():
            continue
        key = str(p.relative_to(folder))
        url = f"s3://{bucket_name}/{key}"
        fs.write_bytes(url, p.read_bytes())
        urls.append(url)

    try:
        yield urls
    finally:
        fs.rm(bucket_name, recursive=True)


@contextlib.contextmanager
def mount_data_nginx(nginx_config: tuple[str, pathlib.Path], folder: pathlib.Path) -> YieldFixture[list[str]]:
    """Mounts data in `folder` into servable static files in NGINX

    Yields a list of HTTP URLs
    """
    server_url, static_assets_tmpdir = nginx_config

    # Copy data
    for root, dirs, files in os.walk(folder, topdown=False):
        for file in files:
            shutil.copy2(os.path.join(root, file), os.path.join(static_assets_tmpdir, file))
        for dir in dirs:
            shutil.copytree(os.path.join(root, dir), os.path.join(static_assets_tmpdir, dir))

    yield [f"{server_url}/{p.relative_to(folder)}" for p in folder.glob("**/*") if p.is_file()]

    # Delete data
    for root, dirs, files in os.walk(static_assets_tmpdir, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            os.rmdir(os.path.join(root, dir))


###
# Image data test fixtures
###


@pytest.fixture(scope="session")
def image_data() -> YieldFixture[bytes]:
    """Bytes of a small image"""
    bio = io.BytesIO()
    image = Image.fromarray(np.ones((3, 3)).astype(np.uint8))
    image.save(bio, format="JPEG")
    return bio.getvalue()


@pytest.fixture(scope="function")
def image_data_folder(image_data, tmpdir) -> YieldFixture[str]:
    """Dumps 10 small JPEG files into a tmpdir"""
    tmpdir = pathlib.Path(tmpdir)

    for i in range(10):
        fp = tmpdir / f"{i}.jpeg"
        fp.write_bytes(image_data)

    yield tmpdir


@pytest.fixture(scope="function")
def mock_http_image_urls(
    nginx_config: tuple[str, pathlib.Path], image_data_folder: pathlib.Path
) -> YieldFixture[list[str]]:
    """Uses the docker-compose Nginx server to serve HTTP image URLs

    This fixture yields:
        list[str]: URLs of files available on the HTTP server
    """
    with mount_data_nginx(nginx_config, image_data_folder) as urls:
        yield urls


@pytest.fixture(scope="function")
def minio_image_data_fixture(minio_io_config, image_data_folder) -> YieldFixture[list[str]]:
    """Populates the minio session with some fake data and yields (S3Config, paths)"""
    with mount_data_minio(minio_io_config, image_data_folder) as urls:
        yield urls


@pytest.fixture(scope="session")
def small_images_s3_paths() -> list[str]:
    """Paths to small *.jpg files in a public S3 bucket"""
    return [f"s3://daft-public-data/test_fixtures/small_images/rickroll{i}.jpg" for i in range(6)]
