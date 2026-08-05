from __future__ import annotations

import contextlib
import uuid
from collections.abc import Generator
from typing import TypeVar

import pytest

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
            use_ssl=False,
        )
    )


@contextlib.contextmanager
def minio_create_bucket(minio_io_config: daft.io.IOConfig, bucket_name: str | None = None) -> YieldFixture[str]:
    """Create a temporary MinIO bucket, yield its name, and clean up."""
    import s3fs

    if bucket_name is None:
        bucket_name = f"ckpt-{uuid.uuid4()}"

    fs = s3fs.S3FileSystem(
        key=minio_io_config.s3.key_id,
        password=minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": minio_io_config.s3.endpoint_url},
    )
    if fs.exists(bucket_name):
        fs.rm(bucket_name, recursive=True)
    fs.mkdir(bucket_name)
    try:
        yield bucket_name
    finally:
        fs.rm(bucket_name, recursive=True)
