from __future__ import annotations

import datetime

import pytest

import daft

from .conftest import minio_create_bucket


@pytest.mark.integration()
def test_read_blob_from_s3_minio(minio_io_config):
    """read_blob over S3 returns content + size + last_modified from listing."""
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket_name):
        url = f"s3://{bucket_name}/blob.bin"
        payload = b"\x00\x01\x02hello"
        fs.write_bytes(url, payload)

        df = daft.read_blob(url, io_config=minio_io_config)
        data = df.to_pydict()
        assert data["content"] == [payload]
        assert data["size"] == [len(payload)]
        ts = data["last_modified"][0]
        assert ts is not None
        assert isinstance(ts, datetime.datetime)
        assert ts.tzinfo is not None


@pytest.mark.integration()
def test_read_blob_s3_minio_glob_last_modified_populated(minio_io_config):
    """Globbed S3 reads carry last_modified for every matched file."""
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket_name):
        fs.write_bytes(f"s3://{bucket_name}/a.bin", b"A")
        fs.write_bytes(f"s3://{bucket_name}/b.bin", b"BB")

        df = daft.read_blob(f"s3://{bucket_name}/*.bin", io_config=minio_io_config)
        data = df.to_pydict()
        assert sorted(data["size"]) == [1, 2]
        for ts in data["last_modified"]:
            assert ts is not None


@pytest.mark.integration()
def test_read_blob_s3_minio_size_only_projection(minio_io_config):
    """Selecting only `size` should skip the byte read; `last_modified` still populated from listing."""
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket_name):
        payload = b"1234567890"
        fs.write_bytes(f"s3://{bucket_name}/f.bin", payload)

        df = daft.read_blob(f"s3://{bucket_name}/f.bin", io_config=minio_io_config).select("size", "last_modified")
        data = df.to_pydict()
        assert data["size"] == [len(payload)]
        assert data["last_modified"][0] is not None
