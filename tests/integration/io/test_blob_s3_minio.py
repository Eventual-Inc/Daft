from __future__ import annotations

import pytest

import daft

from .conftest import minio_create_bucket


@pytest.mark.integration()
def test_read_blob_from_s3_minio(minio_io_config):
    """Test that read_blob reads a single file over S3 with path, size and content columns."""
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket_name):
        url = f"s3://{bucket_name}/file.bin"
        data = b"\x00\x01\x02hello blob"
        fs.write_bytes(url, data)

        df = daft.read_blob(url, io_config=minio_io_config)
        result = df.to_pydict()
        assert len(result["path"]) == 1
        assert result["path"][0].endswith("file.bin")
        assert result["size"] == [len(data)]
        assert result["content"] == [data]


@pytest.mark.integration()
def test_read_blob_from_s3_minio_glob(minio_io_config):
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket_name):
        contents = {}
        for i in range(3):
            data = f"content-{i}".encode()
            fs.write_bytes(f"s3://{bucket_name}/{i}.bin", data)
            contents[f"{i}.bin"] = data

        df = daft.read_blob(f"s3://{bucket_name}/*.bin", io_config=minio_io_config).sort("path")
        result = df.to_pydict()
        assert len(result["path"]) == 3
        for path, size, content in zip(result["path"], result["size"], result["content"]):
            name = path.rsplit("/", 1)[-1]
            assert content == contents[name]
            assert size == len(contents[name])
