from __future__ import annotations

import pytest

import daft

from .conftest import minio_create_bucket


@pytest.mark.integration()
def test_read_text_from_s3_minio(minio_io_config):
    """Test that read_text works over S3 (exercises the GetResult::Stream path)."""
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket_name):
        url = f"s3://{bucket_name}/test.txt"
        fs.write_bytes(url, b"hello\nworld\n")

        df = daft.read_text(url, io_config=minio_io_config)
        assert df.to_pydict()["content"] == ["hello", "world"]


@pytest.mark.integration()
def test_read_text_from_s3_minio_skip_blank_lines(minio_io_config):
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket_name):
        url = f"s3://{bucket_name}/blanks.txt"
        fs.write_bytes(url, b"a\n\nb\n   \nc\n")

        df = daft.read_text(url, skip_blank_lines=True, io_config=minio_io_config)
        assert df.to_pydict()["content"] == ["a", "b", "c"]

        df = daft.read_text(url, skip_blank_lines=False, io_config=minio_io_config)
        assert df.to_pydict()["content"] == ["a", "", "b", "   ", "c"]


@pytest.mark.integration()
def test_read_text_from_s3_minio_glob(minio_io_config):
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket_name):
        fs.write_bytes(f"s3://{bucket_name}/a.txt", b"a1\na2\n")
        fs.write_bytes(f"s3://{bucket_name}/b.txt", b"b1\n")

        df = daft.read_text(f"s3://{bucket_name}/*.txt", io_config=minio_io_config)
        result = sorted(df.to_pydict()["content"])
        assert result == ["a1", "a2", "b1"]


@pytest.mark.integration()
def test_read_text_from_s3_minio_with_limit(minio_io_config):
    with minio_create_bucket(minio_io_config=minio_io_config) as (fs, bucket_name):
        content = "\n".join([f"line{i}" for i in range(100)]) + "\n"
        fs.write_bytes(f"s3://{bucket_name}/many.txt", content.encode())

        df = daft.read_text(f"s3://{bucket_name}/many.txt", io_config=minio_io_config).limit(10)
        assert len(df.collect()) == 10
