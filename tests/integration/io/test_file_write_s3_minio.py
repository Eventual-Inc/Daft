from __future__ import annotations

import boto3
import pytest

import daft

from .conftest import minio_create_bucket

PART_SIZE = 8 * 1024 * 1024  # Default S3Config.multipart_size.


def _list_incomplete_uploads(minio_io_config, bucket_name: str) -> list[str]:
    client = boto3.client(
        "s3",
        aws_access_key_id=minio_io_config.s3.key_id,
        aws_secret_access_key=minio_io_config.s3.access_key,
        endpoint_url=minio_io_config.s3.endpoint_url,
    )
    response = client.list_multipart_uploads(Bucket=bucket_name)
    return [upload["Key"] for upload in response.get("Uploads", [])]


@pytest.mark.integration()
def test_file_write_multipart_commit_on_close(minio_io_config):
    with minio_create_bucket(minio_io_config=minio_io_config) as (_, bucket_name):
        url = f"s3://{bucket_name}/out/data.bin"
        # Span multiple parts so the streaming path uploads at least one full part
        # before the final (smaller) one.
        payload = b"x" * (PART_SIZE + 1024)

        f = daft.File(url, io_config=minio_io_config)
        with f.open("wb") as writer:
            writer.write(payload)

        with daft.File(url, io_config=minio_io_config).open() as reader:
            assert reader.read() == payload
        assert _list_incomplete_uploads(minio_io_config, bucket_name) == []


@pytest.mark.integration()
def test_file_write_multipart_nothing_visible_before_close(minio_io_config):
    with minio_create_bucket(minio_io_config=minio_io_config) as (_, bucket_name):
        url = f"s3://{bucket_name}/out/data.bin"
        payload = b"x" * (PART_SIZE + 1024)

        f = daft.File(url, io_config=minio_io_config)
        with f.open("wb") as writer:
            writer.write(payload)
            writer.flush()
            assert not daft.File(url, io_config=minio_io_config).exists()

        assert daft.File(url, io_config=minio_io_config).exists()


@pytest.mark.integration()
def test_file_write_empty_commit_creates_empty_object(minio_io_config):
    # Committing without streaming any part cannot complete the multipart upload
    # (S3 rejects CompleteMultipartUpload with zero parts), so the writer aborts
    # the empty upload and commits with a single put instead.
    with minio_create_bucket(minio_io_config=minio_io_config) as (_, bucket_name):
        url = f"s3://{bucket_name}/out/empty.bin"

        f = daft.File(url, io_config=minio_io_config)
        with f.open("wb"):
            pass

        with daft.File(url, io_config=minio_io_config).open() as reader:
            assert reader.read() == b""
        assert _list_incomplete_uploads(minio_io_config, bucket_name) == []


@pytest.mark.integration()
def test_file_write_small_commit_below_part_size(minio_io_config):
    # Sub-part-size writes never stream a part either; they take the same
    # abort-then-put path as the empty case.
    with minio_create_bucket(minio_io_config=minio_io_config) as (_, bucket_name):
        url = f"s3://{bucket_name}/out/small.bin"

        f = daft.File(url, io_config=minio_io_config)
        with f.open("wb") as writer:
            writer.write(b"small payload")

        with daft.File(url, io_config=minio_io_config).open() as reader:
            assert reader.read() == b"small payload"
        assert _list_incomplete_uploads(minio_io_config, bucket_name) == []


@pytest.mark.integration()
def test_file_write_multipart_exception_aborts_upload(minio_io_config):
    with minio_create_bucket(minio_io_config=minio_io_config) as (_, bucket_name):
        url = f"s3://{bucket_name}/out/aborted.bin"
        # A full part is streamed to the store before the exception is raised.
        payload = b"x" * (PART_SIZE + 1024)

        f = daft.File(url, io_config=minio_io_config)
        with pytest.raises(RuntimeError, match="boom"), f.open("wb") as writer:
            writer.write(payload)
            raise RuntimeError("boom")

        assert not daft.File(url, io_config=minio_io_config).exists()
        # The streamed part must not linger as an incomplete multipart upload,
        # which S3 would otherwise keep (and bill for) indefinitely.
        assert _list_incomplete_uploads(minio_io_config, bucket_name) == []


@pytest.mark.integration()
def test_file_write_text_minio(minio_io_config):
    with minio_create_bucket(minio_io_config=minio_io_config) as (_, bucket_name):
        url = f"s3://{bucket_name}/out/text.txt"

        f = daft.File(url, io_config=minio_io_config)
        with f.open("w") as writer:
            writer.write("héllo minio")

        with daft.File(url, io_config=minio_io_config).open() as reader:
            assert reader.read().decode("utf-8") == "héllo minio"
