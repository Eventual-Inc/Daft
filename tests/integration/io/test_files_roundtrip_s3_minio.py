from __future__ import annotations

import pytest

import daft

from .conftest import minio_create_bucket, minio_create_public_bucket


def run_url_upload_roundtrip_test(folder: str, io_config, bytes_data: list[bytes]):
    """Helper function to run URL upload/download roundtrip test."""
    data = {"data": bytes_data}
    df = daft.from_pydict(data)
    df = df.with_column("file_paths", df["data"].url.upload(folder, io_config=io_config))
    df.collect()

    df = df.with_column("roundtrip_data", df["file_paths"].url.download(io_config=io_config))
    results = df.to_pydict()

    assert results["data"] == results["roundtrip_data"] == bytes_data
    for path in results["file_paths"]:
        assert path.startswith(folder)


@pytest.mark.integration()
def test_files_roundtrip_minio_native_downloader(minio_io_config):
    bucket_name = "my-bucket"
    folder = f"s3://{bucket_name}/my-folder"
    with minio_create_bucket(minio_io_config=minio_io_config, bucket_name=bucket_name):
        bytes_data = [b"a", b"b", b"c"]
        run_url_upload_roundtrip_test(folder, minio_io_config, bytes_data)


@pytest.mark.integration()
def test_files_roundtrip_minio_anonymous_upload(anonymous_minio_io_config, minio_io_config):
    """Test anonymous URL upload and download roundtrip."""
    bucket_name = "my-public-bucket"
    folder = f"s3://{bucket_name}/my-folder"
    # Use the authenticated config to create the public bucket, but use the anonymous config to upload/download.
    with minio_create_public_bucket(minio_io_config=minio_io_config, bucket_name=bucket_name):
        bytes_data = [b"a", b"b", b"c"]
        run_url_upload_roundtrip_test(folder, anonymous_minio_io_config, bytes_data)
