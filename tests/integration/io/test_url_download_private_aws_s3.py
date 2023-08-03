from __future__ import annotations

import boto3
import pytest

import daft
from daft.io import IOConfig, S3Config


@pytest.fixture(scope="session")
def io_config() -> IOConfig:
    """Create IOConfig with boto's current session"""
    sess = boto3.session.Session()
    creds = sess.get_credentials()

    return IOConfig(
        s3=S3Config(
            key_id=creds.access_key, access_key=creds.secret_key, session_token=creds.token, region_name="us-west-2"
        )
    )


@pytest.mark.integration()
def test_url_download_aws_s3_public_bucket_with_creds(small_images_s3_paths, io_config):
    data = {"urls": small_images_s3_paths}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(use_native_downloader=True, io_config=io_config))

    data = df.to_pydict()
    assert len(data["data"]) == 6
    for img_bytes in data["data"]:
        assert img_bytes is not None


@pytest.mark.integration()
def test_read_parquet_aws_s3_private_bucket_with_creds(io_config):
    filename = "s3://eventual-data-test-bucket/benchmarking/1M-writethrough.parquet/part-00000-ca6132d9-d056-45ad-8d67-e19cf896bc8a-c000.parquet"
    df = daft.read_parquet(filename, io_config=io_config, use_native_downloader=True).collect()
    assert len(df) == 2603
