from __future__ import annotations

import pytest

import daft
from daft.exceptions import ConnectTimeoutError, ReadTimeoutError


@pytest.mark.integration()
def test_url_download_aws_s3_public_bucket_custom_s3fs(small_images_s3_paths):
    data = {"urls": small_images_s3_paths}
    df = daft.from_pydict(data)
    df = df.with_column(
        "data", df["urls"].url.download(io_config=daft.io.IOConfig(s3=daft.io.S3Config(anonymous=True)))
    )

    data = df.to_pydict()
    assert len(data["data"]) == 12
    for img_bytes in data["data"]:
        assert img_bytes is not None


@pytest.mark.integration()
def test_url_download_aws_s3_public_bucket_custom_s3fs_wrong_region(small_images_s3_paths):
    data = {"urls": small_images_s3_paths}
    df = daft.from_pydict(data)
    df = df.with_column(
        "data", df["urls"].url.download(io_config=daft.io.IOConfig(s3=daft.io.S3Config(anonymous=True)))
    )

    data = df.to_pydict()
    assert len(data["data"]) == 12
    for img_bytes in data["data"]:
        assert img_bytes is not None


@pytest.mark.integration()
def test_url_download_aws_s3_public_bucket_native_downloader(aws_public_s3_config, small_images_s3_paths):
    data = {"urls": small_images_s3_paths}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(io_config=aws_public_s3_config))

    data = df.to_pydict()
    assert len(data["data"]) == 12
    for img_bytes in data["data"]:
        assert img_bytes is not None


@pytest.mark.integration()
def test_url_download_aws_s3_public_bucket_native_downloader_with_connect_timeout(small_images_s3_paths):
    data = {"urls": small_images_s3_paths}
    df = daft.from_pydict(data)

    connect_timeout_config = daft.io.IOConfig(
        s3=daft.io.S3Config(
            # NOTE: no keys or endpoints specified for an AWS public s3 bucket
            region_name="us-west-2",
            anonymous=True,
            connect_timeout_ms=1,
        )
    )

    with pytest.raises((ReadTimeoutError, ConnectTimeoutError), match="timed out when trying to connect to"):
        df = df.with_column("data", df["urls"].url.download(io_config=connect_timeout_config)).collect()


@pytest.mark.integration()
def test_url_download_aws_s3_public_bucket_native_downloader_with_read_timeout(small_images_s3_paths):
    data = {"urls": small_images_s3_paths}
    df = daft.from_pydict(data)

    read_timeout_config = daft.io.IOConfig(
        s3=daft.io.S3Config(
            # NOTE: no keys or endpoints specified for an AWS public s3 bucket
            region_name="us-west-2",
            anonymous=True,
            read_timeout_ms=1,
        )
    )

    with pytest.raises((ReadTimeoutError, ConnectTimeoutError), match="timed out when trying to connect to"):
        df = df.with_column("data", df["urls"].url.download(io_config=read_timeout_config)).collect()
