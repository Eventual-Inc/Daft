from __future__ import annotations

import pytest
import s3fs

import daft


def test_url_download_aws_s3_public_bucket_custom_s3fs():
    fs = s3fs.S3FileSystem(anon=True)
    data = {"urls": [f"s3://daft-public-data/test_fixtures/small_images/rickroll{i}.jpg" for i in range(6)]}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(fs=fs))

    data = df.to_pydict()
    assert len(data["data"]) == 6
    for img_bytes in data["data"]:
        assert img_bytes is not None


@pytest.mark.skip(
    reason='[ISSUE #1091] We do not yet support "anonymous-mode" (no credentials) for accessing public buckets with the native downloader'
)
def test_url_download_aws_s3_public_bucket_native_downloader(aws_public_s3_config):
    data = {"urls": [f"s3://daft-public-data/test_fixtures/small_images/rickroll{i}.jpg" for i in range(6)]}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(io_config=aws_public_s3_config, use_native_downloader=True))

    data = df.to_pydict()
    assert len(data["data"]) == 6
    for img_bytes in data["data"]:
        assert img_bytes is not None
