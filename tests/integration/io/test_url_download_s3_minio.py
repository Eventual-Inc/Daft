from __future__ import annotations

import pytest
import s3fs

import daft


@pytest.mark.integration()
def test_url_download_minio_custom_s3fs(minio_io_config, minio_image_data_fixture, image_data):
    urls = minio_image_data_fixture
    fs = s3fs.S3FileSystem(
        key=minio_io_config.s3.key_id,
        password=minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": minio_io_config.s3.endpoint_url},
    )
    data = {"urls": urls}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(fs=fs))

    assert df.to_pydict() == {**data, "data": [image_data for _ in range(len(urls))]}


@pytest.mark.integration()
def test_url_download_minio_native_downloader(minio_io_config, minio_image_data_fixture, image_data):
    data = {"urls": minio_image_data_fixture}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(io_config=minio_io_config, use_native_downloader=True))
    assert df.to_pydict() == {**data, "data": [image_data for _ in range(len(minio_image_data_fixture))]}
