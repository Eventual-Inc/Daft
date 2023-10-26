from __future__ import annotations

import pytest

import daft


@pytest.mark.integration()
def test_url_download_minio_native_downloader(minio_io_config, minio_image_data_fixture, image_data):
    data = {"urls": minio_image_data_fixture}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(io_config=minio_io_config))
    assert df.to_pydict() == {**data, "data": [image_data for _ in range(len(minio_image_data_fixture))]}
