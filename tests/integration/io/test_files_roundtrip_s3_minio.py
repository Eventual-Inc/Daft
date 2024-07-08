from __future__ import annotations

import pytest

import daft

from .conftest import minio_create_bucket


@pytest.mark.integration()
def test_files_roundtrip_minio_native_downloader(minio_io_config):
    bucket_name = "my-bucket"
    folder = f"s3://{bucket_name}/my-folder"
    with minio_create_bucket(minio_io_config=minio_io_config, bucket_name=bucket_name):
        bytes_data = [b"a", b"b", b"c"]
        data = {"data": bytes_data}
        df = daft.from_pydict(data)
        df = df.with_column("file_paths", df["data"].url.upload(folder, io_config=minio_io_config))
        df.collect()

        df = df.with_column("roundtrip_data", df["file_paths"].url.download(io_config=minio_io_config))
        results = df.to_pydict()

        assert results["data"] == results["roundtrip_data"] == bytes_data
        for path, expected in zip(results["file_paths"], bytes_data):
            assert path.startswith(folder)
