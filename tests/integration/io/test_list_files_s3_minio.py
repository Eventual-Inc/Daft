from __future__ import annotations

import pytest

from daft.daft import io_list

from .conftest import minio_create_bucket


def compare_s3_result(daft_ls_result: list, s3fs_result: list):
    daft_files = [(f["path"], f["type"].lower()) for f in daft_ls_result]
    s3fs_files = [(f"s3://{f['Key']}", f["type"]) for f in s3fs_result]
    assert sorted(daft_files) == sorted(s3fs_files)


@pytest.mark.integration()
def test_flat_directory_listing(minio_io_config):
    bucket_name = "bucket"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        files = ["a", "b", "c"]
        for name in files:
            fs.touch(f"{bucket_name}/{name}")
        daft_ls_result = io_list(f"s3://{bucket_name}", io_config=minio_io_config)
        s3fs_result = fs.ls(f"s3://{bucket_name}", detail=True)
        compare_s3_result(daft_ls_result, s3fs_result)
