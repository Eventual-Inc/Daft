from __future__ import annotations

import pytest

from daft.daft import io_list

from .conftest import minio_create_bucket


def compare_s3_result(daft_ls_result: list, s3fs_result: list):
    daft_files = [(f["path"], f["type"].lower()) for f in daft_ls_result]
    s3fs_files = [(f"s3://{f['name']}", f["type"]) for f in s3fs_result]
    assert sorted(daft_files) == sorted(s3fs_files)


def s3fs_recursive_list(fs, path) -> list:
    all_results = []
    curr_level_result = fs.ls(path, detail=True)
    for item in curr_level_result:
        if item["type"] == "directory":
            new_path = f's3://{item["name"]}'
            all_results.extend(s3fs_recursive_list(fs, new_path))
            item["name"] += "/"
            all_results.append(item)
        else:
            all_results.append(item)
    return all_results


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


@pytest.mark.integration()
def test_recursive_directory_listing(minio_io_config):
    bucket_name = "bucket"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        files = ["a", "b/bb", "c/cc/ccc"]
        for name in files:
            fs.write_bytes(f"s3://{bucket_name}/{name}", b"")
        daft_ls_result = io_list(f"s3://{bucket_name}/", io_config=minio_io_config, recursive=True)
        fs.invalidate_cache()
        s3fs_result = s3fs_recursive_list(fs, path=f"s3://{bucket_name}")
        compare_s3_result(daft_ls_result, s3fs_result)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "recursive",
    [False, True],
)
def test_single_file_directory_listing(minio_io_config, recursive):
    bucket_name = "bucket"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        files = ["a", "b/bb", "c/cc/ccc"]
        for name in files:
            fs.write_bytes(f"s3://{bucket_name}/{name}", b"")
        daft_ls_result = io_list(f"s3://{bucket_name}/c/cc/ccc", io_config=minio_io_config, recursive=recursive)
        fs.invalidate_cache()
        s3fs_result = s3fs_recursive_list(fs, path=f"s3://{bucket_name}/c/cc/ccc")
        assert len(daft_ls_result) == 1
        compare_s3_result(daft_ls_result, s3fs_result)
