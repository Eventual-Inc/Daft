from __future__ import annotations

import pytest

import daft
from tests.integration.io.conftest import minio_create_bucket


@pytest.mark.integration()
def test_read_parquet_from_s3_with_include_file_path_column(minio_io_config):
    bucket_name = "bucket"
    data = {"a": [1, 2, 3], "b": ["a", "b", "c"]}
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name):
        file_paths = (
            daft.from_pydict(data).write_parquet(f"s3://{bucket_name}", io_config=minio_io_config).to_pydict()["path"]
        )
        assert len(file_paths) == 1
        file_path = f"s3://{file_paths[0]}"
        read_back = daft.read_parquet(file_path, io_config=minio_io_config, file_path_column="path").to_pydict()
        assert read_back["a"] == data["a"]
        assert read_back["b"] == data["b"]
        assert read_back["path"] == [file_path] * 3


@pytest.mark.integration()
def test_read_multi_parquet_from_s3_with_include_file_path_column(minio_io_config):
    bucket_name = "bucket"
    data = {"a": [1, 2, 3], "b": ["a", "b", "c"]}
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name):
        file_paths = (
            daft.from_pydict(data)
            .write_parquet(f"s3://{bucket_name}", partition_cols=["a"], io_config=minio_io_config)
            .to_pydict()["path"]
        )
        assert len(file_paths) == 3
        file_paths = sorted([f"s3://{path}" for path in file_paths])
        read_back = daft.read_parquet(file_paths, io_config=minio_io_config, file_path_column="path").to_pydict()
        assert read_back["a"] == data["a"]
        assert read_back["b"] == data["b"]
        assert read_back["path"] == file_paths


@pytest.mark.integration()
def test_read_csv_from_s3_with_include_file_path_column(minio_io_config):
    bucket_name = "bucket"
    data = {"a": [1, 2, 3], "b": ["a", "b", "c"]}
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name):
        file_paths = (
            daft.from_pydict(data).write_csv(f"s3://{bucket_name}", io_config=minio_io_config).to_pydict()["path"]
        )
        assert len(file_paths) == 1
        file_path = f"s3://{file_paths[0]}"
        read_back = daft.read_csv(file_path, io_config=minio_io_config, file_path_column="path").to_pydict()
        assert read_back["a"] == data["a"]
        assert read_back["b"] == data["b"]
        assert read_back["path"] == [file_path] * 3


@pytest.mark.integration()
def test_read_multi_csv_from_s3_with_include_file_path_column(minio_io_config):
    bucket_name = "bucket"
    data = {"a": [1, 2, 3], "b": ["a", "b", "c"]}
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name):
        file_paths = (
            daft.from_pydict(data)
            .write_csv(f"s3://{bucket_name}", io_config=minio_io_config, partition_cols=["a"])
            .to_pydict()["path"]
        )
        assert len(file_paths) == 3
        file_paths = sorted([f"s3://{path}" for path in file_paths])
        read_back = daft.read_csv(file_paths, io_config=minio_io_config, file_path_column="path").to_pydict()
        assert read_back["a"] == data["a"]
        assert read_back["b"] == data["b"]
        assert read_back["path"] == file_paths
