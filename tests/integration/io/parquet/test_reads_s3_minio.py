from __future__ import annotations

import pyarrow as pa
import pytest
from pyarrow import parquet as pq

import daft

from ..conftest import minio_create_bucket


@pytest.mark.integration()
def test_minio_parquet_bulk_readback(minio_io_config):
    bucket_name = "data-engineering-prod"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        target_paths = [
            "s3://data-engineering-prod/Y/part-00000-51723f93-0ba2-42f1-a58f-154f0ed40f28.c000.snappy.parquet",
            "s3://data-engineering-prod/Z/part-00000-6d5c7cc6-3b4a-443e-a46a-ca9e080bda1b.c000.snappy.parquet",
        ]
        data = {"x": [1, 2, 3, 4]}
        pa_table = pa.Table.from_pydict(data)
        for path in target_paths:
            pq.write_table(pa_table, path, filesystem=fs)

        readback = daft.table.read_parquet_into_pyarrow_bulk(target_paths, io_config=minio_io_config)
        assert len(readback) == len(target_paths)
        for tab in readback:
            assert tab.to_pydict() == data


@pytest.mark.integration()
def test_minio_parquet_read_no_files(minio_io_config):
    bucket_name = "data-engineering-prod"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        fs.touch("s3://data-engineering-prod/foo/file.txt")

        with pytest.raises(FileNotFoundError, match="Glob path had no matches:"):
            daft.read_parquet("s3://data-engineering-prod/foo/**.parquet", io_config=minio_io_config)


@pytest.mark.integration()
def test_minio_parquet_ignore_marker_files(minio_io_config):
    bucket_name = "data-engineering-prod"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        target_paths = [
            "s3://data-engineering-prod/X/no_ext_parquet_metadata",
            "s3://data-engineering-prod/Y/part-00000-51723f93-0ba2-42f1-a58f-154f0ed40f28.c000.snappy.parquet",
            "s3://data-engineering-prod/Z/part-00000-6d5c7cc6-3b4a-443e-a46a-ca9e080bda1b.c000.snappy.parquet",
        ]
        data = {"x": [1, 2, 3, 4]}
        pa_table = pa.Table.from_pydict(data)
        for path in target_paths:
            pq.write_table(pa_table, path, filesystem=fs)

        marker_files = ["_metadata", "_SUCCESS", "_common_metadata", "a.crc"]
        for marker in marker_files:
            fs.touch(f"s3://{bucket_name}/X/{marker}")
            fs.touch(f"s3://{bucket_name}/Y/{marker}")
            fs.touch(f"s3://{bucket_name}/Z/{marker}")

        read = daft.read_parquet(f"s3://{bucket_name}/**", io_config=minio_io_config)
        assert read.to_pydict() == {"x": [1, 2, 3, 4] * 3}


@pytest.mark.integration()
def test_minio_parquet_read_mismatched_schemas_no_pushdown(minio_io_config):
    # When we read files, we infer schema from the first file
    # Then when we read subsequent files, we want to be able to read the data still but add nulls for columns
    # that don't exist
    bucket_name = "data-engineering-prod"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        data_0 = pa.Table.from_pydict({"x": [1, 2, 3, 4]})
        pq.write_table(data_0, f"s3://{bucket_name}/data_0.parquet", filesystem=fs)
        data_1 = pa.Table.from_pydict({"y": [1, 2, 3, 4]})
        pq.write_table(data_1, f"s3://{bucket_name}/data_1.parquet", filesystem=fs)

        df = daft.read_parquet(
            [f"s3://{bucket_name}/data_0.parquet", f"s3://{bucket_name}/data_1.parquet"], io_config=minio_io_config
        )
        assert df.schema().column_names() == ["x"]
        assert df.to_pydict() == {"x": [1, 2, 3, 4, None, None, None, None]}


@pytest.mark.integration()
def test_minio_parquet_read_mismatched_schemas_with_pushdown(minio_io_config):
    # When we read files, we infer schema from the first file
    # Then when we read subsequent files, we want to be able to read the data still but add nulls for columns
    # that don't exist
    bucket_name = "data-engineering-prod"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        data_0 = pa.Table.from_pydict(
            {
                "x": [1, 2, 3, 4],
                "y": [1, 2, 3, 4],
                # NOTE: Need a column z here because Daft doesn't do a pushdown otherwise.
                "z": [1, 1, 1, 1],
            }
        )
        pq.write_table(data_0, f"s3://{bucket_name}/data_0.parquet", filesystem=fs)
        data_1 = pa.Table.from_pydict({"x": [5, 6, 7, 8]})
        pq.write_table(data_1, f"s3://{bucket_name}/data_1.parquet", filesystem=fs)

        df = daft.read_parquet(
            [f"s3://{bucket_name}/data_0.parquet", f"s3://{bucket_name}/data_1.parquet"], io_config=minio_io_config
        )
        df = df.select("x", "y")  # Applies column selection pushdown on each read
        assert df.schema().column_names() == ["x", "y"]
        assert df.to_pydict() == {"x": [1, 2, 3, 4, 5, 6, 7, 8], "y": [1, 2, 3, 4, None, None, None, None]}


@pytest.mark.integration()
def test_minio_parquet_read_mismatched_schemas_with_pushdown_no_rows_read(minio_io_config):
    # When we read files, we infer schema from the first file
    # Then when we read subsequent files, we want to be able to read the data still but add nulls for columns
    # that don't exist
    bucket_name = "data-engineering-prod"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        data_0 = pa.Table.from_pydict(
            {
                "x": [1, 2, 3, 4],
                # NOTE: Need a column z here because Daft doesn't do a pushdown otherwise.
                "z": [1, 1, 1, 1],
            }
        )
        pq.write_table(data_0, f"s3://{bucket_name}/data_0.parquet", filesystem=fs)
        data_1 = pa.Table.from_pydict({"y": [1, 2, 3, 4]})
        pq.write_table(data_1, f"s3://{bucket_name}/data_1.parquet", filesystem=fs)

        df = daft.read_parquet(
            [f"s3://{bucket_name}/data_0.parquet", f"s3://{bucket_name}/data_1.parquet"], io_config=minio_io_config
        )
        df = df.select("x")  # Applies column selection pushdown on each read
        assert df.schema().column_names() == ["x"]
        assert df.to_pydict() == {"x": [1, 2, 3, 4, None, None, None, None]}
