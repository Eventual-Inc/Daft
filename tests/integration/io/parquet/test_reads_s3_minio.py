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
            f"s3://data-engineering-prod/Y/part-00000-51723f93-0ba2-42f1-a58f-154f0ed40f28.c000.snappy.parquet",
            f"s3://data-engineering-prod/Z/part-00000-6d5c7cc6-3b4a-443e-a46a-ca9e080bda1b.c000.snappy.parquet",
        ]
        data = {"x": [1, 2, 3, 4]}
        pa_table = pa.Table.from_pydict(data)
        for path in target_paths:
            pq.write_table(pa_table, path, filesystem=fs)

        readback = daft.table.read_parquet_into_pyarrow_bulk(target_paths, io_config=minio_io_config)
        assert len(readback) == len(target_paths)
        for tab in readback:
            assert tab.to_pydict() == data
