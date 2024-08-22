from __future__ import annotations

import contextlib

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft


@contextlib.contextmanager
def override_merge_scan_tasks_configs(scan_tasks_min_size_bytes: int, scan_tasks_max_size_bytes: int):
    old_execution_config = daft.context.get_context().daft_execution_config

    try:
        daft.set_execution_config(
            scan_tasks_min_size_bytes=scan_tasks_min_size_bytes,
            scan_tasks_max_size_bytes=scan_tasks_max_size_bytes,
        )
        yield
    finally:
        daft.set_execution_config(old_execution_config)


@pytest.fixture(scope="function")
def parquet_files(tmpdir):
    """Writes 1 Parquet file with 10 rowgroups, each of 100 bytes in size"""
    tbl = pa.table({"data": ["aaa"] * 100})
    path = tmpdir / "file.pq"
    papq.write_table(tbl, str(path), row_group_size=10, use_dictionary=False)

    return tmpdir


def test_split_parquet_read(parquet_files):
    with override_merge_scan_tasks_configs(1, 10):
        df = daft.read_parquet(str(parquet_files))
        assert df.num_partitions() == 10, "Should have 10 partitions since we will split the file"
        assert df.to_pydict() == {"data": ["aaa"] * 100}
