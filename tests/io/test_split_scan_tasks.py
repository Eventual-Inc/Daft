from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft


@pytest.fixture(scope="function")
def parquet_files(tmpdir):
    """Writes 1 Parquet file with 10 rowgroups, each of 100 bytes in size"""
    tbl = pa.table({"data": ["aaa"] * 100})
    path = tmpdir / "file.pq"
    papq.write_table(tbl, str(path), row_group_size=10, use_dictionary=False)

    return tmpdir


def test_split_parquet_read(parquet_files):
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=10,
    ):
        df = daft.read_parquet(str(parquet_files))
        assert df.num_partitions() == 10, "Should have 10 partitions since we will split the file"
        assert df.to_pydict() == {"data": ["aaa"] * 100}
