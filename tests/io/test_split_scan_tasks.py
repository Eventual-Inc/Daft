from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft


@pytest.fixture(scope="function")
def parquet_files(tmpdir):
    """Writes 1 Parquet file with 10 rowgroups, each of 100 bytes in size."""
    tbl = pa.table({"data": ["aaa"] * 100})
    path = tmpdir / "file.pq"
    papq.write_table(tbl, str(path), row_group_size=10, use_dictionary=False)

    return tmpdir


@pytest.mark.skip(reason="Not implemented yet")
def test_split_parquet_read_some_splits(tmpdir):
    with daft.execution_config_ctx(scantask_splitting_level=2):
        # Write a mix of 20 large and 20 small files
        # Small ones should not be split, large ones should be split into 10 rowgroups each
        # This gives us a total of 200 + 20 scantasks

        # Write 20 large files into tmpdir
        large_file_paths = []
        for i in range(20):
            tbl = pa.table({"data": [str(f"large{i}") for i in range(100)]})
            path = tmpdir / f"file.{i}.large.pq"
            papq.write_table(tbl, str(path), row_group_size=10, use_dictionary=False)
            large_file_paths.append(str(path))

        # Write 20 small files into tmpdir
        small_file_paths = []
        for i in range(20):
            tbl = pa.table({"data": ["small"]})
            path = tmpdir / f"file.{i}.small.pq"
            papq.write_table(tbl, str(path), row_group_size=1, use_dictionary=False)
            small_file_paths.append(str(path))
