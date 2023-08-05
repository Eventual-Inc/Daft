from __future__ import annotations

import pytest

PATH = (
    "s3://eventual-dev-benchmarking-fixtures/parquet-benchmarking/tpch/200MB-2RG/daft_200MB_lineitem_chunk.RG-2.parquet"
)


@pytest.mark.benchmark(group="num_files_single_column")
@pytest.mark.parametrize(
    "num_files",
    [1, 2, 4, 8],
)
def test_read_parquet_num_files_single_column(num_files, bulk_read_fn, benchmark):
    data = benchmark(bulk_read_fn, [PATH] * num_files, columns=["L_ORDERKEY"])
    assert len(data) == num_files
    # Make sure the data is correct
    for i in range(num_files):
        assert data[i].column_names == ["L_ORDERKEY"]
        assert len(data[i]) == 5515199


@pytest.mark.benchmark(group="num_rowgroups_all_columns")
@pytest.mark.parametrize(
    "num_files",
    [1, 2, 4],
)
def test_read_parquet_num_files_all_columns(num_files, bulk_read_fn, benchmark):
    data = benchmark(bulk_read_fn, [PATH] * num_files)
    assert len(data) == num_files

    # Make sure the data is correct
    for i in range(num_files):
        assert len(data[i].column_names) == 16
        assert len(data[i]) == 5515199
