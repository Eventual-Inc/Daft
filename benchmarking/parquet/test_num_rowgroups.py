from __future__ import annotations

import pytest

PATHS = [
    "s3://daft-public-data/test_fixtures/parquet-dev/daft_tpch_100g_32part_1RG.parquet",
    "s3://daft-public-data/test_fixtures/parquet-dev/daft_tpch_100g_32part.parquet",
    # Disabled: too slow!
    # "s3://daft-public-data/test_fixtures/parquet-dev/daft_tpch_100g_32part_18kRG.parquet"
    # "s3://daft-public-data/test_fixtures/parquet-dev/daft_tpch_100g_32part_180kRG.parquet",
]

IDS = [
    "1",
    "2k",
    # Disabled: too slow!
    # "18k",
    # "180k",
]


@pytest.mark.benchmark(group="num_rowgroups_single_column")
@pytest.mark.parametrize(
    "path",
    PATHS,
    ids=IDS,
)
def test_read_parquet_num_rowgroups(path, read_fn, benchmark):
    data = benchmark(read_fn, path, columns=["L_ORDERKEY"])

    # Make sure the data is correct
    assert data.column_names == ["L_ORDERKEY"]
    assert len(data) == 18751674


@pytest.mark.benchmark(group="num_rowgroups_multi_contiguous_columns")
@pytest.mark.parametrize(
    "path",
    PATHS,
    ids=IDS,
)
def test_read_parquet_num_rowgroups(path, read_fn, benchmark):
    data = benchmark(read_fn, path, columns=["L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY"])

    # Make sure the data is correct
    assert data.column_names == ["L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY"]
    assert len(data) == 18751674


@pytest.mark.benchmark(group="num_rowgroups_multi_sparse_columns")
@pytest.mark.parametrize(
    "path",
    PATHS,
    ids=IDS,
)
def test_read_parquet_num_rowgroups(path, read_fn, benchmark):
    data = benchmark(read_fn, path, columns=["L_ORDERKEY", "L_TAX"])

    # Make sure the data is correct
    assert data.column_names == ["L_ORDERKEY", "L_TAX"]
    assert len(data) == 18751674


@pytest.mark.benchmark(group="num_rowgroups_all_columns")
@pytest.mark.parametrize(
    "path",
    PATHS,
    ids=IDS,
)
def test_read_parquet_num_rowgroups(path, read_fn, benchmark):
    data = benchmark(read_fn, path)

    # Make sure the data is correct
    assert len(data.column_names) == 16
    assert len(data) == 18751674
