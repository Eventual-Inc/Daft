from __future__ import annotations

import pytest


@pytest.mark.benchmark(group="num_rowgroups")
@pytest.mark.parametrize(
    "path",
    [
        "s3://daft-public-data/test_fixtures/parquet-dev/daft_tpch_100g_32part_1RG.parquet",
        "s3://daft-public-data/test_fixtures/parquet-dev/daft_tpch_100g_32part.parquet",
        # Disabled: too slow!
        # "s3://daft-public-data/test_fixtures/parquet-dev/daft_tpch_100g_32part_18kRG.parquet"
        # "s3://daft-public-data/test_fixtures/parquet-dev/daft_tpch_100g_32part_180kRG.parquet",
    ],
    ids=[
        "1",
        "2k",
        # Disabled: too slow!
        # "18k",
        # "180k",
    ],
)
def test_read_parquet_num_rowgroups(path, read_fn, benchmark):
    data = benchmark(read_fn, path, columns=["L_ORDERKEY"])

    # Make sure the data is correct
    assert data.column_names == ["L_ORDERKEY"]
    assert len(data) == 18751674

    # TODO(jay): Figure out how to track peak memory usage. Not sure yet how to aggregate this across calls.
    benchmark.extra_info["peak_memory_usage"] = None
