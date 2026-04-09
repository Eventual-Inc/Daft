from __future__ import annotations

import sys

import pytest

import daft
from daft import DataType, col
from daft.io import IOConfig, S3Config

if sys.platform == "win32":
    pytest.skip(allow_module_level=True)

from tests.conftest import get_tests_daft_runner_name


def _load_queries() -> list[str]:
    with open("benchmarking/clickbench/queries.sql") as f:
        lines = [ln.strip() for ln in f.readlines() if ln.strip()]
    return lines


CLICKBENCH_QUERIES = _load_queries()
DEFAULT_HITS_URL = "https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits_compatible/hits.parquet"


@pytest.fixture(scope="session")
def clickbench_hits_df():
    """Materialize first N rows of hits in memory for stable SQL benchmarks."""
    ROW_LIMIT = 1_000_000

    io_config = IOConfig(s3=S3Config(anonymous=True, region_name="eu-central-1"))
    return (
        daft.read_parquet(DEFAULT_HITS_URL, io_config=io_config)
        .limit(ROW_LIMIT)
        .with_column("EventDate", col("EventDate").cast(DataType.date()))
        .with_column("EventTime", col("EventTime").cast(DataType.timestamp("s")))
        .collect()
    )


CLICKBENCH_CODSPEED_QUERY_IDS = (0, 1, 2, 3, 4, 6, 7, 19, 36, 37, 38, 39, 40, 41, 42)


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native",
    reason="requires Native Runner to be in use",
)
@pytest.mark.benchmark(group="clickbench")
@pytest.mark.parametrize("q", CLICKBENCH_CODSPEED_QUERY_IDS)
def test_clickbench_sql(clickbench_hits_df, benchmark_with_memray, q):
    query = CLICKBENCH_QUERIES[q]
    hits = clickbench_hits_df

    def f():
        return daft.sql(query, hits=hits).collect()

    benchmark_with_memray(f, f"clickbench-q{q}")
