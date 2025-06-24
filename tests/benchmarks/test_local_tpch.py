from __future__ import annotations

import sys

import pytest

import daft
from benchmarking.tpch import answers

if sys.platform == "win32":
    pytest.skip(allow_module_level=True)

from tests.conftest import get_tests_daft_runner_name
from tests.integration.conftest import check_answer  # noqa F401

TPCH_QUESTIONS = list(range(1, 11))


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native",
    reason="requires Native Runner to be in use",
)
@pytest.mark.benchmark(group="tpch")
@pytest.mark.parametrize("q", TPCH_QUESTIONS)
def test_tpch(tmp_path, check_answer, get_df, benchmark_with_memray, q):  # noqa F811
    get_df, num_parts = get_df

    def f():
        question = getattr(answers, f"q{q}")
        daft_df = question(get_df)
        return daft_df.to_arrow()

    benchmark_group = f"q{q}-parts-{num_parts}"
    daft_pd_df = benchmark_with_memray(f, benchmark_group).to_pandas()
    check_answer(daft_pd_df, q, tmp_path)


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native",
    reason="requires Native Runner to be in use",
)
@pytest.mark.benchmark(group="tpch")
@pytest.mark.parametrize("q", TPCH_QUESTIONS)
def test_tpch_sql(tmp_path, check_answer, get_df, benchmark_with_memray, q):  # noqa F811
    from daft.sql import SQLCatalog

    get_df, num_parts = get_df

    # TODO: remove this once SQL allows case-insensitive column names
    def lowercase_column_names(df):
        return df.select(*[daft.col(name).alias(name.lower()) for name in df.column_names])

    table_names = [
        "part",
        "supplier",
        "partsupp",
        "customer",
        "orders",
        "lineitem",
        "nation",
        "region",
    ]
    catalog = SQLCatalog({tbl: lowercase_column_names(get_df(tbl)) for tbl in table_names})

    with open(f"benchmarking/tpch/queries/{q:02}.sql") as query_file:
        query = query_file.read()

    def f():
        daft_df = daft.sql(query, catalog=catalog)
        return daft_df.to_arrow()

    benchmark_group = f"q{q}-sql-parts-{num_parts}"
    daft_pd_df = benchmark_with_memray(f, benchmark_group).to_pandas()
    check_answer(daft_pd_df, q, tmp_path)
