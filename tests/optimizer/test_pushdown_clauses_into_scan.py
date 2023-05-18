from __future__ import annotations

import pytest

import daft
from daft import DataFrame, col
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan, TabularFilesScan
from daft.logical.optimizer import PushDownClausesIntoScan
from tests.optimizer.conftest import assert_plan_eq


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner([RuleBatch("push_into_scan", Once, [PushDownClausesIntoScan()])])


def test_push_projection_scan_all_cols(valid_data_json_path: str, optimizer):
    df_unoptimized_scan = daft.read_json(valid_data_json_path)
    df_unoptimized = df_unoptimized_scan.select("sepal_length")

    # TODO: switch to using .read_parquet(columns=["sepal_length"]) once that's implemented
    # Manually construct a plan to be what we expect after optimization
    df_optimized = DataFrame(
        TabularFilesScan(
            schema=df_unoptimized_scan.plan()._schema,
            predicate=df_unoptimized_scan.plan()._predicate,
            columns=["sepal_length"],
            source_info=df_unoptimized_scan.plan()._source_info,
            fs=df_unoptimized_scan.plan()._fs,
            filepaths_child=df_unoptimized_scan.plan()._filepaths_child,
            filepaths_column_name=df_unoptimized_scan.plan()._filepaths_column_name,
        )
    )

    assert_plan_eq(optimizer(df_unoptimized.plan()), df_optimized.plan())


def test_push_projection_scan_all_cols_alias(valid_data_json_path: str, optimizer):
    df_unoptimized_scan = daft.read_json(valid_data_json_path)
    df_unoptimized = df_unoptimized_scan.select(col("sepal_length").alias("foo"))

    # TODO: switch to using .read_parquet(columns=["sepal_length"]) once that's implemented
    # Manually construct a plan to be what we expect after optimization
    df_optimized = DataFrame(
        TabularFilesScan(
            schema=df_unoptimized_scan.plan()._schema,
            predicate=df_unoptimized_scan.plan()._predicate,
            columns=["sepal_length"],
            source_info=df_unoptimized_scan.plan()._source_info,
            fs=df_unoptimized_scan.plan()._fs,
            filepaths_child=df_unoptimized_scan.plan()._filepaths_child,
            filepaths_column_name=df_unoptimized_scan.plan()._filepaths_column_name,
        )
    )
    df_optimized = df_optimized.select(col("sepal_length").alias("foo"))

    assert_plan_eq(optimizer(df_unoptimized.plan()), df_optimized.plan())


def test_push_projection_scan_some_cols_aliases(valid_data_json_path: str, optimizer):
    df_unoptimized_scan = daft.read_json(valid_data_json_path)
    df_unoptimized = df_unoptimized_scan.select(col("sepal_length").alias("foo"), col("sepal_width") + 1)

    # TODO: switch to using .read_parquet(columns=["sepal_length"]) once that's implemented
    # Manually construct a plan to be what we expect after optimization
    df_optimized = DataFrame(
        TabularFilesScan(
            schema=df_unoptimized_scan.plan()._schema,
            predicate=df_unoptimized_scan.plan()._predicate,
            columns=["sepal_length", "sepal_width"],
            source_info=df_unoptimized_scan.plan()._source_info,
            fs=df_unoptimized_scan.plan()._fs,
            filepaths_child=df_unoptimized_scan.plan()._filepaths_child,
            filepaths_column_name=df_unoptimized_scan.plan()._filepaths_column_name,
        )
    )
    df_optimized = df_optimized.select(col("sepal_length").alias("foo"), col("sepal_width") + 1)

    assert_plan_eq(optimizer(df_unoptimized.plan()), df_optimized.plan())
