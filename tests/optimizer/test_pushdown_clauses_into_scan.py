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
            schema=df_unoptimized_scan._get_current_builder()._plan._schema,
            predicate=df_unoptimized_scan._get_current_builder()._plan._predicate,
            columns=["sepal_length"],
            file_format_config=df_unoptimized_scan._get_current_builder()._plan._file_format_config,
            storage_config=df_unoptimized_scan._get_current_builder()._plan._storage_config,
            filepaths_child=df_unoptimized_scan._get_current_builder()._plan._filepaths_child,
        ).to_builder()
    )

    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)


def test_push_projection_scan_all_cols_alias(valid_data_json_path: str, optimizer):
    df_unoptimized_scan = daft.read_json(valid_data_json_path)
    df_unoptimized = df_unoptimized_scan.select(col("sepal_length").alias("foo"))

    # TODO: switch to using .read_parquet(columns=["sepal_length"]) once that's implemented
    # Manually construct a plan to be what we expect after optimization
    df_optimized = DataFrame(
        TabularFilesScan(
            schema=df_unoptimized_scan._get_current_builder()._plan._schema,
            predicate=df_unoptimized_scan._get_current_builder()._plan._predicate,
            columns=["sepal_length"],
            file_format_config=df_unoptimized_scan._get_current_builder()._plan._file_format_config,
            storage_config=df_unoptimized_scan._get_current_builder()._plan._storage_config,
            filepaths_child=df_unoptimized_scan._get_current_builder()._plan._filepaths_child,
        ).to_builder()
    )
    df_optimized = df_optimized.select(col("sepal_length").alias("foo"))

    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)


def test_push_projection_scan_some_cols_aliases(valid_data_json_path: str, optimizer):
    df_unoptimized_scan = daft.read_json(valid_data_json_path)
    df_unoptimized = df_unoptimized_scan.select(col("sepal_length").alias("foo"), col("sepal_width") + 1)

    # TODO: switch to using .read_parquet(columns=["sepal_length"]) once that's implemented
    # Manually construct a plan to be what we expect after optimization
    df_optimized = DataFrame(
        TabularFilesScan(
            schema=df_unoptimized_scan._get_current_builder()._plan._schema,
            predicate=df_unoptimized_scan._get_current_builder()._plan._predicate,
            columns=["sepal_length", "sepal_width"],
            file_format_config=df_unoptimized_scan._get_current_builder()._plan._file_format_config,
            storage_config=df_unoptimized_scan._get_current_builder()._plan._storage_config,
            filepaths_child=df_unoptimized_scan._get_current_builder()._plan._filepaths_child,
        ).to_builder()
    )
    df_optimized = df_optimized.select(col("sepal_length").alias("foo"), col("sepal_width") + 1)

    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)
