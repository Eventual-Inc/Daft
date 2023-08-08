from __future__ import annotations

import pytest

import daft
from daft import col
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import PruneColumns
from tests.optimizer.conftest import assert_plan_eq


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner(
        [
            RuleBatch(
                "fold_projections",
                Once,
                [PruneColumns()],
            )
        ]
    )


def test_prune_columns_projection_projection(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    df_unoptimized = df.select("sepal_width", "sepal_length").select("sepal_length")
    df_optimized = df.select("sepal_length").select("sepal_length")

    assert df_unoptimized.column_names == ["sepal_length"]
    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)


def test_prune_columns_projection_projection_aliases(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    df_unoptimized = df.select(col("sepal_width").alias("bar"), col("sepal_length").alias("foo")).select(
        col("foo").alias("bar")
    )
    df_optimized = df.select(col("sepal_length").alias("foo")).select(col("foo").alias("bar"))

    assert df_unoptimized.column_names == ["bar"]
    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)


def test_prune_columns_local_aggregate(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    df_unoptimized = df.agg(
        [
            ("sepal_length", "mean"),
            ("sepal_width", "sum"),
        ]
    ).select("sepal_length")
    df_optimized = (
        df.select("sepal_length")
        .agg(
            [
                ("sepal_length", "mean"),
            ]
        )
        .select("sepal_length")
    )

    assert df_unoptimized.column_names == ["sepal_length"]
    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)


def test_prune_columns_local_aggregate_aliases(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    df_unoptimized = df.agg(
        [
            (col("sepal_length").alias("foo"), "mean"),
            (col("sepal_width").alias("bar"), "sum"),
        ]
    ).select(col("foo").alias("bar"))
    df_optimized = (
        df.select("sepal_length")
        .agg(
            [
                (col("sepal_length").alias("foo"), "mean"),
            ]
        )
        .select(col("foo").alias("bar"))
    )

    assert df_unoptimized.column_names == ["bar"]
    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)


@pytest.mark.parametrize(
    "key_selection", [pytest.param([], id="KeySelection:0"), pytest.param(["variety"], id="KeySelection:1")]
)
@pytest.mark.parametrize(
    "left_selection",
    [
        # TODO: enable after https://github.com/Eventual-Inc/Daft/issues/594 is fixed
        # pytest.param([], id="LeftSelection:0"),
        pytest.param(["sepal_length"], id="LeftSelection:1"),
    ],
)
@pytest.mark.parametrize(
    "right_selection",
    [
        pytest.param([], id="RightSelection:0"),
        pytest.param(["right.sepal_length"], id="RightSelection:1"),
    ],
)
@pytest.mark.parametrize("alias", [True, False])
def test_projection_join_pruning(
    valid_data: list[dict[str, float]],
    key_selection: list[str],
    left_selection: list[str],
    right_selection: list[str],
    alias: bool,
    optimizer,
) -> None:
    # Test invalid when no columns are selected
    if left_selection == [] and right_selection == [] and key_selection == []:
        return

    # If alias=True, run aliasing to munge the selected column names
    left_selection_final = [col(s).alias(f"foo.{s}") for s in left_selection] if alias else left_selection
    right_selection_final = [col(s).alias(f"foo.{s}") for s in right_selection] if alias else right_selection
    key_selection_final = [col(s).alias(f"foo.{s}") for s in key_selection] if alias else key_selection

    df = daft.from_pylist(valid_data)
    df_unoptimized = df.join(df, on="variety").select(
        *left_selection_final, *right_selection_final, *key_selection_final
    )
    df_optimized = (
        df.select(*left_selection, "variety")
        .join(df.select(*[s.replace("right.", "") for s in right_selection], "variety"), on="variety")
        .select(*left_selection_final, *right_selection_final, *key_selection_final)
    )
    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)


def test_projection_concat_pruning(valid_data, optimizer):
    df1 = daft.from_pylist(valid_data)
    df2 = daft.from_pylist(valid_data)
    concatted = df1.concat(df2)

    selected = concatted.select("sepal_length")
    optimized = optimizer(selected._get_current_builder()._plan)

    expected = df1.select(col("sepal_length")).concat(df2.select(col("sepal_length"))).select(col("sepal_length"))
    assert_plan_eq(optimized, expected._get_current_builder()._plan)


@pytest.mark.parametrize(
    "key_aggregation",
    [pytest.param([], id="KeyAgg:0"), pytest.param([(col("variety").alias("count(variety)"), "count")], id="KeyAgg:1")],
)
@pytest.mark.parametrize(
    "left_aggregation",
    [
        # TODO: enable after https://github.com/Eventual-Inc/Daft/issues/594 is fixed
        # pytest.param([], id="LeftAgg:0"),
        pytest.param([("sepal_length", "sum")], id="LeftAgg:1"),
    ],
)
@pytest.mark.parametrize(
    "right_aggregation",
    [
        pytest.param([], id="RightAgg:0"),
        pytest.param([("right.sepal_length", "sum")], id="RightAgg:1"),
    ],
)
@pytest.mark.parametrize("alias", [True, False])
def test_local_aggregate_join_prune(
    valid_data: list[dict[str, float]],
    key_aggregation: list[tuple[str, str]],
    left_aggregation: list[tuple[str, str]],
    right_aggregation: list[tuple[str, str]],
    alias: bool,
    optimizer,
) -> None:
    # No aggregations to perform
    if key_aggregation == [] and left_aggregation == [] and right_aggregation == []:
        return

    # If alias=True, run aliasing to munge the selected column names
    left_final_aggregation = [(col(c).alias(f"foo.{c}"), a) for c, a in left_aggregation] if alias else left_aggregation
    right_final_aggregation = (
        [(col(c).alias(f"foo.{c}"), a) for c, a in right_aggregation] if alias else right_aggregation
    )
    key_final_aggregation = [(c.alias(f"foo.{c}"), a) for c, a in key_aggregation] if alias else key_aggregation

    # Columns in the pushed-down Projections to the left/right children
    left_selection = [c for c, _ in left_aggregation]
    right_selection = [c for c, _ in right_aggregation]
    right_selection_prejoin = [c.replace("right.", "") for c in right_selection]

    df = daft.from_pylist(valid_data)
    df_unoptimized = (
        df.join(df, on="variety")
        .groupby("variety")
        .agg([*key_final_aggregation, *left_final_aggregation, *right_final_aggregation])
    )
    df_optimized = (
        df
        # Left pushdowns
        .select(
            *left_selection,
            "variety",
        )
        .join(
            # Right pushdowns
            df.select(
                *right_selection_prejoin,
                "variety",
            ),
            on="variety",
        )
        # Pushdown projection before LocalAggregation
        .select(
            *left_selection,
            "variety",
            *right_selection,
        )
        .groupby("variety")
        .agg([*key_final_aggregation, *left_final_aggregation, *right_final_aggregation])
    )
    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)


def test_projection_on_scan(valid_data_json_path, optimizer):
    df = daft.read_json(valid_data_json_path)
    df = df.with_column("sepal_length", col("sepal_length") + 1)

    # Projection cannot be pushed down into TabularFileScan
    assert_plan_eq(optimizer(df._get_current_builder()._plan), df._get_current_builder()._plan)
