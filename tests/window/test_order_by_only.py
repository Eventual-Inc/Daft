from __future__ import annotations

import random

import pandas as pd
import pytest

from daft import Window, col
from daft.context import get_context
from daft.functions import dense_rank, rank, row_number
from tests.conftest import assert_df_equals, get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray"
    and get_context().daft_execution_config.use_experimental_distributed_engine is False,
    reason="requires Native Runner or Flotilla to be in use",
)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 20, 50, 100])
def test_row_number(make_df, repartition_nparts, with_morsel_size):
    df = make_df(
        {"category": ["A", "A", "A", "B", "B", "B", "C", "C"], "sales": [100, 200, 50, 500, 125, 300, 250, 150]},
        repartition=repartition_nparts,
        repartition_columns=["category"],
    )

    window_spec = Window().order_by("sales", desc=False)

    result = df.select(
        col("category"), col("sales"), row_number().over(window_spec).alias("row_number_sales")
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 125, 300, 250, 150],
        "row_number_sales": [2, 5, 1, 8, 3, 7, 6, 4],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()), check_dtype=False)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 20, 50, 100])
def test_order_by_only_row_number(make_df, repartition_nparts, with_morsel_size):
    """Test row_number function with order_by only (no partition_by)."""
    random.seed(42)

    data = []
    n = 1000
    total = n + 1

    xs = list(range(1, n + 1))
    ys = list(range(1, n + 1))
    values = list(range(1, n + 1))

    random.shuffle(xs)
    random.shuffle(ys)
    random.shuffle(values)

    for x, y, value in zip(xs, ys, values):
        data.append({"x": x, "y": y, "value": value})

    df = make_df(data, repartition=repartition_nparts, repartition_columns=["x", "y"])

    window_spec_x = Window().order_by("x", desc=False)
    window_spec_y = Window().order_by("y", desc=False)
    window_spec_value = Window().order_by("value", desc=False)

    window_spec_x_desc = Window().order_by("x", desc=True)
    window_spec_y_desc = Window().order_by("y", desc=True)
    window_spec_value_desc = Window().order_by("value", desc=True)

    window_spec_xy = Window().order_by(["x", "y"], desc=[False, False])
    window_spec_xy_mixed = Window().order_by(["x", "y"], desc=[False, True])

    result = df.select(
        col("x"),
        col("y"),
        col("value"),
        row_number().over(window_spec_x).alias("row_by_x_asc"),
        row_number().over(window_spec_y).alias("row_by_y_asc"),
        row_number().over(window_spec_value).alias("row_by_value_asc"),
        row_number().over(window_spec_x_desc).alias("row_by_x_desc"),
        row_number().over(window_spec_y_desc).alias("row_by_y_desc"),
        row_number().over(window_spec_value_desc).alias("row_by_value_desc"),
        row_number().over(window_spec_xy).alias("row_by_xy_asc"),
        row_number().over(window_spec_xy_mixed).alias("row_by_xy_mixed"),
    ).collect()

    result_df = result.to_pandas()

    for _, row in result_df.iterrows():
        assert row["row_by_x_asc"] == row["x"], f"row_by_x_asc {row['row_by_x_asc']} should equal x {row['x']}"
        assert row["row_by_y_asc"] == row["y"], f"row_by_y_asc {row['row_by_y_asc']} should equal y {row['y']}"
        assert (
            row["row_by_value_asc"] == row["value"]
        ), f"row_by_value_asc {row['row_by_value_asc']} should equal value {row['value']}"

        assert (
            row["row_by_x_desc"] == total - row["x"]
        ), f"row_by_x_desc {row['row_by_x_desc']} should equal {total} - x {row['x']}"
        assert (
            row["row_by_y_desc"] == total - row["y"]
        ), f"row_by_y_desc {row['row_by_y_desc']} should equal {total} - y {row['y']}"
        assert (
            row["row_by_value_desc"] == total - row["value"]
        ), f"row_by_value_desc {row['row_by_value_desc']} should equal {total} - value {row['value']}"

        assert row["row_by_xy_asc"] == row["x"], f"row_by_xy_asc {row['row_by_xy_asc']} should equal x {row['x']}"
        assert row["row_by_xy_mixed"] == row["x"], f"row_by_xy_mixed {row['row_by_xy_mixed']} should equal x {row['x']}"


@pytest.mark.parametrize("repartition_nparts", [1, 2, 20, 50, 100])
def test_rank(make_df, repartition_nparts, with_morsel_size):
    """Test rank function with order_by only (no partition_by)."""
    random.seed(42)

    data = []
    n = 1000

    values = []
    for i in range(1, 11):
        values.extend([i] * 100)

    values = values[:n]

    random.shuffle(values)
    categories = ["A", "B", "C", "D", "E"]

    for i, value in enumerate(values):
        data.append({"id": i, "category": random.choice(categories), "value": value})

    df = make_df(data, repartition=repartition_nparts, repartition_columns=["id", "category"])

    window_spec_asc = Window().order_by("value", desc=False)
    window_spec_desc = Window().order_by("value", desc=True)

    result = df.select(
        col("id"),
        col("category"),
        col("value"),
        rank().over(window_spec_asc).alias("rank_asc"),
        rank().over(window_spec_desc).alias("rank_desc"),
    ).collect()

    result_df = result.to_pandas()

    value_to_rank_asc = {}
    current_rank = 1
    for value in sorted(values):
        if value not in value_to_rank_asc:
            value_to_rank_asc[value] = current_rank
        current_rank += 1

    value_to_rank_desc = {}
    current_rank = 1
    for value in sorted(values, reverse=True):
        if value not in value_to_rank_desc:
            value_to_rank_desc[value] = current_rank
        current_rank += 1

    expected_rank_asc = []
    expected_rank_desc = []
    for value in values:
        expected_rank_asc.append(value_to_rank_asc[value])
        expected_rank_desc.append(value_to_rank_desc[value])

    expected = pd.DataFrame(
        {
            "id": list(range(n)),
            "category": [data[i]["category"] for i in range(n)],
            "value": values,
            "rank_asc": expected_rank_asc,
            "rank_desc": expected_rank_desc,
        }
    )

    result_df = result_df.sort_values(by="id").reset_index(drop=True)
    expected = expected.sort_values(by="id").reset_index(drop=True)

    assert_df_equals(result_df, expected, sort_key=["id", "category", "value"], check_dtype=False)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 20, 50, 100])
def test_dense_rank(make_df, repartition_nparts, with_morsel_size):
    """Test dense_rank function with order_by only (no partition_by)."""
    random.seed(43)

    data = []
    n = 1000

    values = []
    for i in range(1, 11):
        values.extend([i] * 100)

    values = values[:n]

    random.shuffle(values)

    categories = ["A", "B", "C", "D", "E"]

    for i, value in enumerate(values):
        data.append({"id": i, "category": random.choice(categories), "value": value})

    df = make_df(data, repartition=repartition_nparts, repartition_columns=["id", "category"])

    window_spec_asc = Window().order_by("value", desc=False)
    window_spec_desc = Window().order_by("value", desc=True)

    result = df.select(
        col("id"),
        col("category"),
        col("value"),
        dense_rank().over(window_spec_asc).alias("dense_rank_asc"),
        dense_rank().over(window_spec_desc).alias("dense_rank_desc"),
    ).collect()

    result_df = result.to_pandas()

    unique_values_asc = sorted(set(values))
    unique_values_desc = sorted(set(values), reverse=True)

    value_to_dense_rank_asc = {value: i + 1 for i, value in enumerate(unique_values_asc)}
    value_to_dense_rank_desc = {value: i + 1 for i, value in enumerate(unique_values_desc)}

    expected = pd.DataFrame(
        {
            "id": list(range(n)),
            "category": [data[i]["category"] for i in range(n)],
            "value": values,
            "dense_rank_asc": [value_to_dense_rank_asc[value] for value in values],
            "dense_rank_desc": [value_to_dense_rank_desc[value] for value in values],
        }
    )

    result_df = result_df.sort_values(by="id").reset_index(drop=True)
    expected = expected.sort_values(by="id").reset_index(drop=True)

    assert_df_equals(result_df, expected, sort_key=["id", "category", "value"], check_dtype=False)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 20, 50, 100])
def test_rank_with_1k_distinct_values(make_df, repartition_nparts, with_morsel_size):
    """Test rank, dense_rank, and row_number with 15,000 distinct values."""
    n = 1000
    random.seed(45)

    values = list(range(n))
    random.shuffle(values)

    data = [{"id": i, "value": value} for i, value in enumerate(values)]

    df = make_df(data, repartition=repartition_nparts, repartition_columns=["id"])

    window_asc = Window().order_by("value", desc=False)
    window_desc = Window().order_by("value", desc=True)

    result = df.select(
        col("id"),
        col("value"),
        row_number().over(window_asc).alias("row_number_asc"),
        rank().over(window_asc).alias("rank_asc"),
        dense_rank().over(window_asc).alias("dense_rank_asc"),
        row_number().over(window_desc).alias("row_number_desc"),
        rank().over(window_desc).alias("rank_desc"),
        dense_rank().over(window_desc).alias("dense_rank_desc"),
    ).collect()

    result_df = result.to_pandas().sort_values("id").reset_index(drop=True)

    value_to_row = {v: i + 1 for i, v in enumerate(sorted(values))}
    value_to_rev_row = {v: i + 1 for i, v in enumerate(sorted(values, reverse=True))}

    expected_df = (
        pd.DataFrame(
            {
                "id": list(range(n)),
                "value": values,
                "row_number_asc": [value_to_row[v] for v in values],
                "rank_asc": [value_to_row[v] for v in values],
                "dense_rank_asc": [value_to_row[v] for v in values],
                "row_number_desc": [value_to_rev_row[v] for v in values],
                "rank_desc": [value_to_rev_row[v] for v in values],
                "dense_rank_desc": [value_to_rev_row[v] for v in values],
            }
        )
        .sort_values("id")
        .reset_index(drop=True)
    )

    assert_df_equals(result_df, expected_df, sort_key=["id", "value"], check_dtype=False)


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="requires Native Runner, Flotilla doesn't support sort and Ray runner doesn't support window functions",
)
@pytest.mark.parametrize(
    "desc,nulls_first,expected",
    [
        (
            False,
            False,
            {
                "row_num": [1, 5, 2, 6, 3, 4],
                "rank": [1, 5, 2, 5, 3, 3],
                "dense_rank": [1, 4, 2, 4, 3, 3],
            },
        ),
        (
            False,
            True,
            {
                "row_num": [3, 1, 4, 2, 5, 6],
                "rank": [3, 1, 4, 1, 5, 5],
                "dense_rank": [2, 1, 3, 1, 4, 4],
            },
        ),
        (
            True,
            False,
            {
                "row_num": [4, 5, 3, 6, 1, 2],
                "rank": [4, 5, 3, 5, 1, 1],
                "dense_rank": [3, 4, 2, 4, 1, 1],
            },
        ),
        (
            True,
            True,
            {
                "row_num": [6, 1, 5, 2, 3, 4],
                "rank": [6, 1, 5, 1, 3, 3],
                "dense_rank": [4, 1, 3, 1, 2, 2],
            },
        ),
    ],
)
def test_window_nulls_first_or_last(make_df, desc, nulls_first, expected):
    """Test window functions with nulls_first=True/False."""
    data = [
        {"id": 1, "value": 10},
        {"id": 2, "value": None},
        {"id": 3, "value": 20},
        {"id": 4, "value": None},
        {"id": 5, "value": 30},
        {"id": 6, "value": 30},
    ]
    df = make_df(data)

    window_specs = Window().order_by("value", desc=desc, nulls_first=nulls_first)
    window_functions = {
        "row_num": row_number(),
        "rank": rank(),
        "dense_rank": dense_rank(),
    }
    select_exprs = [col("id"), col("value")]
    for func_name, func in window_functions.items():
        select_exprs.append(func.over(window_specs).alias(f"{func_name}"))

    result = df.select(*select_exprs).sort("id").collect()
    result_df = result.to_pandas()

    expected_data = {
        "id": [1, 2, 3, 4, 5, 6],
        "value": [10, None, 20, None, 30, 30],
    }
    expected_data.update(expected)
    expected = pd.DataFrame(expected_data)

    assert_df_equals(result_df, expected, sort_key=["id", "value"], check_dtype=False)
