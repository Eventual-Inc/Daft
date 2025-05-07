from __future__ import annotations

import random

import pandas as pd
import pytest

from daft import Window, col
from daft.functions import row_number
from tests.conftest import assert_df_equals, get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner"
)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 20, 50, 100])
def test_row_number_function(make_df, repartition_nparts):
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
def test_order_by_only_row_number(make_df, repartition_nparts):
    """Test row_number function with order_by only (no partition_by)."""
    random.seed(42)

    data = []
    n = 1000000
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
