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
    values = list(range(1, 1000))
    xs = list(range(1, 1000))
    ys = list(range(1, 1000))
    random.shuffle(xs)
    random.shuffle(ys)
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

    expected_order_x_asc = result_df.sort_values("x").reset_index(drop=True)
    expected_row_numbers = list(range(1, len(result_df) + 1))
    actual_row_numbers = result_df.sort_values("row_by_x_asc")["row_by_x_asc"].tolist()
    assert actual_row_numbers == expected_row_numbers, "row_number for x ascending is incorrect"

    for i, row in expected_order_x_asc.iterrows():
        expected_row_num = i + 1
        actual_row_num = row["row_by_x_asc"]
        assert (
            expected_row_num == actual_row_num
        ), f"Expected row {expected_row_num}, got {actual_row_num} for x={row['x']}"

    expected_order_y_asc = result_df.sort_values("y").reset_index(drop=True)
    for i, row in expected_order_y_asc.iterrows():
        expected_row_num = i + 1
        actual_row_num = row["row_by_y_asc"]
        assert (
            expected_row_num == actual_row_num
        ), f"Expected row {expected_row_num}, got {actual_row_num} for y={row['y']}"

    expected_order_value_asc = result_df.sort_values("value").reset_index(drop=True)
    for i, row in expected_order_value_asc.iterrows():
        expected_row_num = i + 1
        actual_row_num = row["row_by_value_asc"]
        assert (
            expected_row_num == actual_row_num
        ), f"Expected row {expected_row_num}, got {actual_row_num} for value={row['value']}"

    expected_order_x_desc = result_df.sort_values("x", ascending=False).reset_index(drop=True)
    for i, row in expected_order_x_desc.iterrows():
        expected_row_num = i + 1
        actual_row_num = row["row_by_x_desc"]
        assert (
            expected_row_num == actual_row_num
        ), f"Expected row {expected_row_num}, got {actual_row_num} for x={row['x']} (desc)"

    expected_order_y_desc = result_df.sort_values("y", ascending=False).reset_index(drop=True)
    for i, row in expected_order_y_desc.iterrows():
        expected_row_num = i + 1
        actual_row_num = row["row_by_y_desc"]
        assert (
            expected_row_num == actual_row_num
        ), f"Expected row {expected_row_num}, got {actual_row_num} for y={row['y']} (desc)"

    expected_order_value_desc = result_df.sort_values("value", ascending=False).reset_index(drop=True)
    for i, row in expected_order_value_desc.iterrows():
        expected_row_num = i + 1
        actual_row_num = row["row_by_value_desc"]
        assert (
            expected_row_num == actual_row_num
        ), f"Expected row {expected_row_num}, got {actual_row_num} for value={row['value']} (desc)"

    expected_order_xy_asc = result_df.sort_values(["x", "y"]).reset_index(drop=True)
    for i, row in expected_order_xy_asc.iterrows():
        expected_row_num = i + 1
        actual_row_num = row["row_by_xy_asc"]
        assert (
            expected_row_num == actual_row_num
        ), f"Expected row {expected_row_num}, got {actual_row_num} for x={row['x']}, y={row['y']}"

    expected_order_xy_mixed = result_df.sort_values(["x", "y"], ascending=[True, False]).reset_index(drop=True)
    for i, row in expected_order_xy_mixed.iterrows():
        expected_row_num = i + 1
        actual_row_num = row["row_by_xy_mixed"]
        assert (
            expected_row_num == actual_row_num
        ), f"Expected row {expected_row_num}, got {actual_row_num} for x={row['x']}, y={row['y']} (mixed)"
