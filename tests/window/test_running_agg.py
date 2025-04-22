from __future__ import annotations

import random

import pandas as pd
import pytest

from daft import Window, col
from tests.conftest import assert_df_equals, get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner"
)


def test_running_sum(make_df):
    """Test running sum over partitioned ordered windows."""
    random.seed(42)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        values = [random.randint(1, 100) for _ in range(10)]
        running_sum = 0

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            running_sum += value
            expected_data.append({"category": category, "ts": ts, "value": value, "running_sum": running_sum})

    df = make_df(data)

    window_spec = (
        Window()
        .partition_by("category")
        .order_by("ts", desc=False)
        .rows_between(Window.unbounded_preceding, Window.current_row)
    )

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").sum().over(window_spec).alias("running_sum")
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_descending_running_sum(make_df):
    """Test running sum over partitioned windows with descending order."""
    random.seed(43)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        values = [random.randint(1, 100) for _ in range(10)]

        desc_values = values.copy()
        desc_values.reverse()
        running_sum = 0
        desc_running_sums = []

        for val in desc_values:
            running_sum += val
            desc_running_sums.append(running_sum)

        desc_running_sums.reverse()

        for ts, (value, run_sum) in enumerate(zip(values, desc_running_sums)):
            data.append({"category": category, "ts": ts, "value": value})
            expected_data.append({"category": category, "ts": ts, "value": value, "running_sum": run_sum})

    df = make_df(data)

    window_spec = (
        Window()
        .partition_by("category")
        .order_by("ts", desc=True)
        .rows_between(Window.unbounded_preceding, Window.current_row)
    )

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").sum().over(window_spec).alias("running_sum")
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_min_periods(make_df):
    """Test running aggregation with minimum periods requirement."""
    random.seed(44)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [random.randint(1, 100) for _ in range(10)]

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            window_size = min(ts + 1, 3)
            if window_size < 3:
                expected_avg = None
            else:
                expected_avg = sum(values[ts - 2 : ts + 1]) / 3.0

            expected_data.append({"category": category, "ts": ts, "value": value, "window_avg": expected_avg})

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(-2, 0, min_periods=3)

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").mean().over(window_spec).alias("window_avg")
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_fixed_start_adjustable_end(make_df):
    """Test running aggregation with fixed start and adjustable end."""
    random.seed(45)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [random.randint(1, 100) for _ in range(10)]

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            end_idx = min(ts + 2 + 1, len(values))
            window_sum = sum(values[:end_idx])

            expected_data.append({"category": category, "ts": ts, "value": value, "window_sum": window_sum})

    df = make_df(data)

    window_spec = (
        Window().partition_by("category").order_by("ts", desc=False).rows_between(Window.unbounded_preceding, 2)
    )

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").sum().over(window_spec).alias("window_sum")
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_adjustable_start_fixed_end(make_df):
    """Test running aggregation with adjustable start and fixed end."""
    random.seed(46)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [random.randint(1, 100) for _ in range(10)]

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            start_idx = max(0, ts - 3)
            window_sum = sum(values[start_idx:])

            expected_data.append({"category": category, "ts": ts, "value": value, "window_sum": window_sum})

    df = make_df(data)

    window_spec = (
        Window().partition_by("category").order_by("ts", desc=False).rows_between(-3, Window.unbounded_following)
    )

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").sum().over(window_spec).alias("window_sum")
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_symmetric_window(make_df):
    """Test running aggregation with symmetric window around current row."""
    random.seed(47)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [random.randint(1, 100) for _ in range(10)]

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            start_idx = max(0, ts - 2)
            end_idx = min(ts + 2 + 1, len(values))
            window_vals = values[start_idx:end_idx]
            window_avg = sum(window_vals) / len(window_vals)

            expected_data.append({"category": category, "ts": ts, "value": value, "window_avg": window_avg})

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(-2, 2)

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").mean().over(window_spec).alias("window_avg")
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_multiple_aggregations(make_df):
    """Test multiple aggregation functions over the same window."""
    random.seed(48)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [random.randint(1, 100) for _ in range(10)]

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            start_idx = max(0, ts - 1)
            end_idx = min(ts + 1 + 1, len(values))
            window_vals = values[start_idx:end_idx]

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "window_sum": sum(window_vals),
                    "window_min": min(window_vals),
                    "window_max": max(window_vals),
                    "window_avg": sum(window_vals) / len(window_vals),
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(-1, 1)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").sum().over(window_spec).alias("window_sum"),
        col("value").min().over(window_spec).alias("window_min"),
        col("value").max().over(window_spec).alias("window_max"),
        col("value").mean().over(window_spec).alias("window_avg"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_different_min_periods(make_df):
    """Test the effect of different min_periods values on the same window."""
    random.seed(49)

    data = []
    expected_data = []

    for category in ["A"]:
        values = [random.randint(1, 100) for _ in range(10)]

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            start_idx = max(0, ts - 3)
            window_vals = values[start_idx : ts + 1]

            avg_no_min = sum(window_vals) / len(window_vals)
            avg_min_2 = sum(window_vals) / len(window_vals) if len(window_vals) >= 2 else None
            avg_min_3 = sum(window_vals) / len(window_vals) if len(window_vals) >= 3 else None
            avg_min_4 = sum(window_vals) / len(window_vals) if len(window_vals) >= 4 else None

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "avg_no_min": avg_no_min,
                    "avg_min_2": avg_min_2,
                    "avg_min_3": avg_min_3,
                    "avg_min_4": avg_min_4,
                }
            )

    df = make_df(data)

    # TODO: would like to support this syntax as well
    # base_window = Window().partition_by("category").order_by("ts", desc=False)
    # window_no_min = base_window.rows_between(-3, 0, min_periods=1)
    # window_min_2 = base_window.rows_between(-3, 0, min_periods=2)
    # window_min_3 = base_window.rows_between(-3, 0, min_periods=3)
    # window_min_4 = base_window.rows_between(-3, 0, min_periods=4)

    window_no_min = Window().partition_by("category").order_by("ts", desc=False).rows_between(-3, 0, min_periods=1)
    window_min_2 = Window().partition_by("category").order_by("ts", desc=False).rows_between(-3, 0, min_periods=2)
    window_min_3 = Window().partition_by("category").order_by("ts", desc=False).rows_between(-3, 0, min_periods=3)
    window_min_4 = Window().partition_by("category").order_by("ts", desc=False).rows_between(-3, 0, min_periods=4)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").mean().over(window_no_min).alias("avg_no_min"),
        col("value").mean().over(window_min_2).alias("avg_min_2"),
        col("value").mean().over(window_min_3).alias("avg_min_3"),
        col("value").mean().over(window_min_4).alias("avg_min_4"),
    ).collect()

    print(result.to_pandas())

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_string_min_max(make_df):
    """Test min and max window aggregations with string values."""
    data = []
    expected_data = []

    categories = ["A", "B"]
    string_values = {
        "A": ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "lemon"],
        "B": [
            "almond",
            "brazil nut",
            "cashew",
            "hazelnut",
            "macadamia",
            "pecan",
            "pistachio",
            "walnut",
            "peanut",
            "chestnut",
        ],
    }

    for category in categories:
        values = string_values[category]

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "text": value})

            start_idx = max(0, ts - 2)
            end_idx = min(ts + 1, len(values))
            window_vals = values[start_idx:end_idx]

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "text": value,
                    "window_min": min(window_vals),
                    "window_max": max(window_vals),
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(-2, 0)

    result = df.select(
        col("category"),
        col("ts"),
        col("text"),
        col("text").min().over(window_spec).alias("window_min"),
        col("text").max().over(window_spec).alias("window_max"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)
