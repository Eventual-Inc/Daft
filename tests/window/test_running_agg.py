from __future__ import annotations

import random
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from daft import DataType, Window, col
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


def test_float32_running_sum(make_df):
    """Test running sum over float32 values."""
    random.seed(60)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        values = [random.uniform(0.1, 100.0) for _ in range(10)]
        running_sum = 0.0

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            running_sum += value
            expected_data.append({"category": category, "ts": ts, "value": value, "running_sum": running_sum})

    df = make_df(data)
    df = df.with_column("value", col("value").cast(DataType.float32()))

    window_spec = (
        Window()
        .partition_by("category")
        .order_by("ts", desc=False)
        .rows_between(Window.unbounded_preceding, Window.current_row)
    )

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").sum().over(window_spec).alias("running_sum")
    ).collect()

    expected_df = pd.DataFrame(expected_data)
    expected_df["value"] = expected_df["value"].astype(np.float32)
    expected_df["running_sum"] = expected_df["running_sum"].astype(np.float32)

    assert_df_equals(result.to_pandas(), expected_df, sort_key=["category", "ts"], check_dtype=True)


def test_float64_running_window(make_df):
    """Test running window operations over float64 values."""
    random.seed(61)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [random.uniform(0.1, 100.0) for _ in range(15)]

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            start_idx = max(0, ts - 2)
            end_idx = min(ts + 1, len(values))
            window_vals = values[start_idx:end_idx]

            window_sum = sum(window_vals)
            window_avg = sum(window_vals) / len(window_vals)
            window_min = min(window_vals)
            window_max = max(window_vals)

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "window_sum": window_sum,
                    "window_avg": window_avg,
                    "window_min": window_min,
                    "window_max": window_max,
                }
            )

    df = make_df(data)
    df = df.with_column("value", col("value").cast(DataType.float64()))

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(-2, 0)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").sum().over(window_spec).alias("window_sum"),
        col("value").mean().over(window_spec).alias("window_avg"),
        col("value").min().over(window_spec).alias("window_min"),
        col("value").max().over(window_spec).alias("window_max"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


@pytest.mark.skip(
    reason="""
Mismatch between expected and actual Arrow types for DataArray.
Field name: value
Logical type: Decimal(precision=5, scale=2)
Physical type: Decimal(precision=5, scale=2)
Expected Arrow physical type: Decimal(5, 2)
Actual Arrow Logical type: Decimal(32, 32)
"""
)
def test_decimal_running_sum(make_df):
    """Test running sum over decimal values."""
    random.seed(62)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [Decimal(str(round(random.uniform(0.01, 1000.0), 2))) for _ in range(10)]
        running_sum = Decimal("0.0")

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


def test_mixed_numeric_types(make_df):
    """Test window operations with mixed numeric data types."""
    random.seed(63)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        int_values = [random.randint(1, 100) for _ in range(10)]
        float32_values = [float(random.uniform(0.1, 100.0)) for _ in range(10)]

        all_values = int_values + float32_values
        random.shuffle(all_values)

        running_sum = 0.0

        for ts, value in enumerate(all_values):
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


def test_sliding_sum_negative_bounds(make_df):
    """Test running sum with negative bounds and min periods."""
    random.seed(444)

    data = []
    expected_data = []

    for category in ["X", "Y"]:
        values = [random.randint(1, 100) for _ in range(100)]

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            start_idx = max(0, ts - 10)
            end_idx = max(0, ts - 2)

            window_vals = values[start_idx:end_idx]
            min_periods = 3

            if len(window_vals) < min_periods:
                expected_sum = None
            else:
                expected_sum = sum(window_vals)

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "sliding_sum": expected_sum,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(-10, -3, min_periods=3)

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").sum().over(window_spec).alias("sliding_sum")
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_sliding_sum_positive_bounds(make_df):
    """Test running sum with positive bounds and min periods."""
    random.seed(444)

    data = []
    expected_data = []

    for category in ["X", "Y"]:
        values = [random.randint(1, 100) for _ in range(100)]

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            start_idx = min(len(values), ts + 2)
            end_idx = min(len(values), ts + 10)

            window_vals = values[start_idx:end_idx]
            min_periods = 3

            if len(window_vals) < min_periods:
                expected_sum = None
            else:
                expected_sum = sum(window_vals)

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "sliding_sum": expected_sum,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(2, 9, min_periods=3)

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").sum().over(window_spec).alias("sliding_sum")
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


def test_min_max_with_none(make_df):
    """Test min and max window functions with None values."""
    random.seed(42)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = []
        for _ in range(1000):
            if random.random() < 0.3:
                values.append(None)
            else:
                values.append(random.randint(1, 100))

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            start_idx = max(0, ts - 1)
            end_idx = min(ts + 1 + 1, len(values))
            window_vals = [v for v in values[start_idx:end_idx] if v is not None]

            window_min = min(window_vals) if window_vals else None
            window_max = max(window_vals) if window_vals else None

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "window_min": window_min,
                    "window_max": window_max,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(-1, 1)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").min().over(window_spec).alias("window_min"),
        col("value").max().over(window_spec).alias("window_max"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_count_modes(make_df):
    """Test all three count modes (all, valid, null)."""
    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [None if random.random() < 0.5 else f"val_{category}_{i}" for i in range(1000)]

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "text": value})

            start_idx = max(0, ts - 3)
            end_idx = min(ts + 1 + 3, len(values))
            window_vals = values[start_idx:end_idx]

            count_all = len(window_vals)
            count_valid = sum(1 for v in window_vals if v is not None)
            count_null = sum(1 for v in window_vals if v is None)

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "text": value,
                    "count_all": count_all,
                    "count_valid": count_valid,
                    "count_null": count_null,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(-3, 3)

    result = df.select(
        col("category"),
        col("ts"),
        col("text"),
        col("text").count("all").over(window_spec).alias("count_all"),
        col("text").count("valid").over(window_spec).alias("count_valid"),
        col("text").count("null").over(window_spec).alias("count_null"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_count_count_distinct_with_none(make_df):
    """Test count and count_distinct window functions with None values."""
    random.seed(51)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = []
        for _ in range(1000):
            if random.random() < 0.3:
                values.append(None)
            else:
                values.append(random.randint(1, 5))

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            start_idx = max(0, ts - 2)
            end_idx = min(ts + 2 + 1, len(values))
            window_vals = values[start_idx:end_idx]

            window_count = sum(1 for v in window_vals if v is not None)

            window_distinct = len(set(v for v in window_vals if v is not None))

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "window_count": window_count,
                    "window_distinct": window_distinct,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(-2, 2)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").count().over(window_spec).alias("window_count"),
        col("value").count_distinct().over(window_spec).alias("window_distinct"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_sum_avg_with_none(make_df):
    """Test sum and avg window functions with None values."""
    random.seed(52)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = []
        for _ in range(100):
            if random.random() < 0.5:
                values.append(None)
            else:
                values.append(random.randint(1, 100))

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            start_idx = max(0, ts - 2)
            end_idx = min(ts + 2 + 1, len(values))
            window_vals = [v for v in values[start_idx:end_idx] if v is not None]

            window_sum = sum(window_vals) if window_vals else None
            window_avg = sum(window_vals) / len(window_vals) if window_vals else None

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "window_sum": window_sum,
                    "window_avg": window_avg,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(-2, 2)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").sum().over(window_spec).alias("window_sum"),
        col("value").mean().over(window_spec).alias("window_avg"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_running_sum_default_window(make_df):
    """Test running sum over partitioned ordered windows without explicit window bounds."""
    random.seed(70)

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

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").sum().over(window_spec).alias("running_sum")
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_running_agg_default_window(make_df):
    """Test various running aggregations over partitioned ordered windows without explicit window bounds."""
    random.seed(71)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [random.randint(1, 100) for _ in range(10)]
        running_sum = 0
        running_min = float("inf")
        running_max = float("-inf")
        running_count = 0

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            running_sum += value
            running_min = min(running_min, value)
            running_max = max(running_max, value)
            running_count += 1

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "running_sum": running_sum,
                    "running_min": running_min,
                    "running_max": running_max,
                    "running_count": running_count,
                    "running_avg": running_sum / running_count,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").sum().over(window_spec).alias("running_sum"),
        col("value").min().over(window_spec).alias("running_min"),
        col("value").max().over(window_spec).alias("running_max"),
        col("value").count().over(window_spec).alias("running_count"),
        col("value").mean().over(window_spec).alias("running_avg"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_running_agg_default_window_desc(make_df):
    """Test running aggregations over partitioned windows with descending order without explicit window bounds."""
    random.seed(72)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        values = [random.randint(1, 100) for _ in range(10)]

        desc_values = values.copy()
        desc_values.reverse()
        running_sum = 0
        running_min = float("inf")
        running_max = float("-inf")
        running_count = 0
        desc_running_sums = []
        desc_running_mins = []
        desc_running_maxs = []
        desc_running_counts = []
        desc_running_avgs = []

        for val in desc_values:
            running_sum += val
            running_min = min(running_min, val)
            running_max = max(running_max, val)
            running_count += 1
            desc_running_sums.append(running_sum)
            desc_running_mins.append(running_min)
            desc_running_maxs.append(running_max)
            desc_running_counts.append(running_count)
            desc_running_avgs.append(running_sum / running_count)

        desc_running_sums.reverse()
        desc_running_mins.reverse()
        desc_running_maxs.reverse()
        desc_running_counts.reverse()
        desc_running_avgs.reverse()

        for ts, (value, run_sum, run_min, run_max, run_count, run_avg) in enumerate(
            zip(values, desc_running_sums, desc_running_mins, desc_running_maxs, desc_running_counts, desc_running_avgs)
        ):
            data.append({"category": category, "ts": ts, "value": value})
            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "running_sum": run_sum,
                    "running_min": run_min,
                    "running_max": run_max,
                    "running_count": run_count,
                    "running_avg": run_avg,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=True)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").sum().over(window_spec).alias("running_sum"),
        col("value").min().over(window_spec).alias("running_min"),
        col("value").max().over(window_spec).alias("running_max"),
        col("value").count().over(window_spec).alias("running_count"),
        col("value").mean().over(window_spec).alias("running_avg"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_running_agg_default_window_with_nulls(make_df):
    """Test running aggregations over partitioned windows with null values without explicit window bounds."""
    random.seed(73)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = []
        for _ in range(10):
            if random.random() < 0.3:
                values.append(None)
            else:
                values.append(random.randint(1, 100))

        running_sum = 0
        running_min = float("inf")
        running_max = float("-inf")
        running_count = 0
        running_valid_count = 0

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            if value is not None:
                running_sum += value
                running_min = min(running_min, value)
                running_max = max(running_max, value)
                running_valid_count += 1
            running_count += 1

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "running_sum": running_sum if running_valid_count > 0 else None,
                    "running_min": running_min if running_valid_count > 0 else None,
                    "running_max": running_max if running_valid_count > 0 else None,
                    "running_count": running_count,
                    "running_valid_count": running_valid_count,
                    "running_avg": running_sum / running_valid_count if running_valid_count > 0 else None,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").sum().over(window_spec).alias("running_sum"),
        col("value").min().over(window_spec).alias("running_min"),
        col("value").max().over(window_spec).alias("running_max"),
        col("value").count("all").over(window_spec).alias("running_count"),
        col("value").count("valid").over(window_spec).alias("running_valid_count"),
        col("value").mean().over(window_spec).alias("running_avg"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_running_agg_asc_desc_windows(make_df):
    """Test running aggregations with both ascending and descending windows in the same query."""
    random.seed(74)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        values = [random.randint(1, 100) for _ in range(10)]

        asc_running_sum = 0
        asc_running_min = float("inf")
        asc_running_max = float("-inf")
        asc_running_count = 0
        asc_running_sums = []
        asc_running_mins = []
        asc_running_maxs = []
        asc_running_counts = []
        asc_running_avgs = []

        for value in values:
            asc_running_sum += value
            asc_running_min = min(asc_running_min, value)
            asc_running_max = max(asc_running_max, value)
            asc_running_count += 1
            asc_running_sums.append(asc_running_sum)
            asc_running_mins.append(asc_running_min)
            asc_running_maxs.append(asc_running_max)
            asc_running_counts.append(asc_running_count)
            asc_running_avgs.append(asc_running_sum / asc_running_count)

        desc_values = values.copy()
        desc_values.reverse()
        desc_running_sum = 0
        desc_running_min = float("inf")
        desc_running_max = float("-inf")
        desc_running_count = 0
        desc_running_sums = []
        desc_running_mins = []
        desc_running_maxs = []
        desc_running_counts = []
        desc_running_avgs = []

        for value in desc_values:
            desc_running_sum += value
            desc_running_min = min(desc_running_min, value)
            desc_running_max = max(desc_running_max, value)
            desc_running_count += 1
            desc_running_sums.append(desc_running_sum)
            desc_running_mins.append(desc_running_min)
            desc_running_maxs.append(desc_running_max)
            desc_running_counts.append(desc_running_count)
            desc_running_avgs.append(desc_running_sum / desc_running_count)

        desc_running_sums.reverse()
        desc_running_mins.reverse()
        desc_running_maxs.reverse()
        desc_running_counts.reverse()
        desc_running_avgs.reverse()

        for ts, (
            value,
            asc_sum,
            asc_min,
            asc_max,
            asc_count,
            asc_avg,
            desc_sum,
            desc_min,
            desc_max,
            desc_count,
            desc_avg,
        ) in enumerate(
            zip(
                values,
                asc_running_sums,
                asc_running_mins,
                asc_running_maxs,
                asc_running_counts,
                asc_running_avgs,
                desc_running_sums,
                desc_running_mins,
                desc_running_maxs,
                desc_running_counts,
                desc_running_avgs,
            )
        ):
            data.append({"category": category, "ts": ts, "value": value})
            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "asc_sum": asc_sum,
                    "asc_min": asc_min,
                    "asc_max": asc_max,
                    "asc_count": asc_count,
                    "asc_avg": asc_avg,
                    "desc_sum": desc_sum,
                    "desc_min": desc_min,
                    "desc_max": desc_max,
                    "desc_count": desc_count,
                    "desc_avg": desc_avg,
                }
            )

    df = make_df(data)

    asc_window = Window().partition_by("category").order_by("ts", desc=False)
    desc_window = Window().partition_by("category").order_by("ts", desc=True)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").sum().over(asc_window).alias("asc_sum"),
        col("value").min().over(asc_window).alias("asc_min"),
        col("value").max().over(asc_window).alias("asc_max"),
        col("value").count().over(asc_window).alias("asc_count"),
        col("value").mean().over(asc_window).alias("asc_avg"),
        col("value").sum().over(desc_window).alias("desc_sum"),
        col("value").min().over(desc_window).alias("desc_min"),
        col("value").max().over(desc_window).alias("desc_max"),
        col("value").count().over(desc_window).alias("desc_count"),
        col("value").mean().over(desc_window).alias("desc_avg"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_sliding_sum_with_nan_and_none(make_df):
    """Test sliding sum over partitioned ordered windows with NaN and None values."""
    random.seed(80)
    np.random.seed(80)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = []
        for _ in range(100):
            r = random.random()
            if r < 0.2:
                values.append(None)
            elif r < 0.4:
                values.append(float("nan"))
            else:
                values.append(random.randint(1, 100))

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            start_idx = max(0, ts - 2)
            window_vals = values[start_idx : ts + 1]

            if all(v is None for v in window_vals):
                sliding_sum = None
            else:
                sliding_sum = sum(v for v in window_vals if v is not None)

            expected_data.append({"category": category, "ts": ts, "value": value, "sliding_sum": sliding_sum})

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False).rows_between(-2, 0)

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").sum().over(window_spec).alias("sliding_sum")
    ).collect()

    result_pd = result.to_pandas()
    expected_pd = pd.DataFrame(expected_data)

    assert_df_equals(result_pd, expected_pd, sort_key=["category", "ts"], check_dtype=False)
