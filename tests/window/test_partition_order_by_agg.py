from __future__ import annotations

import random

import pandas as pd

import daft
from daft import Window, col, lit
from tests.conftest import assert_df_equals


def test_partition_order_by_literal():
    """Test window functions with ORDER BY literal."""
    df = daft.from_pydict({"a": [1, 1, 1, 2], "b": [3, 3, 4, 4]})

    res = df.with_column("c", col("a").count(mode="all").over(Window().partition_by("a", "b").order_by(lit(1))))

    assert res.sort(["a", "b"]).to_pydict() == {"a": [1, 1, 1, 2], "b": [3, 3, 4, 4], "c": [2, 2, 1, 1]}


def test_partition_order_by_agg(make_df):
    """Test partition by + order by + agg functions."""
    random.seed(70)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        values = [random.randint(1, 100) for _ in range(10)]
        total = sum(values)

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            expected_data.append({"category": category, "ts": ts, "value": value, "part_sum": total})

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").sum().over(window_spec).alias("part_sum")
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_partition_order_by_agg_default_window(make_df):
    """Test partition by + order by + agg functions without explicit window bounds."""
    random.seed(71)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [random.randint(1, 100) for _ in range(10)]
        part_sum = sum(values)
        part_min = min(values)
        part_max = max(values)
        part_count = len(values)
        part_avg = sum(values) / len(values)

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "part_sum": part_sum,
                    "part_min": part_min,
                    "part_max": part_max,
                    "part_count": part_count,
                    "part_avg": part_avg,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").sum().over(window_spec).alias("part_sum"),
        col("value").min().over(window_spec).alias("part_min"),
        col("value").max().over(window_spec).alias("part_max"),
        col("value").count().over(window_spec).alias("part_count"),
        col("value").mean().over(window_spec).alias("part_avg"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_partition_order_by_agg_desc(make_df):
    """Test partition by + order by + agg functions with descending order."""
    random.seed(72)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        values = [random.randint(1, 100) for _ in range(10)]

        desc_values = values.copy()
        desc_values.reverse()
        part_sum = sum(values)
        part_min = min(values)
        part_max = max(values)
        part_count = len(values)
        part_avg = sum(values) / len(values)

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})
            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "part_sum": part_sum,
                    "part_min": part_min,
                    "part_max": part_max,
                    "part_count": part_count,
                    "part_avg": part_avg,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=True)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").sum().over(window_spec).alias("part_sum"),
        col("value").min().over(window_spec).alias("part_min"),
        col("value").max().over(window_spec).alias("part_max"),
        col("value").count().over(window_spec).alias("part_count"),
        col("value").mean().over(window_spec).alias("part_avg"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_partition_order_by_agg_with_nulls(make_df):
    """Test partition by + order by + agg functions with null values."""
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

        non_null_values = [v for v in values if v is not None]
        part_sum = sum(non_null_values) if len(non_null_values) > 0 else None
        part_min = min(non_null_values) if len(non_null_values) > 0 else None
        part_max = max(non_null_values) if len(non_null_values) > 0 else None
        part_count = len(values)
        part_valid_count = len(non_null_values)
        part_avg = sum(non_null_values) / len(non_null_values) if len(non_null_values) > 0 else None

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "part_sum": part_sum,
                    "part_min": part_min,
                    "part_max": part_max,
                    "part_count": part_count,
                    "part_valid_count": part_valid_count,
                    "part_avg": part_avg,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").sum().over(window_spec).alias("part_sum"),
        col("value").min().over(window_spec).alias("part_min"),
        col("value").max().over(window_spec).alias("part_max"),
        col("value").count("all").over(window_spec).alias("part_count"),
        col("value").count("valid").over(window_spec).alias("part_valid_count"),
        col("value").mean().over(window_spec).alias("part_avg"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)
