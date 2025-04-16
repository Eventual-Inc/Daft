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
    """Test running sum over partitioned ordered windows.

    Creates randomized data for multiple categories, orders by timestamp,
    and verifies that the running sum correctly accumulates values within
    each partition.
    """
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


@pytest.mark.skip("TODO")
def test_running_sum_with_nulls(make_df):
    """Test running sum with null values in the data.

    Verifies that the running sum correctly handles null values by skipping them
    in the aggregation.
    """
    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [10, None, 20, 30, None, 40, 50]
        running_sum = 0

        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})

            if value is not None:
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


@pytest.mark.skip("TODO")
def test_running_sum_with_multi_partition(make_df):
    """Test running sum with multiple partition columns.

    Verifies that running sum works correctly when partitioning by multiple
    columns.
    """
    random.seed(43)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        for group in [1, 2, 3]:
            values = [random.randint(1, 50) for _ in range(5)]
            running_sum = 0

            for ts, value in enumerate(values):
                data.append({"category": category, "group": group, "ts": ts, "value": value})

                running_sum += value
                expected_data.append(
                    {"category": category, "group": group, "ts": ts, "value": value, "running_sum": running_sum}
                )

    df = make_df(data)

    window_spec = (
        Window()
        .partition_by(["category", "group"])
        .order_by("ts", desc=False)
        .rows_between(Window.unbounded_preceding, Window.current_row)
    )

    result = df.select(
        col("category"),
        col("group"),
        col("ts"),
        col("value"),
        col("value").sum().over(window_spec).alias("running_sum"),
    ).collect()

    assert_df_equals(
        result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "group", "ts"], check_dtype=False
    )


@pytest.mark.skip("TODO")
def test_running_sum_desc_order(make_df):
    """Test running sum with descending order.

    Verifies that running sum works correctly when ordering in descending order.
    """
    random.seed(44)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = [random.randint(1, 100) for _ in range(8)]

        # Generate expected data with descending running sum
        desc_running_sums = []
        running_sum = 0
        for value in reversed(values):
            running_sum += value
            desc_running_sums.insert(0, running_sum)

        for ts, (value, running_sum) in enumerate(zip(values, desc_running_sums)):
            data.append({"category": category, "ts": ts, "value": value})
            expected_data.append({"category": category, "ts": ts, "value": value, "running_sum": running_sum})

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
