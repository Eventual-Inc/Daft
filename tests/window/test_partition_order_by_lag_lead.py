from __future__ import annotations

import random

import pandas as pd
import pytest

from daft import Window, col
from daft.context import get_context
from daft.functions import dense_rank, rank
from tests.conftest import assert_df_equals, get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray"
    and get_context().daft_execution_config.use_experimental_distributed_engine is False,
    reason="requires Native Runner or Flotilla to be in use",
)


def test_basic_lag_function(make_df):
    """Test basic lag function with default offset."""
    random.seed(42)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        values = random.sample(range(10, 100), 10)
        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})
            expected_data.append(
                {"category": category, "ts": ts, "value": value, "lagged_value": None if ts == 0 else values[ts - 1]}
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").lag(1).over(window_spec).alias("lagged_value")
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_basic_lead_function(make_df):
    """Test basic lead function with default offset."""
    random.seed(43)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        values = random.sample(range(10, 100), 10)
        for ts, value in enumerate(values):
            data.append({"category": category, "ts": ts, "value": value})
            expected_data.append(
                {"category": category, "ts": ts, "value": value, "lead_value": None if ts == 9 else values[ts + 1]}
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"), col("ts"), col("value"), col("value").lead(1).over(window_spec).alias("lead_value")
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


@pytest.mark.parametrize(
    "desc,nulls_first,expected_lead",
    [
        (False, False, [2, 3, 4, None, 6, 7, 8, None, 10, 11, 12, None]),
        (False, True, [2, 3, None, 1, 6, 7, None, 5, 10, 11, None, 9]),
        (True, False, [4, 1, 2, None, 8, 5, 6, None, 12, 9, 10, None]),
        (True, True, [None, 1, 2, 3, None, 5, 6, 7, None, 9, 10, 11]),
    ],
)
def test_lead_nulls_first_or_last(make_df, desc, nulls_first, expected_lead):
    """Test lag and lead functions with nulls first or last."""
    data = [
        {"id": 1, "category": "A", "ts": 1},
        {"id": 2, "category": "A", "ts": 2},
        {"id": 3, "category": "A", "ts": 3},
        {"id": 4, "category": "A", "ts": None},
        {"id": 5, "category": "B", "ts": 1},
        {"id": 6, "category": "B", "ts": 2},
        {"id": 7, "category": "B", "ts": 3},
        {"id": 8, "category": "B", "ts": None},
        {"id": 9, "category": "C", "ts": 1},
        {"id": 10, "category": "C", "ts": 2},
        {"id": 11, "category": "C", "ts": 3},
        {"id": 12, "category": "C", "ts": None},
    ]

    df = make_df(data)
    window_spec = Window().partition_by("category").order_by("ts", desc=desc, nulls_first=nulls_first)
    result = df.select(
        col("id"), col("category"), col("ts"), col("id").lead(1).over(window_spec).alias("lead_value")
    ).collect()

    expected = {
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        "category": ["A", "A", "A", "A", "B", "B", "B", "B", "C", "C", "C", "C"],
        "ts": [1, 2, 3, None, 1, 2, 3, None, 1, 2, 3, None],
        "lead_value": expected_lead,
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=["id"], check_dtype=False)


def test_lag_lead_with_different_offsets(make_df):
    """Test lag and lead functions with different offset values."""
    random.seed(44)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = random.sample(range(10, 100), 10)
        for ts in range(7):
            value = values[ts]
            data.append({"category": category, "ts": ts, "value": value})

            expected_row = {
                "category": category,
                "ts": ts,
                "value": value,
                "lag_1": None if ts < 1 else values[ts - 1],
                "lag_2": None if ts < 2 else values[ts - 2],
                "lag_3": None if ts < 3 else values[ts - 3],
                "lead_1": None if ts >= 6 else values[ts + 1],
                "lead_2": None if ts >= 5 else values[ts + 2],
                "lead_3": None if ts >= 4 else values[ts + 3],
            }

            expected_data.append(expected_row)

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").lag(1).over(window_spec).alias("lag_1"),
        col("value").lag(2).over(window_spec).alias("lag_2"),
        col("value").lag(3).over(window_spec).alias("lag_3"),
        col("value").lead(1).over(window_spec).alias("lead_1"),
        col("value").lead(2).over(window_spec).alias("lead_2"),
        col("value").lead(3).over(window_spec).alias("lead_3"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_lag_lead_with_col_default(make_df):
    """Test lag and lead functions with column default values."""
    random.seed(45)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        defaults = random.sample(range(10, 100), 50)
        values = random.sample(range(10, 100), 50)
        for ts in range(50):
            value = values[ts]
            data.append({"category": category, "ts": ts, "default": defaults[ts], "value": value})

            expected_row = {
                "category": category,
                "ts": ts,
                "default": defaults[ts],
                "value": value,
                "lag_10": 2 * defaults[ts] if ts < 10 else values[ts - 10],
                "lead_10": 2 * defaults[ts] if ts >= 40 else values[ts + 10],
                "lag_50": 2 * defaults[ts],
                "lead_50": 2 * defaults[ts],
                "lag_15_default_99": 99 if ts < 15 else values[ts - 15],
                "lead_15_default_99": 99 if ts >= 35 else values[ts + 15],
            }
            expected_data.append(expected_row)

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("ts"),
        col("default"),
        col("value"),
        col("value").lag(10, default=2 * col("default")).over(window_spec).alias("lag_10"),
        col("value").lead(10, default=2 * col("default")).over(window_spec).alias("lead_10"),
        col("value").lag(50, default=2 * col("default")).over(window_spec).alias("lag_50"),
        col("value").lead(50, default=2 * col("default")).over(window_spec).alias("lead_50"),
        col("value").lag(15, default=99).over(window_spec).alias("lag_15_default_99"),
        col("value").lead(15, default=99).over(window_spec).alias("lead_15_default_99"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_lag_lead_with_multiple_partitions(make_df):
    """Test lag and lead functions with multiple partition columns."""
    random.seed(45)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        for subcategory in [1, 2, 3]:
            values = random.sample(range(10, 100), 4)
            for ts in range(4):
                value = values[ts]
                data.append({"category": category, "subcategory": subcategory, "ts": ts, "value": value})

                lag_value = None if ts == 0 else values[ts - 1]
                lead_value = None if ts == 3 else values[ts + 1]

                expected_data.append(
                    {
                        "category": category,
                        "subcategory": subcategory,
                        "ts": ts,
                        "value": value,
                        "lag_value": lag_value,
                        "lead_value": lead_value,
                    }
                )

    df = make_df(data)

    combined_window = Window().partition_by(["category", "subcategory"]).order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("subcategory"),
        col("ts"),
        col("value"),
        col("value").lag(1).over(combined_window).alias("lag_value"),
        col("value").lead(1).over(combined_window).alias("lead_value"),
    ).collect()

    assert_df_equals(
        result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "subcategory", "ts"], check_dtype=False
    )


def test_lag_lead_for_delta_calculation(make_df):
    """Test using lag and lead functions to calculate deltas (differences between values)."""
    random.seed(46)

    data = []
    expected_data = []

    for category in ["A", "B"]:
        values = random.sample(range(10, 100), 5)
        for ts in range(5):
            value = values[ts]
            data.append({"category": category, "ts": ts, "value": value})

            delta_from_prev = None if ts == 0 else value - values[ts - 1]
            delta_to_next = None if ts == 4 else values[ts + 1] - value
            delta_from_2_prev = None if ts < 2 else value - values[ts - 2]

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "delta_from_prev": delta_from_prev,
                    "delta_to_next": delta_to_next,
                    "delta_from_2_prev": delta_from_2_prev,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        (col("value") - col("value").lag(1).over(window_spec)).alias("delta_from_prev"),
        (col("value").lead(1).over(window_spec) - col("value")).alias("delta_to_next"),
        (col("value") - col("value").lag(2).over(window_spec)).alias("delta_from_2_prev"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_lag_lead_with_zero_offset(make_df):
    """Test lag and lead functions with zero offset (should return current row)."""
    random.seed(47)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        values = random.sample(range(10, 100), 3)
        for ts in range(3):
            value = values[ts]
            data.append({"category": category, "ts": ts, "value": value})

            expected_data.append({"category": category, "ts": ts, "value": value, "lag_0": value, "lead_0": value})

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").lag(0).over(window_spec).alias("lag_0"),
        col("value").lead(0).over(window_spec).alias("lead_0"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_lag_lead_with_large_offset(make_df):
    """Test lag and lead functions with offset larger than partition size."""
    random.seed(48)

    data = []
    expected_data = []

    for category in ["A", "B", "C"]:
        partition_size = 4
        values = random.sample(range(10, 100), partition_size)

        for ts in range(partition_size):
            value = values[ts]
            data.append({"category": category, "ts": ts, "value": value})

            large_offset = 10
            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    f"lag_{large_offset}": None,
                    f"lead_{large_offset}": None,
                }
            )

    df = make_df(data)

    large_offset = 10
    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").lag(large_offset).over(window_spec).alias(f"lag_{large_offset}"),
        col("value").lead(large_offset).over(window_spec).alias(f"lead_{large_offset}"),
    ).collect()

    result = result.to_pandas().map(lambda x: None if pd.isna(x) else x)

    assert_df_equals(result, pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_lag_lead_with_default_values(make_df):
    """Test lag and lead functions with default values for NULL results."""
    random.seed(49)

    data = []
    expected_data = []
    default_value = -999

    for category in ["A", "B"]:
        values = random.sample(range(10, 100), 5)
        for ts in range(5):
            value = values[ts]
            data.append({"category": category, "ts": ts, "value": value})

            lag_with_default = default_value if ts == 0 else values[ts - 1]
            lead_with_default = default_value if ts == 4 else values[ts + 1]

            lag_large_with_default = default_value
            lead_large_with_default = default_value

            expected_data.append(
                {
                    "category": category,
                    "ts": ts,
                    "value": value,
                    "lag_with_default": lag_with_default,
                    "lead_with_default": lead_with_default,
                    "lag_large_with_default": lag_large_with_default,
                    "lead_large_with_default": lead_large_with_default,
                }
            )

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("ts"),
        col("value"),
        col("value").lag(1, default=default_value).over(window_spec).alias("lag_with_default"),
        col("value").lead(1, default=default_value).over(window_spec).alias("lead_with_default"),
        col("value").lag(10, default=default_value).over(window_spec).alias("lag_large_with_default"),
        col("value").lead(10, default=default_value).over(window_spec).alias("lead_large_with_default"),
    ).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected_data), sort_key=["category", "ts"], check_dtype=False)


def test_all_partition_order_by(make_df):
    """Test combining multiple window functions across different partition schemes."""
    random.seed(42)

    data = []

    for category in ["A", "B", "C"]:
        for subcategory in [1, 2, 3]:
            for ts in range(5):
                value = random.randint(10, 100)
                data.append({"category": category, "subcategory": subcategory, "ts": ts, "value": value})

    df = make_df(data)

    category_value_window = Window().partition_by("category").order_by("value", desc=False)
    subcategory_value_window = Window().partition_by("subcategory").order_by("value", desc=False)
    combined_value_window = Window().partition_by(["category", "subcategory"]).order_by("value", desc=False)
    combined_window = Window().partition_by(["category", "subcategory"]).order_by("ts", desc=False)

    result = df.select(
        col("category"),
        col("subcategory"),
        col("ts"),
        col("value"),
        rank().over(category_value_window).alias("category_rank"),
        dense_rank().over(category_value_window).alias("category_dense_rank"),
        rank().over(subcategory_value_window).alias("subcategory_rank"),
        dense_rank().over(subcategory_value_window).alias("subcategory_dense_rank"),
        rank().over(combined_value_window).alias("combined_rank"),
        dense_rank().over(combined_value_window).alias("combined_dense_rank"),
        col("value").lag(1).over(combined_window).alias("combined_lag"),
        col("value").lead(1).over(combined_window).alias("combined_lead"),
    ).collect()

    result_dict = result.to_pydict()

    for category in ["A", "B", "C"]:
        category_indices = [i for i, cat in enumerate(result_dict["category"]) if cat == category]
        category_values = [result_dict["value"][i] for i in category_indices]

        sorted_values = sorted(set(category_values))
        value_to_dense_rank = {value: i + 1 for i, value in enumerate(sorted_values)}

        value_counts = {}
        for value in category_values:
            value_counts[value] = value_counts.get(value, 0) + 1

        value_to_rank = {}
        current_rank = 1
        for value in sorted_values:
            value_to_rank[value] = current_rank
            current_rank += value_counts[value]

        for idx in category_indices:
            value = result_dict["value"][idx]
            expected_rank = value_to_rank[value]
            expected_dense_rank = value_to_dense_rank[value]

            assert (
                result_dict["category_rank"][idx] == expected_rank
            ), f"Incorrect rank for value {value} in category {category}"
            assert (
                result_dict["category_dense_rank"][idx] == expected_dense_rank
            ), f"Incorrect dense_rank for value {value} in category {category}"

    for subcategory in [1, 2, 3]:
        subcategory_indices = [i for i, sc in enumerate(result_dict["subcategory"]) if sc == subcategory]
        subcategory_values = [result_dict["value"][i] for i in subcategory_indices]

        sorted_values = sorted(set(subcategory_values))
        value_to_dense_rank = {value: i + 1 for i, value in enumerate(sorted_values)}

        value_counts = {}
        for value in subcategory_values:
            value_counts[value] = value_counts.get(value, 0) + 1

        value_to_rank = {}
        current_rank = 1
        for value in sorted_values:
            value_to_rank[value] = current_rank
            current_rank += value_counts[value]

        for idx in subcategory_indices:
            value = result_dict["value"][idx]
            expected_rank = value_to_rank[value]
            expected_dense_rank = value_to_dense_rank[value]

            assert (
                result_dict["subcategory_rank"][idx] == expected_rank
            ), f"Incorrect rank for value {value} in subcategory {subcategory}"
            assert (
                result_dict["subcategory_dense_rank"][idx] == expected_dense_rank
            ), f"Incorrect dense_rank for value {value} in subcategory {subcategory}"

    for category in ["A", "B", "C"]:
        for subcategory in [1, 2, 3]:
            combined_indices = [
                i
                for i, (cat, sc) in enumerate(zip(result_dict["category"], result_dict["subcategory"]))
                if cat == category and sc == subcategory
            ]
            combined_values = [result_dict["value"][i] for i in combined_indices]

            sorted_values = sorted(set(combined_values))
            value_to_dense_rank = {value: i + 1 for i, value in enumerate(sorted_values)}

            value_counts = {}
            for value in combined_values:
                value_counts[value] = value_counts.get(value, 0) + 1

            value_to_rank = {}
            current_rank = 1
            for value in sorted_values:
                value_to_rank[value] = current_rank
                current_rank += value_counts[value]

            for idx in combined_indices:
                value = result_dict["value"][idx]
                expected_rank = value_to_rank[value]
                expected_dense_rank = value_to_dense_rank[value]

                assert (
                    result_dict["combined_rank"][idx] == expected_rank
                ), f"Incorrect rank for value {value} in {category}-{subcategory}"
                assert (
                    result_dict["combined_dense_rank"][idx] == expected_dense_rank
                ), f"Incorrect dense_rank for value {value} in {category}-{subcategory}"

    for category in ["A", "B", "C"]:
        for subcategory in [1, 2, 3]:
            combined_indices = [
                i
                for i, (cat, sc) in enumerate(zip(result_dict["category"], result_dict["subcategory"]))
                if cat == category and sc == subcategory
            ]

            for ts in range(5):
                current_ts_indices = [i for i in combined_indices if result_dict["ts"][i] == ts]

                if not current_ts_indices:
                    continue

                idx = current_ts_indices[0]

                if ts == 0:
                    assert (
                        result_dict["combined_lag"][idx] is None
                    ), f"First ts in {category}-{subcategory} should have None lag"
                else:
                    prev_ts_indices = [i for i in combined_indices if result_dict["ts"][i] == ts - 1]
                    if prev_ts_indices:
                        prev_idx = prev_ts_indices[0]
                        prev_value = result_dict["value"][prev_idx]
                        assert (
                            result_dict["combined_lag"][idx] == prev_value
                        ), f"Incorrect lag for {category}-{subcategory}, ts {ts}"

                if ts == 4:
                    assert (
                        result_dict["combined_lead"][idx] is None
                    ), f"Last ts in {category}-{subcategory} should have None lead"
                else:
                    next_ts_indices = [i for i in combined_indices if result_dict["ts"][i] == ts + 1]
                    if next_ts_indices:
                        next_idx = next_ts_indices[0]
                        next_value = result_dict["value"][next_idx]
                        assert (
                            result_dict["combined_lead"][idx] == next_value
                        ), f"Incorrect lead for {category}-{subcategory}, ts {ts}"
