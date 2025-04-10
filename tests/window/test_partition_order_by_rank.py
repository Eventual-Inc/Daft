from __future__ import annotations

import pandas as pd
import pytest

from daft import Window, col
from tests.conftest import assert_df_equals, get_tests_daft_runner_name


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_rank_function(make_df):
    df = make_df(
        {"category": ["A", "A", "A", "B", "B", "B", "C", "C"], "sales": [100, 200, 50, 500, 100, 300, 250, 150]}
    )

    window_spec = Window().partition_by("category").order_by("sales", ascending=False)

    result = df.select(
        col("category"), col("sales"), col("sales").rank().over(window_spec).alias("rank_sales")
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 100, 300, 250, 150],
        "rank_sales": [2, 1, 3, 1, 3, 2, 1, 2],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()), check_dtype=False)


@pytest.mark.skip(reason="Dense rank function not yet implemented")
@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_dense_rank_function(make_df):
    df = make_df(
        {
            "category": ["A", "A", "A", "B", "B", "B", "B", "C", "C"],
            "sales": [100, 200, 50, 500, 100, 100, 300, 250, 150],
        }
    )

    window_spec = Window().partition_by("category").order_by("sales", ascending=False)

    result = df.select(
        col("category"), col("sales"), col("sales").dense_rank().over(window_spec).alias("dense_rank_sales")
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 100, 100, 300, 250, 150],
        "dense_rank_sales": [2, 1, 3, 1, 3, 3, 2, 1, 2],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()), check_dtype=False)


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_multiple_window_partitions(make_df):
    """Test multiple window functions with different partition keys using random numbers.

    Creates a dataset with 18 rows (2 rows each for A1, A2, A3, B1, B2, B3, C1, C2, C3)
    and verifies sums across different partition keys (A/B/C and 1/2/3).
    """
    import random

    random.seed(42)

    data = []
    for letter in ["A", "B", "C"]:
        for num in ["1", "2", "3"]:
            for _ in range(3):
                data.append({"letter": letter, "num": num, "value": random.randint(1, 100)})

    df = make_df(data)

    letter_window = Window().partition_by("letter").order_by("value")
    num_window = Window().partition_by("num").order_by("value")
    combined_window = Window().partition_by(["letter", "num"]).order_by("value")

    result = df.select(
        col("letter"),
        col("num"),
        col("value"),
        col("value").sum().over(letter_window).alias("letter_sum"),
        col("value").sum().over(num_window).alias("num_sum"),
        col("value").sum().over(combined_window).alias("combined_sum"),
    ).collect()

    result_dict = result.to_pydict()

    for letter in ["A", "B", "C"]:
        letter_indices = [i for i, ltr in enumerate(result_dict["letter"]) if ltr == letter]
        letter_values = [result_dict["value"][i] for i in letter_indices]
        expected_letter_sum = sum(letter_values)
        actual_letter_sums = [result_dict["letter_sum"][i] for i in letter_indices]
        assert all(
            sum == expected_letter_sum for sum in actual_letter_sums
        ), f"Incorrect sum for letter {letter}: {actual_letter_sums} != {expected_letter_sum}"

    for num in ["1", "2", "3"]:
        num_indices = [i for i, n in enumerate(result_dict["num"]) if n == num]
        num_values = [result_dict["value"][i] for i in num_indices]
        expected_num_sum = sum(num_values)
        actual_num_sums = [result_dict["num_sum"][i] for i in num_indices]
        assert all(
            sum == expected_num_sum for sum in actual_num_sums
        ), f"Incorrect sum for number {num}: {actual_num_sums} != {expected_num_sum}"

    for letter in ["A", "B", "C"]:
        for num in ["1", "2", "3"]:
            combined_indices = [
                i
                for i, (ltr, n) in enumerate(zip(result_dict["letter"], result_dict["num"]))
                if ltr == letter and n == num
            ]
            combined_values = [result_dict["value"][i] for i in combined_indices]
            expected_combined_sum = sum(combined_values)
            actual_combined_sums = [result_dict["combined_sum"][i] for i in combined_indices]
            assert all(
                sum == expected_combined_sum for sum in actual_combined_sums
            ), f"Incorrect sum for combination {letter}{num}: {actual_combined_sums} != {expected_combined_sum}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_multi_window_agg_functions(make_df):
    """Test multiple window aggregation functions with different partition keys.

    Tests window functions with two different partition specifications:
    1. Partitioning by both category and group
    2. Partitioning by just category

    Using sum(), mean(), min(), and max() aggregations.
    """
    data = [
        {"category": "A", "group": 1, "value": 10},
        {"category": "A", "group": 1, "value": 20},
        {"category": "A", "group": 2, "value": 30},
        {"category": "A", "group": 2, "value": 40},
        {"category": "B", "group": 1, "value": 50},
        {"category": "B", "group": 1, "value": 60},
        {"category": "B", "group": 2, "value": 70},
        {"category": "B", "group": 2, "value": 80},
    ]

    df = make_df(data)

    multi_partition_window = Window().partition_by(["category", "group"]).order_by("value")
    single_partition_window = Window().partition_by("category").order_by("value")

    result = df.select(
        col("category"),
        col("group"),
        col("value"),
        col("value").sum().over(multi_partition_window).alias("sum_multi"),
        col("value").mean().over(multi_partition_window).alias("avg_multi"),
        col("value").min().over(single_partition_window).alias("min_single"),
        col("value").max().over(single_partition_window).alias("max_single"),
    ).collect()

    result_dict = result.to_pydict()

    for category in ["A", "B"]:
        for group in [1, 2]:
            indices = [
                i
                for i, (cat, grp) in enumerate(zip(result_dict["category"], result_dict["group"]))
                if cat == category and grp == group
            ]

            values = [result_dict["value"][i] for i in indices]

            expected_sum = sum(values)
            expected_avg = sum(values) / len(values)

            for idx in indices:
                assert (
                    result_dict["sum_multi"][idx] == expected_sum
                ), f"Incorrect sum for {category}/{group}: {result_dict['sum_multi'][idx]} != {expected_sum}"
                assert (
                    abs(result_dict["avg_multi"][idx] - expected_avg) < 1e-10
                ), f"Incorrect avg for {category}/{group}: {result_dict['avg_multi'][idx]} != {expected_avg}"

    for category in ["A", "B"]:
        indices = [i for i, cat in enumerate(result_dict["category"]) if cat == category]

        values = [result_dict["value"][i] for i in indices]

        expected_min = min(values)
        expected_max = max(values)

        for idx in indices:
            assert (
                result_dict["min_single"][idx] == expected_min
            ), f"Incorrect min for {category}: {result_dict['min_single'][idx]} != {expected_min}"
            assert (
                result_dict["max_single"][idx] == expected_max
            ), f"Incorrect max for {category}: {result_dict['max_single'][idx]} != {expected_max}"
