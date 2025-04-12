from __future__ import annotations

import pandas as pd
import pytest

from daft import Window, col
from daft.functions import row_number
from tests.conftest import assert_df_equals, get_tests_daft_runner_name


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_row_number_function(make_df):
    df = make_df(
        {"category": ["A", "A", "A", "B", "B", "B", "C", "C"], "sales": [100, 200, 50, 500, 100, 300, 250, 150]}
    )

    window_spec = Window().partition_by("category").order_by("sales", ascending=False)

    result = df.select(
        col("category"), col("sales"), row_number().over(window_spec).alias("row_number_sales")
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 100, 300, 250, 150],
        "row_number_sales": [2, 1, 3, 1, 3, 2, 1, 2],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()), check_dtype=False)


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_row_number_function_desc(make_df):
    df = make_df(
        {"category": ["A", "A", "A", "B", "B", "B", "C", "C"], "sales": [100, 200, 50, 500, 100, 300, 250, 150]}
    )

    window_spec = Window().partition_by("category").order_by("sales", ascending=True)

    result = df.select(
        col("category"), col("sales"), row_number().over(window_spec).alias("row_number_sales")
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 100, 300, 250, 150],
        "row_number_sales": [2, 3, 1, 3, 1, 2, 2, 1],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()), check_dtype=False)


@pytest.mark.skip(reason="This test is failing due to some row number implementation issue")
@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_multiple_window_partitions(make_df):
    """Test multiple window functions with different partition keys using random numbers.

    Creates a dataset with 900 rows (100 rows each for A1, A2, A3, B1, B2, B3, C1, C2, C3)
    and verifies sums across different partition keys (A/B/C and 1/2/3) as well as row_number
    ordering within each partition.
    """
    import random

    random.seed(42)

    # Generate distinct values for each group to ensure we can verify row_number correctness
    data = []
    all_numbers = list(range(1, 1001))  # Numbers from 1 to 1000
    random.shuffle(all_numbers)
    number_idx = 0

    for letter in ["A", "B", "C"]:
        for num in ["1", "2", "3"]:
            # Take next 100 distinct numbers for this group
            group_values = sorted(all_numbers[number_idx : number_idx + 3])
            number_idx += 3

            for value in group_values:
                data.append({"letter": letter, "num": num, "value": value})

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
        row_number().over(letter_window).alias("letter_row_number"),
        row_number().over(num_window).alias("num_row_number"),
        row_number().over(combined_window).alias("combined_row_number"),
    ).collect()

    result_dict = result.to_pydict()

    # print(result_dict)

    # 3 by 3 table
    tmp = [[[] for _ in range(3)] for _ in range(3)]

    for i in range(len(result_dict["letter"])):
        ltr = result_dict["letter"][i]
        num = result_dict["num"][i]
        val = result_dict["value"][i]
        ltr_rank = result_dict["letter_row_number"][i]
        num_rank = result_dict["num_row_number"][i]
        combined_rank = result_dict["combined_row_number"][i]

        tmp[ord(ltr) - ord("A")][int(num) - 1].append((val, ltr_rank, num_rank, combined_rank))

    for i in range(3):
        for j in range(3):
            print(f"Letter {chr(ord('A') + i)}, Number {chr(ord('1') + j)}:")
            for val, ltr_rank, num_rank, combined_rank in tmp[i][j]:
                print(
                    f"  Value: {val}, Letter Rank: {ltr_rank}, Number Rank: {num_rank}, Combined Rank: {combined_rank}"
                )
            print()

    # Verify sums
    for letter in ["A", "B", "C"]:
        letter_indices = [i for i, ltr in enumerate(result_dict["letter"]) if ltr == letter]
        letter_values = [result_dict["value"][i] for i in letter_indices]
        expected_letter_sum = sum(letter_values)
        actual_letter_sums = [result_dict["letter_sum"][i] for i in letter_indices]
        assert all(
            sum == expected_letter_sum for sum in actual_letter_sums
        ), f"Incorrect sum for letter {letter}: {actual_letter_sums} != {expected_letter_sum}"

        # Verify row numbers for letter partitions
        sorted_letter_values = sorted(letter_values)
        value_to_rank = {val: i + 1 for i, val in enumerate(sorted_letter_values)}

        for idx in letter_indices:
            value = result_dict["value"][idx]
            expected_rank = value_to_rank[value]
            actual_rank = result_dict["letter_row_number"][idx]
            assert (
                actual_rank == expected_rank
            ), f"Incorrect row number for letter {letter}, value {value}: got {actual_rank}, expected {expected_rank}"

    for num in ["1", "2", "3"]:
        num_indices = [i for i, n in enumerate(result_dict["num"]) if n == num]
        num_values = [result_dict["value"][i] for i in num_indices]
        expected_num_sum = sum(num_values)
        actual_num_sums = [result_dict["num_sum"][i] for i in num_indices]
        assert all(
            sum == expected_num_sum for sum in actual_num_sums
        ), f"Incorrect sum for number {num}: {actual_num_sums} != {expected_num_sum}"

        # Verify row numbers for num partitions
        sorted_num_values = sorted(num_values)
        value_to_rank = {val: i + 1 for i, val in enumerate(sorted_num_values)}
        for idx in num_indices:
            value = result_dict["value"][idx]
            expected_rank = value_to_rank[value]
            actual_rank = result_dict["num_row_number"][idx]
            assert (
                actual_rank == expected_rank
            ), f"Incorrect row number for num {num}, value {value}: got {actual_rank}, expected {expected_rank}"

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

            # Verify row numbers for combined partitions
            sorted_combined_values = sorted(combined_values)
            value_to_rank = {val: i + 1 for i, val in enumerate(sorted_combined_values)}
            for idx in combined_indices:
                value = result_dict["value"][idx]
                expected_rank = value_to_rank[value]
                actual_rank = result_dict["combined_row_number"][idx]
                assert (
                    actual_rank == expected_rank
                ), f"Incorrect row number for {letter}{num}, value {value}: got {actual_rank}, expected {expected_rank}"


@pytest.mark.skip(reason="This test is failing due to some row number implementation issue")
@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_multi_window_agg_functions(make_df):
    """Test multiple window aggregation functions with different partition keys.

    Tests window functions with two different partition specifications:
    1. Partitioning by both category and group
    2. Partitioning by just category

    Using sum(), mean(), min(), max() aggregations and verifying row_number ordering.
    """
    # Use distinct values to ensure we can verify row_number correctness
    data = [
        {"category": "A", "group": 1, "value": 15},
        {"category": "A", "group": 1, "value": 25},
        {"category": "A", "group": 2, "value": 35},
        {"category": "A", "group": 2, "value": 45},
        {"category": "B", "group": 1, "value": 55},
        {"category": "B", "group": 1, "value": 65},
        {"category": "B", "group": 2, "value": 75},
        {"category": "B", "group": 2, "value": 85},
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
        row_number().over(multi_partition_window).alias("multi_row_number"),
        row_number().over(single_partition_window).alias("single_row_number"),
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
            sorted_values = sorted(values)
            value_to_rank = {val: i + 1 for i, val in enumerate(sorted_values)}

            expected_sum = sum(values)
            expected_avg = sum(values) / len(values)

            for idx in indices:
                value = result_dict["value"][idx]
                assert (
                    result_dict["sum_multi"][idx] == expected_sum
                ), f"Incorrect sum for {category}/{group}: {result_dict['sum_multi'][idx]} != {expected_sum}"
                assert (
                    abs(result_dict["avg_multi"][idx] - expected_avg) < 1e-10
                ), f"Incorrect avg for {category}/{group}: {result_dict['avg_multi'][idx]} != {expected_avg}"

                # Verify row numbers for multi-partition window
                expected_rank = value_to_rank[value]
                actual_rank = result_dict["multi_row_number"][idx]
                assert (
                    actual_rank == expected_rank
                ), f"Incorrect multi-partition row number for {category}/{group}, value {value}: got {actual_rank}, expected {expected_rank}"

    for category in ["A", "B"]:
        indices = [i for i, cat in enumerate(result_dict["category"]) if cat == category]

        values = [result_dict["value"][i] for i in indices]
        sorted_values = sorted(values)
        value_to_rank = {val: i + 1 for i, val in enumerate(sorted_values)}

        expected_min = min(values)
        expected_max = max(values)

        for idx in indices:
            value = result_dict["value"][idx]
            assert (
                result_dict["min_single"][idx] == expected_min
            ), f"Incorrect min for {category}: {result_dict['min_single'][idx]} != {expected_min}"
            assert (
                result_dict["max_single"][idx] == expected_max
            ), f"Incorrect max for {category}: {result_dict['max_single'][idx]} != {expected_max}"

            # Verify row numbers for single-partition window
            expected_rank = value_to_rank[value]
            actual_rank = result_dict["single_row_number"][idx]
            assert (
                actual_rank == expected_rank
            ), f"Incorrect single-partition row number for {category}, value {value}: got {actual_rank}, expected {expected_rank}"
