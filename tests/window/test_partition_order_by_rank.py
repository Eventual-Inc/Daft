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


def test_row_number_function(make_df):
    df = make_df(
        {"category": ["A", "A", "A", "B", "B", "B", "C", "C"], "sales": [100, 200, 50, 500, 100, 300, 250, 150]}
    )

    window_spec = Window().partition_by("category").order_by("sales", desc=False)

    result = df.select(
        col("category"), col("sales"), row_number().over(window_spec).alias("row_number_sales")
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 100, 300, 250, 150],
        "row_number_sales": [2, 3, 1, 3, 1, 2, 2, 1],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()), check_dtype=False)


def test_row_number_function_desc(make_df):
    df = make_df(
        {"category": ["A", "A", "A", "B", "B", "B", "C", "C"], "sales": [100, 200, 50, 500, 100, 300, 250, 150]}
    )

    window_spec = Window().partition_by("category").order_by("sales", desc=True)

    result = df.select(
        col("category"), col("sales"), row_number().over(window_spec).alias("row_number_sales")
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 100, 300, 250, 150],
        "row_number_sales": [2, 1, 3, 1, 3, 2, 1, 2],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()), check_dtype=False)


def test_multiple_window_partitions(make_df):
    """Test multiple window functions with different partition keys using random numbers.

    Creates a dataset with 900 rows (100 rows each for A1, A2, A3, B1, B2, B3, C1, C2, C3)
    and verifies row_number ordering within each partition.
    """
    import random

    random.seed(42)

    data = []
    all_numbers = list(range(1, 1001))
    random.shuffle(all_numbers)
    number_idx = 0

    for letter in ["A", "B", "C"]:
        for num in ["1", "2", "3"]:
            group_values = sorted(all_numbers[number_idx : number_idx + 3])
            number_idx += 3

            for value in group_values:
                data.append({"letter": letter, "num": num, "value": value})

    df = make_df(data)

    letter_window = Window().partition_by("letter").order_by("value", desc=False)
    num_window = Window().partition_by("num").order_by("value", desc=False)
    combined_window = Window().partition_by(["letter", "num"]).order_by("value", desc=False)

    result = df.select(
        col("letter"),
        col("num"),
        col("value"),
        row_number().over(letter_window).alias("letter_row_number"),
        row_number().over(num_window).alias("num_row_number"),
        row_number().over(combined_window).alias("combined_row_number"),
    ).collect()

    result_dict = result.to_pydict()

    for letter in ["A", "B", "C"]:
        letter_indices = [i for i, ltr in enumerate(result_dict["letter"]) if ltr == letter]
        letter_values = [result_dict["value"][i] for i in letter_indices]

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

            sorted_combined_values = sorted(combined_values)
            value_to_rank = {val: i + 1 for i, val in enumerate(sorted_combined_values)}
            for idx in combined_indices:
                value = result_dict["value"][idx]
                expected_rank = value_to_rank[value]
                actual_rank = result_dict["combined_row_number"][idx]
                assert (
                    actual_rank == expected_rank
                ), f"Incorrect row number for {letter}{num}, value {value}: got {actual_rank}, expected {expected_rank}"


def test_multi_window_agg_functions(make_df):
    """Test multiple window functions with different partition keys.

    Tests window functions with two different partition specifications:
    1. Partitioning by both category and group
    2. Partitioning by just category

    Using row_number ordering.
    """
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

    multi_partition_window = Window().partition_by(["category", "group"]).order_by("value", desc=False)
    single_partition_window = Window().partition_by("category").order_by("value", desc=False)

    result = df.select(
        col("category"),
        col("group"),
        col("value"),
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

            for idx in indices:
                value = result_dict["value"][idx]
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

        for idx in indices:
            value = result_dict["value"][idx]
            expected_rank = value_to_rank[value]
            actual_rank = result_dict["single_row_number"][idx]
            assert (
                actual_rank == expected_rank
            ), f"Incorrect single-partition row number for {category}, value {value}: got {actual_rank}, expected {expected_rank}"


def test_rank_function_single_row_per_group(make_df):
    """Test rank function with single row per group."""
    data = [
        {"category": "A", "value": 10},
        {"category": "B", "value": 20},
        {"category": "C", "value": 30},
    ]

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("value", desc=False)

    result = df.select(col("category"), col("value"), rank().over(window_spec).alias("rank")).collect()

    expected = {
        "category": ["A", "B", "C"],
        "value": [10, 20, 30],
        "rank": [1, 1, 1],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=["category"], check_dtype=False)

    result = df.select(col("category"), col("value"), dense_rank().over(window_spec).alias("rank")).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=["category"], check_dtype=False)


@pytest.mark.parametrize(
    "desc,nulls_first,expected_rank",
    [
        (False, False, [1, 2, 1, 2, 1, 2]),
        (False, True, [2, 1, 2, 1, 2, 1]),
        (True, False, [1, 2, 1, 2, 1, 2]),
        (True, True, [2, 1, 2, 1, 2, 1]),
    ],
)
def test_rank_function_single_row_per_group_nulls_first_or_last(make_df, desc, nulls_first, expected_rank):
    """Test rank function with single row per group and nulls_first or nulls_last."""
    data = [
        {"id": 1, "category": "A", "value": 10},
        {"id": 2, "category": "A", "value": None},
        {"id": 3, "category": "B", "value": 20},
        {"id": 4, "category": "B", "value": None},
        {"id": 5, "category": "C", "value": 30},
        {"id": 6, "category": "C", "value": None},
    ]

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("value", desc=desc, nulls_first=nulls_first)

    result = df.select(col("id"), col("category"), col("value"), rank().over(window_spec).alias("rank")).collect()

    expected = {
        "id": [1, 2, 3, 4, 5, 6],
        "category": ["A", "A", "B", "B", "C", "C"],
        "value": [10, None, 20, None, 30, None],
        "rank": expected_rank,
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=["id"], check_dtype=False)

    result = df.select(col("id"), col("category"), col("value"), dense_rank().over(window_spec).alias("rank")).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=["id"], check_dtype=False)


def test_rank_function_no_order_by(make_df):
    """Test rank function when order_by columns are the same as group_by."""
    data = [
        {"category": "A", "value": 10},
        {"category": "A", "value": 20},
        {"category": "A", "value": 30},
        {"category": "B", "value": 40},
        {"category": "B", "value": 50},
        {"category": "C", "value": 60},
    ]

    df = make_df(data)

    window_spec = Window().partition_by("category").order_by("category", desc=False)

    result = df.select(col("category"), col("value"), rank().over(window_spec).alias("rank")).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "C"],
        "value": [10, 20, 30, 40, 50, 60],
        "rank": [1, 1, 1, 1, 1, 1],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=["category", "value"], check_dtype=False)

    result = df.select(col("category"), col("value"), dense_rank().over(window_spec).alias("rank")).collect()

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=["category", "value"], check_dtype=False)


def test_multiple_rank_functions(make_df):
    """Test multiple rank functions over different window specifications.

    Creates a dataset with category (A, B, C) and subcategory (1, 2, 3) groups,
    each containing values with ties. Tests row_number, rank, and dense_rank
    functions over different window specifications.
    """
    import random

    random.seed(42)

    data = []

    for category in ["A", "B", "C"]:
        for subcategory in [1, 2, 3]:
            values = [random.randint(1, 10) for _ in range(8)]

            for value in values:
                data.append({"category": category, "subcategory": subcategory, "value": value})

    random.shuffle(data)
    df = make_df(data)

    category_window = Window().partition_by("category").order_by("value", desc=False)
    subcategory_window = Window().partition_by("subcategory").order_by("value", desc=False)
    combined_window = Window().partition_by(["category", "subcategory"]).order_by("value", desc=False)

    result = df.select(
        col("category"),
        col("subcategory"),
        col("value"),
        row_number().over(category_window).alias("row_number_by_category"),
        row_number().over(subcategory_window).alias("row_number_by_subcategory"),
        row_number().over(combined_window).alias("row_number_combined"),
        rank().over(category_window).alias("rank_by_category"),
        rank().over(subcategory_window).alias("rank_by_subcategory"),
        rank().over(combined_window).alias("rank_combined"),
        dense_rank().over(category_window).alias("dense_rank_by_category"),
        dense_rank().over(subcategory_window).alias("dense_rank_by_subcategory"),
        dense_rank().over(combined_window).alias("dense_rank_combined"),
    ).collect()

    result_df = result.to_pandas()

    def validate_rank_function(df, partition_cols, rank_col, rank_type):
        partition_groups = df.groupby(partition_cols)

        for partition_key, group in partition_groups:
            sorted_group = group.sort_values("value").reset_index(drop=True)

            value_to_ranks = {}
            for _, row in sorted_group.iterrows():
                value = row["value"]
                rank = row[rank_col]

                if value not in value_to_ranks:
                    value_to_ranks[value] = []
                value_to_ranks[value].append(rank)

            if rank_type == "row_number":
                all_ranks = sorted([r for ranks in value_to_ranks.values() for r in ranks])
                assert len(all_ranks) == len(
                    set(all_ranks)
                ), f"Row numbers should be unique within partition {partition_key}"

                assert all_ranks == list(range(1, len(all_ranks) + 1)), f"Row numbers should be sequential: {all_ranks}"

            elif rank_type == "rank":
                for value, ranks in value_to_ranks.items():
                    assert len(set(ranks)) == 1, f"All instances of value {value} should have the same rank: {ranks}"

                unique_value_ranks = {value: ranks[0] for value, ranks in value_to_ranks.items()}

                prev_value = None
                prev_rank = 0
                for value in sorted(unique_value_ranks.keys()):
                    rank = unique_value_ranks[value]
                    assert (
                        rank > prev_rank
                    ), f"Rank should increase for larger values: {value} has rank {rank}, previous rank was {prev_rank}"

                    if prev_value is not None:
                        expected_min_rank = prev_rank + len(value_to_ranks[prev_value])
                        assert (
                            rank >= expected_min_rank
                        ), f"Rank for value {value} should be at least {expected_min_rank}, got {rank}"

                    prev_value = value
                    prev_rank = rank

            elif rank_type == "dense_rank":
                for value, ranks in value_to_ranks.items():
                    assert (
                        len(set(ranks)) == 1
                    ), f"All instances of value {value} should have the same dense rank: {ranks}"

                unique_value_ranks = {value: ranks[0] for value, ranks in value_to_ranks.items()}

                unique_values = sorted(unique_value_ranks.keys())
                unique_ranks = [unique_value_ranks[v] for v in unique_values]

                assert unique_ranks[0] == 1, f"First dense rank should be 1, got {unique_ranks[0]}"

                for i in range(1, len(unique_ranks)):
                    assert (
                        unique_ranks[i] == unique_ranks[i - 1] + 1
                    ), f"Dense rank should increase by 1 for each distinct value: {unique_values[i]} has rank {unique_ranks[i]}, previous was {unique_ranks[i-1]}"

    for partition, col_prefix in [
        (["category"], "by_category"),
        (["subcategory"], "by_subcategory"),
        (["category", "subcategory"], "combined"),
    ]:
        validate_rank_function(result_df, partition, f"row_number_{col_prefix}", "row_number")
        validate_rank_function(result_df, partition, f"rank_{col_prefix}", "rank")
        validate_rank_function(result_df, partition, f"dense_rank_{col_prefix}", "dense_rank")


def test_multi_ordering_combinations(make_df):
    """Test row numbering with all possible ordering combinations."""
    random.seed(42)

    all_point_coordinates = []
    for x in range(10):
        for y in range(10):
            all_point_coordinates.append((x, y))

    test_data = []
    for group in ["A", "B", "C"]:
        selected_points = random.sample(all_point_coordinates, 10)
        for x, y in selected_points:
            test_data.append({"group": group, "x": x, "y": y})

    df = make_df(test_data)

    window_spec1 = Window().partition_by("group").order_by(["x", "y"], desc=[False, False])  # x asc, y asc
    window_spec2 = Window().partition_by("group").order_by(["x", "y"], desc=[False, True])  # x asc, y desc
    window_spec3 = Window().partition_by("group").order_by(["x", "y"], desc=[True, False])  # x desc, y asc
    window_spec4 = Window().partition_by("group").order_by(["x", "y"], desc=[True, True])  # x desc, y desc
    window_spec5 = Window().partition_by("group").order_by(["y", "x"], desc=[False, False])  # y asc, x asc
    window_spec6 = Window().partition_by("group").order_by(["y", "x"], desc=[False, True])  # y asc, x desc
    window_spec7 = Window().partition_by("group").order_by(["y", "x"], desc=[True, False])  # y desc, x asc
    window_spec8 = Window().partition_by("group").order_by(["y", "x"], desc=[True, True])  # y desc, x desc

    result = df.select(
        col("group"),
        col("x"),
        col("y"),
        row_number().over(window_spec1).alias("row_number_1"),
        row_number().over(window_spec2).alias("row_number_2"),
        row_number().over(window_spec3).alias("row_number_3"),
        row_number().over(window_spec4).alias("row_number_4"),
        row_number().over(window_spec5).alias("row_number_5"),
        row_number().over(window_spec6).alias("row_number_6"),
        row_number().over(window_spec7).alias("row_number_7"),
        row_number().over(window_spec8).alias("row_number_8"),
    ).collect()

    result_dict = result.to_pydict()

    row_number_mapping = {
        (0, 1, 1): 1,  # x asc, y asc
        (0, 1, -1): 2,  # x asc, y desc
        (0, -1, 1): 3,  # x desc, y asc
        (0, -1, -1): 4,  # x desc, y desc
        (1, 1, 1): 5,  # y asc, x asc
        (1, 1, -1): 6,  # y asc, x desc
        (1, -1, 1): 7,  # y desc, x asc
        (1, -1, -1): 8,  # y desc, x desc
    }

    for group in ["A", "B", "C"]:
        for primary_key in [0, 1]:  # 0 = x, 1 = y
            for primary_desc in [1, -1]:  # 1 = asc, -1 = desc
                for secondary_desc in [1, -1]:  # 1 = asc, -1 = desc
                    row_number_index = row_number_mapping[(primary_key, primary_desc, secondary_desc)]

                    points_with_row_numbers = [
                        (result_dict["x"][i], result_dict["y"][i], result_dict[f"row_number_{row_number_index}"][i])
                        for i, grp in enumerate(result_dict["group"])
                        if grp == group
                    ]

                    sorted_points = sorted(
                        points_with_row_numbers,
                        key=lambda p: (p[primary_key] * primary_desc, p[not primary_key] * secondary_desc),
                    )

                    for i, (x, y, actual_row_num) in enumerate(sorted_points):
                        expected_row_num = i + 1
                        assert expected_row_num == actual_row_num, (
                            f"Incorrect row number for group {group}, "
                            f"primary key {['x', 'y'][primary_key]} {('asc' if primary_desc == 1 else 'desc')}, "
                            f"secondary key {['x', 'y'][not primary_key]} {('asc' if secondary_desc == 1 else 'desc')}: "
                            f"expected {expected_row_num}, got {actual_row_num}"
                        )
