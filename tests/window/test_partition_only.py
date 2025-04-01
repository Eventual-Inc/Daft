from __future__ import annotations

import random

import pandas as pd
import pytest

from daft import Window, col
from tests.conftest import get_tests_daft_runner_name

# from daft.expressions import count, max, mean, min, sum

# Skip all tests in this module since execution is not implemented yet
pytestmark = pytest.mark.skip(reason="Window function execution not implemented yet")


def assert_equal_ignoring_order(result_dict, expected_dict):
    """Helper function to verify dictionaries are equal, ignoring row order.

    Converts both dictionaries to pandas DataFrames, sorts them by the keys,
    and then compares equality.
    """
    # Convert dictionaries to DataFrames
    result_df = pd.DataFrame(result_dict)
    expected_df = pd.DataFrame(expected_dict)

    # Sort both DataFrames by all columns
    result_df = result_df.sort_values(by=list(result_dict.keys())).reset_index(drop=True)
    expected_df = expected_df.sort_values(by=list(expected_dict.keys())).reset_index(drop=True)

    # Convert back to dictionaries for comparison
    sorted_result = {k: result_df[k].tolist() for k in result_dict.keys()}
    sorted_expected = {k: expected_df[k].tolist() for k in expected_dict.keys()}

    # Compare using normal equality
    assert (
        sorted_result == sorted_expected
    ), f"Result data doesn't match expected after sorting.\nGot: {sorted_result}\nExpected: {sorted_expected}"


# @pytest.mark.skip(reason="Skipping this test (currently hardcoded to pass in pipeline.rs)")
@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_single_partition_sum(make_df):
    """Stage: PARTITION BY-Only Window Aggregations.

    Test sum over a single partition column.
    """
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        # sum("value").over(window).alias("sum"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "sum": [22, 22, 22, 29, 29, 29, 21, 21],
    }

    # Use our helper function instead of direct equality
    assert_equal_ignoring_order(result.to_pydict(), expected)


# @pytest.mark.skip(reason="Skipping this test (currently hardcoded to pass in pipeline.rs)")
@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_single_partition_min(make_df):
    """Test min over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").min().over(window).alias("min"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "min": [5, 5, 5, 7, 7, 7, 6, 6],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_single_partition_max(make_df):
    """Test max over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").max().over(window).alias("max"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "max": [9, 9, 9, 12, 12, 12, 15, 15],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_single_partition_mean(make_df):
    """Test mean over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").mean().over(window).alias("mean"),
    ).collect()

    # A: (5+8+9)/3 = 7.333, B: (10+12+7)/3 = 9.667, C: (15+6)/2 = 10.5
    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "mean": [
            7.333333333333333,
            7.333333333333333,
            7.333333333333333,
            9.666666666666666,
            9.666666666666666,
            9.666666666666666,
            10.5,
            10.5,
        ],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_single_partition_count(make_df):
    """Test count over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").count().over(window).alias("count"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "count": [3, 3, 3, 3, 3, 3, 2, 2],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_multiple_partition_columns(make_df):
    """Test sum over multiple partition columns."""
    df = make_df(
        {
            "category": ["B", "A", "C", "A", "B", "C", "A", "B"],
            "group": [1, 1, 2, 1, 2, 2, 2, 1],
            "value": [10, 5, 15, 8, 12, 6, 9, 7],
        }
    )

    window = Window().partition_by(["category", "group"])
    result = df.select(
        col("category"),
        col("group"),
        col("value"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "group": [1, 1, 2, 1, 2, 1, 2, 2],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "sum": [13, 13, 9, 17, 12, 17, 21, 21],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_similar_partition_keys(make_df):
    """Test partition keys with similar string/number combinations that might cause hash collisions."""
    df = make_df(
        {
            "key1": ["A1", "B2", "C3", "D4", "E5", "A2", "B1", "C4", "D3", "E2"],
            "key2": ["X1", "Y2", "Z3", "X2", "Y3", "Z1", "X3", "Y1", "Z2", "X4"],
            "value": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )

    window = Window().partition_by(["key1", "key2"])
    result = df.select(
        col("key1"),
        col("key2"),
        col("value"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    # Each partition should have exactly one row (all key combinations are unique)
    expected_dict = result.to_pydict()

    # Verify each partition has a sum equal to its own value (no collisions)
    for i in range(len(expected_dict["key1"])):
        assert (
            expected_dict["sum"][i] == expected_dict["value"][i]
        ), f"Hash collision detected for keys {expected_dict['key1'][i]}/{expected_dict['key2'][i]}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_null_partition_values(make_df):
    """Test window functions with null values in partition columns."""
    df = make_df(
        {
            "category": ["A", "A", None, "B", "B", None, "C", None],
            "value": [5, 8, 9, 10, 12, 7, 15, 6],
        }
    )

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    # Convert to dictionaries for easy processing
    result_dict = result.to_pydict()

    # Group by category and verify sums
    for category in set(result_dict["category"]):
        if category is None:
            # All null categories should be grouped together
            null_indices = [i for i, cat in enumerate(result_dict["category"]) if cat is None]
            null_values = [result_dict["value"][i] for i in null_indices]
            expected_sum = sum(null_values)
            actual_sums = [result_dict["sum"][i] for i in null_indices]
            assert all(
                sum == expected_sum for sum in actual_sums
            ), f"Incorrect sum for null category: {actual_sums} != {expected_sum}"
        else:
            # Check non-null categories
            cat_indices = [i for i, cat in enumerate(result_dict["category"]) if cat == category]
            cat_values = [result_dict["value"][i] for i in cat_indices]
            expected_sum = sum(cat_values)
            actual_sums = [result_dict["sum"][i] for i in cat_indices]
            assert all(
                sum == expected_sum for sum in actual_sums
            ), f"Incorrect sum for category {category}: {actual_sums} != {expected_sum}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_multiple_window_functions(make_df):
    """Test multiple window functions in the same query."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").sum().over(window).alias("sum"),
        col("value").min().over(window).alias("min"),
        col("value").max().over(window).alias("max"),
        col("value").mean().over(window).alias("mean"),
        col("value").count().over(window).alias("count"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "sum": [22, 22, 22, 29, 29, 29, 21, 21],
        "min": [5, 5, 5, 7, 7, 7, 6, 6],
        "max": [9, 9, 9, 12, 12, 12, 15, 15],
        "mean": [
            7.333333333333333,
            7.333333333333333,
            7.333333333333333,
            9.666666666666666,
            9.666666666666666,
            9.666666666666666,
            10.5,
            10.5,
        ],
        "count": [3, 3, 3, 3, 3, 3, 2, 2],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_many_partitions(make_df):
    """Test window functions with a large number of partitions."""
    # Create a dataset with 100 unique partition keys
    num_partitions = 100
    categories = [f"category_{i}" for i in range(num_partitions)]
    values = list(range(num_partitions))

    df = make_df({"category": categories, "value": values})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    # Each partition should have exactly one value
    result_dict = result.to_pydict()

    # Verify each partition has a sum equal to its own value
    for i in range(len(result_dict["category"])):
        assert (
            result_dict["sum"][i] == result_dict["value"][i]
        ), f"Expected sum equal to value for single-row partition {result_dict['category'][i]}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_window_mean_minus_value(make_df):
    """Test arithmetic with window mean and value."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        (col("value").mean().over(window) - col("value")).alias("mean_minus_value"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 7, 10, 12, 6, 15],
        "mean_minus_value": [
            2.333333333333333,
            -0.666666666666667,
            -1.666666666666667,
            2.666666666666666,
            -0.3333333333333339,
            -2.333333333333334,
            4.5,
            -4.5,
        ],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_window_value_over_sum(make_df):
    """Test division with value and window sum."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        (col("value") / col("value").sum().over(window)).alias("value_over_sum"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 7, 10, 12, 6, 15],
        "value_over_sum": [
            0.22727272727272727,
            0.36363636363636365,
            0.4090909090909091,
            0.2413793103448276,
            0.3448275862068966,
            0.41379310344827586,
            0.2857142857142857,
            0.7142857142857143,
        ],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_partition_by_with_expressions(make_df):
    """Test window functions with expressions in partition_by clause."""
    df = make_df({"num": [1, 2, 3, 4, 5, 6, 7, 8], "value": [10, 20, 30, 40, 50, 60, 70, 80]})

    # First add the expression as a column to the dataframe
    df = df.with_column("parity", col("num") % 2)

    # Use the column in partition_by as an expression
    parity_expr = col("parity")
    window = Window().partition_by(parity_expr)

    result = df.select(
        col("num"),
        col("value"),
        col("parity"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    result_dict = result.to_pydict()

    # Group by parity and check sums
    odd_values = [result_dict["value"][i] for i, p in enumerate(result_dict["parity"]) if p == 1]
    even_values = [result_dict["value"][i] for i, p in enumerate(result_dict["parity"]) if p == 0]

    odd_sum = sum(odd_values)
    even_sum = sum(even_values)

    # Verify each group has the correct sum
    for i, parity in enumerate(result_dict["parity"]):
        expected_sum = odd_sum if parity == 1 else even_sum
        assert result_dict["sum"][i] == expected_sum, f"Row {i} has incorrect sum for parity {parity}"


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_multiple_window_partitions(make_df):
    """Test multiple window functions with different partition keys using random numbers.

    Creates a dataset with 18 rows (2 rows each for A1, A2, A3, B1, B2, B3, C1, C2, C3)
    and verifies sums across different partition keys (A/B/C and 1/2/3).
    """
    random.seed(42)  # For reproducibility

    # Create data with 3 rows for each combination of letter (A/B/C) and number (1/2/3)
    data = []
    for letter in ["A", "B", "C"]:
        for num in ["1", "2", "3"]:
            # Generate 2 random values for each combination
            for _ in range(3):
                data.append({"letter": letter, "num": num, "value": random.randint(1, 100)})

    df = make_df(data)

    # Create windows for different partition keys
    letter_window = Window().partition_by("letter")
    num_window = Window().partition_by("num")
    combined_window = Window().partition_by(["letter", "num"])

    result = df.select(
        col("letter"),
        col("num"),
        col("value"),
        col("value").sum().over(letter_window).alias("letter_sum"),
        col("value").sum().over(num_window).alias("num_sum"),
        col("value").sum().over(combined_window).alias("combined_sum"),
    ).collect()

    result_dict = result.to_pydict()

    # Verify letter-based sums (A, B, C)
    for letter in ["A", "B", "C"]:
        letter_indices = [i for i, ltr in enumerate(result_dict["letter"]) if ltr == letter]
        letter_values = [result_dict["value"][i] for i in letter_indices]
        expected_letter_sum = sum(letter_values)
        actual_letter_sums = [result_dict["letter_sum"][i] for i in letter_indices]
        assert all(
            sum == expected_letter_sum for sum in actual_letter_sums
        ), f"Incorrect sum for letter {letter}: {actual_letter_sums} != {expected_letter_sum}"

    # Verify number-based sums (1, 2, 3)
    for num in ["1", "2", "3"]:
        num_indices = [i for i, n in enumerate(result_dict["num"]) if n == num]
        num_values = [result_dict["value"][i] for i in num_indices]
        expected_num_sum = sum(num_values)
        actual_num_sums = [result_dict["num_sum"][i] for i in num_indices]
        assert all(
            sum == expected_num_sum for sum in actual_num_sums
        ), f"Incorrect sum for number {num}: {actual_num_sums} != {expected_num_sum}"

    # Verify combined window sums (A1, A2, A3, B1, B2, B3, C1, C2, C3)
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

    # Verify that each row's letter_sum + num_sum - combined_sum equals the total sum
    total_sum = sum(result_dict["value"])
    for i in range(len(result_dict["value"])):
        expected = total_sum
        actual = result_dict["letter_sum"][i] + result_dict["num_sum"][i] - result_dict["combined_sum"][i]
        assert abs(expected - actual) < 1e-10, f"Row {i} has incorrect combined sum"
