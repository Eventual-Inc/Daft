from __future__ import annotations

import pandas as pd

from daft import Window, col

# from daft.expressions import count, max, mean, min, sum


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
def test_single_partition_sum(make_df):
    """Stage: PARTITION BY-Only Window Aggregations.

    Test sum over a single partition column.
    """
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window.partition_by("category")
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
def test_single_partition_min(make_df):
    """Test min over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window.partition_by("category")
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


def test_single_partition_max(make_df):
    """Test max over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window.partition_by("category")
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


def test_single_partition_mean(make_df):
    """Test mean over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window.partition_by("category")
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


def test_single_partition_count(make_df):
    """Test count over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window.partition_by("category")
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


def test_multiple_partition_columns(make_df):
    """Test sum over multiple partition columns."""
    df = make_df(
        {
            "category": ["B", "A", "C", "A", "B", "C", "A", "B"],
            "group": [1, 1, 2, 1, 2, 2, 2, 1],
            "value": [10, 5, 15, 8, 12, 6, 9, 7],
        }
    )

    window = Window.partition_by(["category", "group"])
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


def test_similar_partition_keys(make_df):
    """Test partition keys with similar string/number combinations that might cause hash collisions."""
    df = make_df(
        {
            "key1": ["A1", "B2", "C3", "D4", "E5", "A2", "B1", "C4", "D3", "E2"],
            "key2": ["X1", "Y2", "Z3", "X2", "Y3", "Z1", "X3", "Y1", "Z2", "X4"],
            "value": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )

    window = Window.partition_by(["key1", "key2"])
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


def test_null_partition_values(make_df):
    """Test window functions with null values in partition columns."""
    df = make_df(
        {
            "category": ["A", "A", None, "B", "B", None, "C", None],
            "value": [5, 8, 9, 10, 12, 7, 15, 6],
        }
    )

    window = Window.partition_by("category")
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


def test_multiple_window_functions(make_df):
    """Test multiple window functions in the same query."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window.partition_by("category")
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


def test_many_partitions(make_df):
    """Test window functions with a large number of partitions."""
    # Create a dataset with 100 unique partition keys
    num_partitions = 100
    categories = [f"category_{i}" for i in range(num_partitions)]
    values = list(range(num_partitions))

    df = make_df({"category": categories, "value": values})

    window = Window.partition_by("category")
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
