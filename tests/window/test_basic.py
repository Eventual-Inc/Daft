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
