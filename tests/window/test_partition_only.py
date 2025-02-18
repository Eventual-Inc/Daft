from __future__ import annotations

import pytest

from daft import Window, col

# from daft.expressions import count, max, mean, min, sum


@pytest.fixture
def sample_data():
    return {
        # Unsorted data with multiple groups
        "category": ["B", "A", "C", "A", "B", "C", "A", "B"],
        "region": ["West", "East", "North", "West", "East", "South", "North", "South"],
        "value": [10, 5, 15, 8, 12, 6, 9, 7],
        "nullable_value": [10, None, 15, 8, None, 6, 9, 7],
    }


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
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_multiple_partitions_sum(make_df):
    """Stage: PARTITION BY-Only Window Aggregations.

    Test sum over multiple partition columns.
    """
    df = make_df(
        {
            "category": ["B", "A", "C", "A", "B", "C", "A", "B"],
            "region": ["West", "East", "North", "West", "East", "South", "North", "South"],
            "value": [10, 5, 15, 8, 12, 6, 9, 7],
        }
    )

    window = Window.partition_by(["category", "region"])
    result = df.select(
        col("*"),
        # sum("value").over(window).alias("sum"),
        col("value").sum().over(window).alias("sum"),
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_partition_all_aggs(make_df):
    """Stage: PARTITION BY-Only Window Aggregations.

    Test all aggregation functions over partitions.
    """
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window.partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        # sum("value").over(window).alias("sum"),
        # mean("value").over(window).alias("avg"),
        # min("value").over(window).alias("min"),
        # max("value").over(window).alias("max"),
        # count("value").over(window).alias("count"),
        col("value").sum().over(window).alias("sum"),
        col("value").mean().over(window).alias("avg"),
        col("value").min().over(window).alias("min"),
        col("value").max().over(window).alias("max"),
        col("value").count().over(window).alias("count"),
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_partition_null_handling(make_df):
    """Stage: PARTITION BY-Only Window Aggregations.

    Test handling of null values in window aggregations.
    """
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, None, 15, 8, None, 6, 9, 7]})

    window = Window.partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        # sum("value").over(window).alias("sum"),
        # mean("value").over(window).alias("avg"),
        # count("value").over(window).alias("count"),
        col("value").sum().over(window).alias("sum"),
        col("value").mean().over(window).alias("avg"),
        col("value").count().over(window).alias("count"),
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected
