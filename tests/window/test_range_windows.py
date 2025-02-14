from __future__ import annotations

import pytest

from daft import Window, col
from daft.expressions import count, mean, sum


@pytest.fixture
def time_series_data():
    return {
        # Time series data with regular intervals
        "date": [f"2024-01-{str(i+1).zfill(2)}" for i in range(10)],
        "value": [100, 110, 95, 105, 115, 90, 100, 120, 110, 105],
        "category": ["A", "A", "B", "B", "A", "B", "A", "B", "A", "B"],
    }


def test_range_7day_window(make_df):
    """Stage: Complex Window Frames & Sliding Windows.

    Test 7-day range window.
    """
    df = make_df(
        {
            "date": [f"2024-01-{str(i+1).zfill(2)}" for i in range(10)],
            "value": [100, 110, 95, 105, 115, 90, 100, 120, 110, 105],
        }
    )

    window = Window.order_by("date").range_between("7 days preceding", "current row")
    result = df.select(
        [
            col("date"),
            col("value"),
            mean("value").over(window).alias("7day_avg"),
            sum("value").over(window).alias("7day_sum"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_range_partition_7day_window(make_df):
    """Stage: Complex Window Frames & Sliding Windows.

    Test 7-day range window with partitioning.
    """
    df = make_df(
        {
            "date": [f"2024-01-{str(i+1).zfill(2)}" for i in range(10)],
            "value": [100, 110, 95, 105, 115, 90, 100, 120, 110, 105],
            "category": ["A", "A", "B", "B", "A", "B", "A", "B", "A", "B"],
        }
    )

    window = Window.partition_by("category").order_by("date").range_between("7 days preceding", "current row")
    result = df.select(
        [
            col("category"),
            col("date"),
            col("value"),
            mean("value").over(window).alias("7day_avg"),
            sum("value").over(window).alias("7day_sum"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_range_numeric_window(make_df):
    """Stage: Complex Window Frames & Sliding Windows.

    Test range window based on numeric values.
    """
    df = make_df({"id": list(range(10)), "value": [100, 110, 95, 105, 115, 90, 100, 120, 110, 105]})

    window = Window.order_by("value").range_between(10, 0)  # Current value and up to 10 less
    result = df.select(
        [
            col("id"),
            col("value"),
            mean("value").over(window).alias("range_avg"),
            count("value").over(window).alias("range_count"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_range_mixed_bounds(make_df):
    """Stage: Complex Window Frames & Sliding Windows.

    Test range window with both preceding and following bounds.
    """
    df = make_df(
        {
            "date": [f"2024-01-{str(i+1).zfill(2)}" for i in range(10)],
            "value": [100, 110, 95, 105, 115, 90, 100, 120, 110, 105],
        }
    )

    window = Window.order_by("date").range_between("3 days preceding", "3 days following")
    result = df.select(
        [
            col("date"),
            col("value"),
            mean("value").over(window).alias("7day_centered_avg"),
            sum("value").over(window).alias("7day_centered_sum"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_range_unbounded(make_df):
    """Stage: Complex Window Frames & Sliding Windows.

    Test range window with unbounded preceding/following.
    """
    df = make_df(
        {
            "date": [f"2024-01-{str(i+1).zfill(2)}" for i in range(10)],
            "value": [100, 110, 95, 105, 115, 90, 100, 120, 110, 105],
        }
    )

    window = Window.order_by("date").range_between(Window.unboundedPreceding, "current row")
    result = df.select(
        [
            col("date"),
            col("value"),
            mean("value").over(window).alias("cumulative_avg"),
            sum("value").over(window).alias("cumulative_sum"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4, 8])
def test_range_distributed(make_df, n_partitions):
    """Stage: Complex Window Frames & Sliding Windows.

    Test range windows with different numbers of partitions.
    """
    df = make_df(
        {
            "date": [f"2024-01-{str(i+1).zfill(2)}" for i in range(10)],
            "value": [100, 110, 95, 105, 115, 90, 100, 120, 110, 105],
        },
        repartition=n_partitions,
    )

    window = Window.order_by("date").range_between("7 days preceding", "current row")
    result = df.select(
        [
            col("date"),
            col("value"),
            mean("value").over(window).alias("7day_avg"),
            sum("value").over(window).alias("7day_sum"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected
