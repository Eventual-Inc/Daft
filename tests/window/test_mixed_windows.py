from __future__ import annotations

import pytest

from daft import Window, col
from daft.expressions import mean, rank, sum


@pytest.fixture
def mixed_data():
    return {
        # Data suitable for multiple window types
        "category": ["A", "B", "A", "B", "A", "B"],
        "region": ["West", "East", "West", "East", "West", "East"],
        "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
        "value": [100, 150, 120, 160, 90, 140],
        "quantity": [10, 15, 12, 16, 9, 14],
    }


def test_multiple_window_specs_same_col(make_df):
    """Stage: Other.

    Test multiple different window specs on the same column.
    """
    df = make_df(
        {
            "category": ["A", "B", "A", "B", "A", "B"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "value": [100, 150, 120, 160, 90, 140],
        }
    )

    partition_window = Window.partition_by("category")
    ordered_window = Window.partition_by("category").order_by("date")
    running_window = (
        Window.partition_by("category").order_by("date").rows_between(Window.unboundedPreceding, Window.currentRow)
    )

    result = df.select(
        [
            col("category"),
            col("date"),
            col("value"),
            mean("value").over(partition_window).alias("category_avg"),
            rank().over(ordered_window).alias("rank_in_category"),
            sum("value").over(running_window).alias("running_total"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_mixed_partition_and_global(make_df):
    """Stage: Other.

    Test mixing partitioned and global window functions.
    """
    df = make_df({"category": ["A", "B", "A", "B", "A", "B"], "value": [100, 150, 120, 160, 90, 140]})

    partition_window = Window.partition_by("category")
    global_window = Window.order_by("value")

    result = df.select(
        [
            col("category"),
            col("value"),
            mean("value").over(partition_window).alias("category_avg"),
            rank().over(global_window).alias("global_rank"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_mixed_rows_and_range(make_df):
    """Stage: Complex Window Frames & Sliding Windows.

    Test mixing row-based and range-based windows.
    """
    df = make_df(
        {
            "category": ["A", "B", "A", "B", "A", "B"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "value": [100, 150, 120, 160, 90, 140],
        }
    )

    rows_window = Window.partition_by("category").order_by("date").rows_between(-1, 1)  # Previous, current, next
    range_window = Window.partition_by("category").order_by("date").range_between("1 day preceding", "1 day following")

    result = df.select(
        [
            col("category"),
            col("date"),
            col("value"),
            mean("value").over(rows_window).alias("rows_avg"),
            mean("value").over(range_window).alias("range_avg"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_complex_window_combination(make_df):
    """Stage: Other.

    Test complex combination of different window types.
    """
    df = make_df(
        {
            "category": ["A", "B", "A", "B", "A", "B"],
            "region": ["West", "East", "West", "East", "West", "East"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "value": [100, 150, 120, 160, 90, 140],
        }
    )

    # Multiple partition levels
    category_window = Window.partition_by("category")
    region_window = Window.partition_by(["category", "region"])

    # Ordered windows
    time_window = (
        Window.partition_by("category").order_by("date").rows_between(Window.unboundedPreceding, Window.currentRow)
    )
    global_window = Window.order_by("value")

    result = df.select(
        [
            col("category"),
            col("region"),
            col("date"),
            col("value"),
            mean("value").over(category_window).alias("category_avg"),
            mean("value").over(region_window).alias("region_avg"),
            sum("value").over(time_window).alias("running_total"),
            rank().over(global_window).alias("global_rank"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_nested_window_functions(make_df):
    """Stage: Other.

    Test operations on window function results.
    """
    df = make_df(
        {
            "category": ["A", "B", "A", "B", "A", "B"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "value": [100, 150, 120, 160, 90, 140],
        }
    )

    # Calculate running total, then rank based on that
    running_window = (
        Window.partition_by("category").order_by("date").rows_between(Window.unboundedPreceding, Window.currentRow)
    )
    rank_window = Window.partition_by("category").order_by("running_total")

    # First calculate running total
    df_with_running = df.select(
        [col("category"), col("date"), col("value"), sum("value").over(running_window).alias("running_total")]
    )

    # Then rank based on running total
    result = df_with_running.select(
        [
            col("category"),
            col("date"),
            col("value"),
            col("running_total"),
            rank().over(rank_window).alias("rank_by_running_total"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected
