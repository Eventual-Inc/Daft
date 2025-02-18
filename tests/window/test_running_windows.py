from __future__ import annotations

import pytest

from daft import Window, col

# from daft.expressions import mean, sum


@pytest.fixture
def sales_data():
    return {
        # Unsorted sales data with multiple stores
        "store": ["A", "B", "A", "B", "A", "B", "A", "B"],
        "date": [
            "2024-01-01",
            "2024-01-01",
            "2024-01-02",
            "2024-01-02",
            "2024-01-03",
            "2024-01-03",
            "2024-01-04",
            "2024-01-04",
        ],
        "sales": [100, 150, 120, 160, 90, 140, 110, 170],
        "returns": [10, 15, 8, 12, 5, 10, 7, 13],
    }


def test_cumulative_sum(make_df):
    """Stage: PARTITION BY + ORDER BY + ROWS BETWEEN (Unbounded Preceding to Current Row).

    Test cumulative sum within partitions.
    """
    df = make_df(
        {
            "store": ["A", "B", "A", "B", "A", "B"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "sales": [100, 150, 120, 160, 90, 140],
        }
    )

    window = Window.partition_by("store").order_by("date").rows_between(Window.unboundedPreceding, Window.currentRow)
    result = df.select(
        [
            col("store"),
            col("date"),
            col("sales"),
            # sum("sales").over(window).alias("running_total"),
            col("sales").sum().over(window).alias("running_total"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_cumulative_multiple_aggs(make_df):
    """Stage: PARTITION BY + ORDER BY + ROWS BETWEEN (Unbounded Preceding to Current Row).

    Test multiple cumulative aggregations.
    """
    df = make_df(
        {
            "store": ["A", "B", "A", "B", "A", "B"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "sales": [100, 150, 120, 160, 90, 140],
            "returns": [10, 15, 8, 12, 5, 10],
        }
    )

    window = Window.partition_by("store").order_by("date").rows_between(Window.unboundedPreceding, Window.currentRow)
    result = df.select(
        [
            col("store"),
            col("date"),
            col("sales"),
            col("returns"),
            # sum("sales").over(window).alias("running_sales"),
            # sum("returns").over(window).alias("running_returns"),
            # mean("sales").over(window).alias("avg_sales_to_date"),
            col("sales").sum().over(window).alias("running_sales"),
            col("returns").sum().over(window).alias("running_returns"),
            col("sales").mean().over(window).alias("avg_sales_to_date"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_global_cumulative(make_df):
    """Stage: Global Rank / ORDER BY Without PARTITION.

    Test cumulative calculations without partitioning.
    """
    df = make_df({"date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"], "sales": [250, 280, 230, 280]})

    window = Window.order_by("date").rows_between(Window.unboundedPreceding, Window.currentRow)
    result = df.select(
        [
            col("date"),
            col("sales"),
            # sum("sales").over(window).alias("total_sales_to_date"),
            # mean("sales").over(window).alias("avg_sales_to_date"),
            col("sales").sum().over(window).alias("total_sales_to_date"),
            col("sales").mean().over(window).alias("avg_sales_to_date"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_running_window_fixed_rows(make_df):
    """Stage: Complex Window Frames & Sliding Windows.

    Test running window with fixed number of preceding rows.
    """
    df = make_df(
        {
            "store": ["A", "B", "A", "B", "A", "B", "A", "B"],
            "date": [
                "2024-01-01",
                "2024-01-01",
                "2024-01-02",
                "2024-01-02",
                "2024-01-03",
                "2024-01-03",
                "2024-01-04",
                "2024-01-04",
            ],
            "sales": [100, 150, 120, 160, 90, 140, 110, 170],
        }
    )

    # 3-day moving average
    window = Window.partition_by("store").order_by("date").rows_between(-2, 0)  # 2 preceding rows and current row
    result = df.select(
        [
            col("store"),
            col("date"),
            col("sales"),
            # mean("sales").over(window).alias("moving_avg_3day"),
            col("sales").mean().over(window).alias("moving_avg_3day"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_running_window_mixed_bounds(make_df):
    """Stage: Complex Window Frames & Sliding Windows.

    Test running window with both preceding and following rows.
    """
    df = make_df(
        {
            "store": ["A", "B", "A", "B", "A", "B", "A", "B"],
            "date": [
                "2024-01-01",
                "2024-01-01",
                "2024-01-02",
                "2024-01-02",
                "2024-01-03",
                "2024-01-03",
                "2024-01-04",
                "2024-01-04",
            ],
            "sales": [100, 150, 120, 160, 90, 140, 110, 170],
        }
    )

    # Centered 3-day moving average
    window = Window.partition_by("store").order_by("date").rows_between(-1, 1)  # 1 preceding, current, and 1 following
    result = df.select(
        [
            col("store"),
            col("date"),
            col("sales"),
            # mean("sales").over(window).alias("centered_moving_avg"),
            col("sales").mean().over(window).alias("centered_moving_avg"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected
