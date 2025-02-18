from __future__ import annotations

import pytest

from daft import Window, col

# from daft.expressions import dense_rank, max, mean, min, rank, row_number, sum


@pytest.fixture
def global_data():
    return {
        # Unsorted data with ties
        "value": [10, 5, 15, 5, 10, 20, 8, 7],
        "date": [
            "2024-01-03",
            "2024-01-01",
            "2024-01-02",
            "2024-01-04",
            "2024-01-05",
            "2024-01-06",
            "2024-01-07",
            "2024-01-08",
        ],
        "quantity": [100, 50, 150, 50, 100, 200, 80, 70],
    }


def test_global_rank_basic(make_df):
    """Stage: Global Rank / ORDER BY Without PARTITION.

    Test basic global ranking without partitioning.
    """
    df = make_df({"value": [10, 5, 15, 5, 10, 20, 8, 7]})

    window = Window.order_by("value")
    result = df.select(
        col("value"),
        # rank().over(window).alias("rank"),
        # dense_rank().over(window).alias("dense_rank"),
        # row_number().over(window).alias("row_num"),
        col("value").rank().over(window).alias("rank"),
        col("value").dense_rank().over(window).alias("dense_rank"),
        col("value").row_number().over(window).alias("row_num"),
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_global_rank_desc(make_df):
    """Stage: Global Rank / ORDER BY Without PARTITION.

    Test global ranking in descending order.
    """
    df = make_df({"value": [10, 5, 15, 5, 10, 20, 8, 7]})

    window = Window.order_by("value", ascending=False)
    result = df.select(
        col("value"),
        # rank().over(window).alias("rank"),
        # dense_rank().over(window).alias("dense_rank"),
        # row_number().over(window).alias("row_num"),
        col("value").rank().over(window).alias("rank"),
        col("value").dense_rank().over(window).alias("dense_rank"),
        col("value").row_number().over(window).alias("row_num"),
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_global_running_total(make_df):
    """Stage: Global Rank / ORDER BY Without PARTITION.

    Test global running total without partitioning.
    """
    df = make_df({"date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"], "value": [100, 150, 120, 180]})

    window = Window.order_by("date").rows_between(Window.unbounded_preceding, Window.current_row)
    result = df.select(
        [
            col("date"),
            col("value"),
            # sum("value").over(window).alias("running_total"),
            # mean("value").over(window).alias("running_avg"),
            col("value").sum().over(window).alias("running_total"),
            col("value").mean().over(window).alias("running_avg"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_global_running_stats(make_df):
    """Stage: Global Rank / ORDER BY Without PARTITION.

    Test global running statistics without partitioning.
    """
    df = make_df({"date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"], "value": [100, 150, 120, 180]})

    window = Window.order_by("date").rows_between(Window.unbounded_preceding, Window.current_row)
    result = df.select(
        col("date"),
        col("value"),
        # min("value").over(window).alias("running_min"),
        # max("value").over(window).alias("running_max"),
        col("value").min().over(window).alias("running_min"),
        col("value").max().over(window).alias("running_max"),
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4, 8])
def test_global_rank_distributed(make_df, n_partitions):
    """Stage: Global Rank / ORDER BY Without PARTITION.

    Test global ranking with different numbers of partitions.
    """
    df = make_df({"value": [10, 5, 15, 5, 10, 20, 8, 7]}, repartition=n_partitions)

    window = Window.order_by("value")
    result = df.select(
        col("value"),
        # rank().over(window).alias("rank"),
        # dense_rank().over(window).alias("dense_rank"),
        # row_number().over(window).alias("row_num"),
        col("value").rank().over(window).alias("rank"),
        col("value").dense_rank().over(window).alias("dense_rank"),
        col("value").row_number().over(window).alias("row_num"),
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected
