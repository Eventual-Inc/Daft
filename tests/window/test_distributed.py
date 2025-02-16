from __future__ import annotations

import pytest

from daft import Window, col
from daft.expressions import count, dense_rank, mean, rank, sum


@pytest.fixture
def skewed_data():
    # Create skewed data where one partition is much larger than others
    n_rows = 1000
    return {
        "category": ["A"] * 800 + ["B"] * 150 + ["C"] * 50,  # Highly skewed distribution
        "value": list(range(n_rows)),
    }


@pytest.mark.parametrize("n_partitions", [1, 2, 4, 8])
def test_partition_only_distributed(make_df, n_partitions):
    """Stage: PARTITION BY-Only Window Aggregations.

    Test partition-only aggregations with different numbers of partitions.
    """
    df = make_df(
        {"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]},
        repartition=n_partitions,
    )

    window = Window.partition_by("category")
    result = df.select(
        [
            col("category"),
            col("value"),
            sum("value").over(window).alias("sum"),
            mean("value").over(window).alias("avg"),
            count("value").over(window).alias("count"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4, 8])
def test_rank_distributed(make_df, n_partitions):
    """Stage: PARTITION BY + ORDER BY for Rank-Like Functions.

    Test ranking functions with different numbers of partitions.
    """
    df = make_df(
        {"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 5, 10, 20, 8, 7]},
        repartition=n_partitions,
    )

    window = Window.partition_by("category").order_by("value")
    result = df.select(
        [
            col("category"),
            col("value"),
            rank().over(window).alias("rank"),
            dense_rank().over(window).alias("dense_rank"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


@pytest.mark.parametrize("n_partitions", [1, 2, 4, 8])
def test_running_window_distributed(make_df, n_partitions):
    """Stage: PARTITION BY + ORDER BY + ROWS BETWEEN (Unbounded Preceding to Current Row).

    Test running windows with different numbers of partitions.
    """
    df = make_df(
        {
            "store": ["A", "B", "A", "B", "A", "B"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "sales": [100, 150, 120, 160, 90, 140],
        },
        repartition=n_partitions,
    )

    window = Window.partition_by("store").order_by("date").rows_between(Window.unboundedPreceding, Window.currentRow)
    result = df.select([col("store"), col("date"), col("sales"), sum("sales").over(window).alias("running_total")])

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_skewed_partition_only(make_df, skewed_data):
    """Stage: PARTITION BY-Only Window Aggregations.

    Test partition-only aggregations with highly skewed data.
    """
    df = make_df(skewed_data)

    window = Window.partition_by("category")
    result = df.select(
        [
            col("category"),
            col("value"),
            sum("value").over(window).alias("sum"),
            mean("value").over(window).alias("avg"),
            count("value").over(window).alias("count"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_skewed_rank(make_df, skewed_data):
    """Stage: PARTITION BY + ORDER BY for Rank-Like Functions.

    Test ranking functions with highly skewed data.
    """
    df = make_df(skewed_data)

    window = Window.partition_by("category").order_by("value")
    result = df.select(
        [
            col("category"),
            col("value"),
            rank().over(window).alias("rank"),
            dense_rank().over(window).alias("dense_rank"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_skewed_running_window(make_df):
    """Stage: PARTITION BY + ORDER BY + ROWS BETWEEN (Unbounded Preceding to Current Row).

    Test running windows with highly skewed data.
    """
    # Create time series data with skewed distribution
    df = make_df(
        {
            "store": ["A"] * 800 + ["B"] * 150 + ["C"] * 50,
            "date": [f"2024-01-{str(i+1).zfill(2)}" for i in range(1000)],
            "sales": list(range(1000)),
        }
    )

    window = Window.partition_by("store").order_by("date").rows_between(Window.unboundedPreceding, Window.currentRow)
    result = df.select([col("store"), col("date"), col("sales"), sum("sales").over(window).alias("running_total")])

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected
