from __future__ import annotations

import pytest

from daft import Window, col

# from daft.expressions import dense_rank, rank, row_number


@pytest.fixture
def rank_data():
    return {
        # Unsorted data with ties and multiple groups
        "category": ["B", "A", "C", "A", "B", "C", "A", "B"],
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
    }


def test_row_number_partition_order(make_df):
    """Stage: PARTITION BY + ORDER BY for Rank-Like Functions.

    Test row_number() with partition and order.
    """
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 5, 10, 20, 8, 7]})

    window = Window.partition_by("category").order_by("value")
    result = df.select(
        [
            col("category"),
            col("value"),
            # row_number().over(window).alias("row_num"),
            col("value").row_number().over(window).alias("row_num"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_rank_with_ties(make_df):
    """Stage: PARTITION BY + ORDER BY for Rank-Like Functions.

    Test rank() with ties in the ordering column.
    """
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 5, 10, 20, 8, 7]})

    window = Window.partition_by("category").order_by("value")
    result = df.select(
        [
            col("category"),
            col("value"),
            # rank().over(window).alias("rank"),
            # dense_rank().over(window).alias("dense_rank"),
            col("value").rank().over(window).alias("rank"),
            col("value").dense_rank().over(window).alias("dense_rank"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_rank_desc_order(make_df):
    """Stage: PARTITION BY + ORDER BY for Rank-Like Functions.

    Test ranking with descending order.
    """
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 5, 10, 20, 8, 7]})

    window = Window.partition_by("category").order_by("value", ascending=False)
    result = df.select(
        [
            col("category"),
            col("value"),
            # rank().over(window).alias("rank"),
            # dense_rank().over(window).alias("dense_rank"),
            col("value").rank().over(window).alias("rank"),
            col("value").dense_rank().over(window).alias("dense_rank"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_rank_multiple_order_cols(make_df):
    """Stage: PARTITION BY + ORDER BY for Rank-Like Functions.

    Test ranking with multiple ordering columns.
    """
    df = make_df(
        {
            "category": ["B", "A", "C", "A", "B", "C", "A", "B"],
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
        }
    )

    window = Window.partition_by("category").order_by(["value", "date"])
    result = df.select(
        [
            col("category"),
            col("value"),
            col("date"),
            # rank().over(window).alias("rank"),
            # dense_rank().over(window).alias("dense_rank"),
            col("value").rank().over(window).alias("rank"),
            col("value").dense_rank().over(window).alias("dense_rank"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_global_rank(make_df):
    """Stage: Global Rank / ORDER BY Without PARTITION.

    Test ranking without partitioning (global ranking).
    """
    df = make_df({"value": [10, 5, 15, 5, 10, 20, 8, 7]})

    window = Window.order_by("value")
    result = df.select(
        [
            col("value"),
            # rank().over(window).alias("rank"),
            # dense_rank().over(window).alias("dense_rank"),
            # row_number().over(window).alias("row_num"),
            col("value").rank().over(window).alias("rank"),
            col("value").dense_rank().over(window).alias("dense_rank"),
            col("value").row_number().over(window).alias("row_num"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected
