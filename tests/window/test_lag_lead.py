from __future__ import annotations

import pytest

from daft import Window, col

# from daft.expressions import lag, lead


@pytest.fixture
def time_series_data():
    return {
        # Unsorted time series data with multiple stocks
        "stock": ["AAPL", "GOOGL", "AAPL", "GOOGL", "AAPL", "GOOGL", "AAPL", "GOOGL"],
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
        "price": [180.5, 140.2, 182.3, 141.5, 179.8, 142.1, 183.4, 143.2],
        "volume": [1000, 500, 1200, 600, 800, 450, 1500, 700],
    }


def test_lag_basic(make_df):
    """Stage: PARTITION BY + ORDER BY + LAG / LEAD.

    Test basic lag function with default offset.
    """
    df = make_df(
        {
            "stock": ["AAPL", "GOOGL", "AAPL", "GOOGL", "AAPL", "GOOGL"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "price": [180.5, 140.2, 182.3, 141.5, 179.8, 142.1],
        }
    )

    window = Window.partition_by("stock").order_by("date")
    result = df.select(
        [
            col("stock"),
            col("date"),
            col("price"),
            # lag("price").over(window).alias("prev_price"),
            col("price").lag().over(window).alias("prev_price"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_lead_basic(make_df):
    """Stage: PARTITION BY + ORDER BY + LAG / LEAD.

    Test basic lead function with default offset.
    """
    df = make_df(
        {
            "stock": ["AAPL", "GOOGL", "AAPL", "GOOGL", "AAPL", "GOOGL"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "price": [180.5, 140.2, 182.3, 141.5, 179.8, 142.1],
        }
    )

    window = Window.partition_by("stock").order_by("date")
    result = df.select(
        [
            col("stock"),
            col("date"),
            col("price"),
            # lead("price").over(window).alias("next_price"),
            col("price").lead().over(window).alias("next_price"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_lag_lead_custom_offset(make_df):
    """Stage: PARTITION BY + ORDER BY + LAG / LEAD.

    Test lag and lead with custom offsets.
    """
    df = make_df(
        {
            "stock": ["AAPL", "GOOGL", "AAPL", "GOOGL", "AAPL", "GOOGL", "AAPL", "GOOGL"],
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
            "price": [180.5, 140.2, 182.3, 141.5, 179.8, 142.1, 183.4, 143.2],
        }
    )

    window = Window.partition_by("stock").order_by("date")
    result = df.select(
        [
            col("stock"),
            col("date"),
            col("price"),
            # lag("price", offset=2).over(window).alias("price_2_days_ago"),
            # lead("price", offset=2).over(window).alias("price_2_days_ahead"),
            col("price").lag(offset=2).over(window).alias("price_2_days_ago"),
            col("price").lead(offset=2).over(window).alias("price_2_days_ahead"),
        ]
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_lag_lead_default_value(make_df):
    """Stage: PARTITION BY + ORDER BY + LAG / LEAD.

    Test lag and lead with custom default values.
    """
    df = make_df(
        {
            "stock": ["AAPL", "GOOGL", "AAPL", "GOOGL", "AAPL", "GOOGL"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "price": [180.5, 140.2, 182.3, 141.5, 179.8, 142.1],
        }
    )

    window = Window.partition_by("stock").order_by("date")
    result = df.select(
        col("stock"),
        col("date"),
        col("price"),
        # lag("price", default=0.0).over(window).alias("prev_price"),
        # lead("price", default=0.0).over(window).alias("next_price"),
        col("price").lag(default=0.0).over(window).alias("prev_price"),
        col("price").lead(default=0.0).over(window).alias("next_price"),
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected


def test_lag_lead_multiple_order_cols(make_df):
    """Stage: PARTITION BY + ORDER BY + LAG / LEAD.

    Test lag and lead with multiple ordering columns.
    """
    df = make_df(
        {
            "stock": ["AAPL", "GOOGL", "AAPL", "GOOGL", "AAPL", "GOOGL"],
            "date": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"],
            "time": ["09:30", "09:30", "09:30", "09:35", "09:30", "09:40"],
            "price": [180.5, 140.2, 182.3, 141.5, 179.8, 142.1],
        }
    )

    window = Window.partition_by("stock").order_by(["date", "time"])
    result = df.select(
        col("stock"),
        col("date"),
        col("time"),
        col("price"),
        # lag("price").over(window).alias("prev_price"),
        # lead("price").over(window).alias("next_price"),
        col("price").lag().over(window).alias("prev_price"),
        col("price").lead().over(window).alias("next_price"),
    )

    # TODO: Add expected output once implementation is ready
    expected = None
    assert result == expected
