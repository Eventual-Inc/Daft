"""Tests for Spark-compatible window functions added by feat/spark-window-functions.

Covers:
- cume_dist, percent_rank, ntile  (partition + order_by, no frame)
- first_value, last_value, nth_value  (frame-bounded window aggregates)
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

from daft import Window, col
from daft.functions import (
    cume_dist,
    first_value,
    last_value,
    nth_value,
    ntile,
    percent_rank,
)
from tests.conftest import assert_df_equals

# ---------------------------------------------------------------------------
# cume_dist
# ---------------------------------------------------------------------------


def test_cume_dist_partitioned(make_df):
    """cume_dist over partition + order_by; ties share the same value."""
    df = make_df(
        {
            "category": ["A", "A", "A", "A", "B", "B", "B"],
            "value": [1, 2, 2, 3, 10, 20, 20],
        }
    )

    window = Window().partition_by("category").order_by("value")
    result = df.select(
        col("category"),
        col("value"),
        cume_dist().over(window).alias("cd"),
    ).collect()

    # Partition A (n=4): peers
    #   value=1 -> 1/4 = 0.25
    #   value=2 (twice, peers) -> 3/4 = 0.75
    #   value=3 -> 4/4 = 1.0
    # Partition B (n=3):
    #   value=10 -> 1/3
    #   value=20 (twice, peers) -> 3/3 = 1.0
    expected = {
        "category": ["A", "A", "A", "A", "B", "B", "B"],
        "value": [1, 2, 2, 3, 10, 20, 20],
        "cd": [0.25, 0.75, 0.75, 1.0, 1 / 3, 1.0, 1.0],
    }
    assert_df_equals(
        result.to_pandas(),
        pd.DataFrame(expected),
        sort_key=["category", "value", "cd"],
        check_dtype=False,
    )


def test_cume_dist_order_by_only(make_df):
    """cume_dist with only ORDER BY (no PARTITION BY)."""
    df = make_df({"value": [1, 1, 2, 3]})
    window = Window().order_by("value")
    result = df.select(col("value"), cume_dist().over(window).alias("cd")).collect()
    pdf = result.to_pandas()
    by_value = dict(zip(pdf["value"].tolist(), pdf["cd"].tolist()))
    # Two rows with value=1 are peers -> 2/4 = 0.5; value=2 -> 3/4; value=3 -> 1.0.
    assert math.isclose(by_value[1], 0.5)
    assert math.isclose(by_value[2], 0.75)
    assert math.isclose(by_value[3], 1.0)


# ---------------------------------------------------------------------------
# percent_rank
# ---------------------------------------------------------------------------


def test_percent_rank_partitioned(make_df):
    """percent_rank uses gap-rank semantics: (rank - 1) / (n - 1)."""
    df = make_df(
        {
            "category": ["A", "A", "A", "A"],
            "value": [10, 20, 20, 30],
        }
    )

    window = Window().partition_by("category").order_by("value")
    result = df.select(
        col("category"),
        col("value"),
        percent_rank().over(window).alias("pr"),
    ).collect()

    # ranks: 10->1, 20->2, 20->2, 30->4. denominator = 3.
    expected = {
        "category": ["A", "A", "A", "A"],
        "value": [10, 20, 20, 30],
        "pr": [0.0, 1 / 3, 1 / 3, 1.0],
    }
    assert_df_equals(
        result.to_pandas(),
        pd.DataFrame(expected),
        sort_key=["category", "value", "pr"],
        check_dtype=False,
    )


def test_percent_rank_singleton_partition(make_df):
    """A single-row partition yields percent_rank = 0.0."""
    df = make_df({"category": ["A", "B"], "value": [1, 5]})
    window = Window().partition_by("category").order_by("value")
    result = df.select(
        col("category"),
        percent_rank().over(window).alias("pr"),
    ).collect()
    pr_values = result.to_pandas()["pr"].tolist()
    assert all(math.isclose(v, 0.0) for v in pr_values)


# ---------------------------------------------------------------------------
# ntile
# ---------------------------------------------------------------------------


def test_ntile_even_split(make_df):
    """Ntile splits the partition into n equal-size buckets when divisible."""
    df = make_df(
        {
            "category": ["A"] * 6,
            "value": [1, 2, 3, 4, 5, 6],
        }
    )
    window = Window().partition_by("category").order_by("value")
    result = df.select(
        col("value"),
        ntile(3).over(window).alias("bucket"),
    ).collect()
    pdf = result.to_pandas().sort_values("value")
    assert pdf["bucket"].tolist() == [1, 1, 2, 2, 3, 3]


def test_ntile_uneven_split_first_buckets_get_extra(make_df):
    """When partition_size % n != 0, the FIRST `extra` buckets get +1 row."""
    df = make_df(
        {
            "category": ["A"] * 7,
            "value": [1, 2, 3, 4, 5, 6, 7],
        }
    )
    window = Window().partition_by("category").order_by("value")
    result = df.select(
        col("value"),
        ntile(3).over(window).alias("bucket"),
    ).collect()
    pdf = result.to_pandas().sort_values("value")
    # 7 rows, 3 buckets: extra=1 -> sizes [3, 2, 2].
    assert pdf["bucket"].tolist() == [1, 1, 1, 2, 2, 3, 3]


def test_ntile_more_buckets_than_rows(make_df):
    """When n > partition_size, the first partition_size buckets get one row each."""
    df = make_df({"category": ["A"] * 3, "value": [1, 2, 3]})
    window = Window().partition_by("category").order_by("value")
    result = df.select(
        col("value"),
        ntile(5).over(window).alias("bucket"),
    ).collect()
    pdf = result.to_pandas().sort_values("value")
    # 3 rows / 5 buckets: base_size=0, extra=3 -> sizes [1,1,1,0,0]; only buckets 1..3 are filled.
    assert pdf["bucket"].tolist() == [1, 2, 3]


def test_ntile_invalid_n_raises():
    import daft

    df = daft.from_pydict({"v": [1, 2, 3]})
    with pytest.raises(ValueError):
        ntile(0)
    with pytest.raises(ValueError):
        ntile(-1)
    # Sanity: the dataframe import was used.
    assert df.count_rows() == 3


# ---------------------------------------------------------------------------
# first_value / last_value / nth_value (window frame)
# ---------------------------------------------------------------------------


def test_nth_value_unbounded_frame(make_df):
    """nth_value over an unbounded frame returns the global N-th value (per partition)."""
    df = make_df(
        {
            "category": ["A", "A", "A", "A", "B", "B", "B"],
            "time": [1, 2, 3, 4, 1, 2, 3],
            "value": [10, 20, 30, 40, 100, 200, 300],
        }
    )

    import daft

    window = (
        Window()
        .partition_by("category")
        .order_by("time")
        .rows_between(daft.Window.unbounded_preceding, daft.Window.unbounded_following)
    )
    result = df.select(
        col("category"),
        col("time"),
        nth_value(col("value"), 2).over(window).alias("n2"),
    ).collect()

    pdf = result.to_pandas().sort_values(["category", "time"])
    # Partition A: 2nd value is 20; Partition B: 2nd value is 200.
    assert pdf.loc[pdf["category"] == "A", "n2"].unique().tolist() == [20]
    assert pdf.loc[pdf["category"] == "B", "n2"].unique().tolist() == [200]


def test_nth_value_returns_null_when_frame_too_small(make_df):
    """nth_value returns NULL when fewer than n rows are available in the frame."""
    df = make_df(
        {
            "category": ["A", "A"],
            "time": [1, 2],
            "value": [10, 20],
        }
    )

    import daft

    # Frame is just the current row: only 1 element, so nth_value(_, 2) is NULL.
    window = (
        Window()
        .partition_by("category")
        .order_by("time")
        .rows_between(daft.Window.current_row, daft.Window.current_row)
    )
    result = df.select(
        col("time"),
        nth_value(col("value"), 2).over(window).alias("n2"),
    ).collect()
    assert result.to_pandas()["n2"].isna().all()


def test_nth_value_ignore_nulls(make_df):
    """nth_value(ignore_nulls=True) skips nulls inside the frame."""
    df = make_df(
        {
            "category": ["A", "A", "A", "A"],
            "time": [1, 2, 3, 4],
            "value": [None, 100, None, 200],
        }
    )

    import daft

    window = (
        Window()
        .partition_by("category")
        .order_by("time")
        .rows_between(daft.Window.unbounded_preceding, daft.Window.unbounded_following)
    )
    result = df.select(
        col("time"),
        nth_value(col("value"), 2, ignore_nulls=True).over(window).alias("n2"),
    ).collect()
    # 2nd non-null value is 200.
    assert result.to_pandas()["n2"].dropna().unique().tolist() == [200]


def test_first_value_window_unchanged_behavior(make_df):
    """Sanity check: first_value over an unbounded frame still returns the leading value."""
    df = make_df(
        {
            "category": ["A", "A", "A"],
            "time": [1, 2, 3],
            "value": [10, 20, 30],
        }
    )

    import daft

    window = (
        Window()
        .partition_by("category")
        .order_by("time")
        .rows_between(daft.Window.unbounded_preceding, daft.Window.unbounded_following)
    )
    result = df.select(
        col("time"),
        first_value(col("value")).over(window).alias("fv"),
        last_value(col("value")).over(window).alias("lv"),
    ).collect()
    pdf = result.to_pandas()
    assert pdf["fv"].unique().tolist() == [10]
    assert pdf["lv"].unique().tolist() == [30]


def test_nth_value_invalid_n_raises():
    with pytest.raises(ValueError):
        nth_value(col("v"), 0)
    with pytest.raises(ValueError):
        nth_value(col("v"), -3)
