"""Tests for point-in-time (asof) joins."""

from __future__ import annotations

import pytest

import daft


def _vals(df, col):
    """Get column values from a collected DataFrame."""
    return df.to_pydict()[col]


def test_backward_basic_allow_exact_false():
    """With allow_exact_matches=False, probe value must be STRICTLY GREATER."""
    left = daft.from_pydict({"time": [1, 2, 4, 7, 10, 15], "val_l": [10, 20, 40, 50, 60, 70]})
    right = daft.from_pydict({"time": [1, 2, 3, 6, 9], "val_r": [100, 200, 300, 700, 900]})

    result = left.asof_join(right, left_on="time", right_on="time", left_by=[], right_by=[]).sort("time").collect()

    assert _vals(result, "val_r") == [None, 100, 300, 700, 900, 900]


def test_backward_basic_allow_exact_true():
    """With allow_exact_matches=True, probe value may be EQUAL to build value."""
    left = daft.from_pydict({"time": [1, 2, 4, 7, 10, 15], "val_l": [10, 20, 40, 50, 60, 70]})
    right = daft.from_pydict({"time": [1, 2, 3, 7, 9], "val_r": [100, 200, 300, 700, 900]})

    result = (
        left.asof_join(
            right,
            left_on="time",
            right_on="time",
            left_by=[],
            right_by=[],
            allow_exact_matches=True,
        )
        .sort("time")
        .collect()
    )

    assert _vals(result, "val_r") == [100, 200, 300, 700, 900, 900]


def test_backward_basic_with_nulls():
    """With allow_exact_matches=False, probe value must be STRICTLY GREATER."""
    left = daft.from_pydict({"time": [1, 2, 4, 7, None, 15], "val_l": [10, 20, 40, 50, 60, 70]})
    right = daft.from_pydict({"time": [1, 2, 3, 6, 9], "val_r": [100, 200, 300, 700, 900]})

    result = left.asof_join(right, left_on="time", right_on="time", left_by=[], right_by=[]).sort("time").collect()
    print(result)
    assert _vals(result, "val_r") == [None, 100, 300, 700, 900, None]


def test_backward_null_right_on_key():
    """Null on-key on the right side is never a valid match — right nulls sort last and are skipped."""
    left = daft.from_pydict({"time": [5, 10, 15], "val_l": [1, 2, 3]})
    # right has a null time; only time=3 and time=8 are valid candidates
    right = daft.from_pydict({"time": [3, None, 8], "val_r": [100, 999, 200]})

    result = left.asof_join(right, on="time", allow_exact_matches=True).sort("time").collect()

    # time=5  → right time=3 (val_r=100); null is skipped
    # time=10 → right time=8 (val_r=200); null is skipped
    # time=15 → right time=8 (val_r=200); null is skipped
    assert _vals(result, "val_r") == [100, 200, 200]


def test_grouped_backward_with_nulls():
    """Grouped asof join, strict inequality, two groups."""
    left = daft.from_pydict({"id": [0, 0, None, 1], "time": [1, 4, 2, 5], "val_l": [10, 40, 20, 50]})
    right = daft.from_pydict({"id": [0, 0, 1, 1], "time": [2, 3, 1, 4], "val_r": [100, 200, 300, 400]})

    result = (
        left.asof_join(right, left_on="time", right_on="time", left_by="id", right_by="id")
        .sort(["id", "time"])
        .collect()
    )

    # id=0: right times {2,3}. time=1 → None (1 not > 2). time=4 → 200 (3 < 4).
    # id=1: right times {1,4}. time=2 → 300 (1 < 2). time=5 → 400 (4 < 5).
    assert _vals(result, "val_r") == [None, 200, 400, None]


def test_grouped_backward_exact_false():
    """Grouped asof join, strict inequality, two groups."""
    left = daft.from_pydict({"id": [0, 0, 1, 1], "time": [1, 4, 2, 5], "val_l": [10, 40, 20, 50]})
    right = daft.from_pydict({"id": [0, 0, 1, 1], "time": [2, 3, 1, 4], "val_r": [100, 200, 300, 400]})

    result = (
        left.asof_join(right, left_on="time", right_on="time", left_by="id", right_by="id")
        .sort(["id", "time"])
        .collect()
    )

    # id=0: right times {2,3}. time=1 → None (1 not > 2). time=4 → 200 (3 < 4).
    # id=1: right times {1,4}. time=2 → 300 (1 < 2). time=5 → 400 (4 < 5).
    assert _vals(result, "val_r") == [None, 200, 300, 400]


def test_grouped_backward_exact_true():
    """Grouped asof join, allow exact, two groups."""
    left = daft.from_pydict({"id": [0, 0, 1, 1], "time": [1, 4, 2, 5], "val_l": [10, 40, 20, 50]})
    right = daft.from_pydict({"id": [0, 0, 1, 1], "time": [2, 3, 1, 4], "val_r": [100, 200, 300, 400]})

    result = (
        left.asof_join(
            right,
            left_on="time",
            right_on="time",
            left_by="id",
            right_by="id",
            allow_exact_matches=True,
        )
        .sort(["id", "time"])
        .collect()
    )

    # id=0: right times {2,3}. time=1 → None. time=4 → 200 (3≤4).
    # id=1: right times {1,4}. time=2 → 300 (1≤2). time=5 → 400 (4≤5).
    assert _vals(result, "val_r") == [None, 200, 300, 400]


def test_no_match_for_group():
    """Build side exists but has no matching group key for some probe rows."""
    left = daft.from_pydict({"id": [0, 1], "time": [5, 5], "val_l": [10, 20]})
    right = daft.from_pydict({"id": [0], "time": [3], "val_r": [100]})

    result = (
        left.asof_join(
            right,
            left_on="time",
            right_on="time",
            left_by="id",
            right_by="id",
            allow_exact_matches=True,
        )
        .sort(["id", "time"])
        .collect()
    )

    # id=0 → matches val_r=100; id=1 → no match → None
    assert _vals(result, "val_r") == [100, None]


def test_all_probe_before_build():
    """All probe times are before all build times → all nulls."""
    left = daft.from_pydict({"id": [0, 0], "time": [1, 2], "val_l": [10, 20]})
    right = daft.from_pydict({"id": [0, 0], "time": [5, 6], "val_r": [100, 200]})

    result = (
        left.asof_join(
            right,
            left_on="time",
            right_on="time",
            left_by="id",
            right_by="id",
            allow_exact_matches=True,
        )
        .sort("time")
        .collect()
    )

    assert _vals(result, "val_r") == [None, None]


def test_exact_match_boundary():
    """Probe time exactly equals min/max build time."""
    left = daft.from_pydict({"id": [0, 0, 0], "time": [1, 5, 10], "val_l": [0, 0, 0]})
    right = daft.from_pydict({"id": [0, 0, 0], "time": [1, 5, 10], "val_r": [10, 50, 100]})

    # allow_exact_matches=True → exact match at each boundary
    result_exact = (
        left.asof_join(
            right,
            left_on="time",
            right_on="time",
            left_by="id",
            right_by="id",
            allow_exact_matches=True,
        )
        .sort("time")
        .collect()
    )
    assert _vals(result_exact, "val_r") == [10, 50, 100]

    # allow_exact_matches=False → exact matches become nulls/predecessor
    result_strict = (
        left.asof_join(
            right,
            left_on="time",
            right_on="time",
            left_by="id",
            right_by="id",
            allow_exact_matches=False,
        )
        .sort("time")
        .collect()
    )
    # time=1: no build < 1 → None
    # time=5: build times < 5 → {1} → val_r=10
    # time=10: build times < 10 → {1,5} → val_r=50
    assert _vals(result_strict, "val_r") == [None, 10, 50]


def test_multiple_build_rows_same_time():
    """Multiple right rows with same timestamp — any valid one is acceptable."""
    left = daft.from_pydict({"id": [0], "time": [5], "val_l": [0]})
    right = daft.from_pydict({"id": [0, 0, 0], "time": [3, 3, 3], "val_r": [10, 20, 30]})

    result = left.asof_join(
        right,
        left_on="time",
        right_on="time",
        left_by="id",
        right_by="id",
        allow_exact_matches=True,
    ).collect()

    # val_r must be one of {10, 20, 30}
    val = _vals(result, "val_r")[0]
    assert val in {10, 20, 30}, f"Expected one of {{10, 20, 30}}, got {val}"


def test_single_row_each():
    """Single row on each side."""
    left = daft.from_pydict({"id": [0], "time": [10], "val_l": [99]})
    right = daft.from_pydict({"id": [0], "time": [5], "val_r": [42]})

    result = left.asof_join(
        right,
        left_on="time",
        right_on="time",
        left_by="id",
        right_by="id",
        allow_exact_matches=True,
    ).collect()

    assert _vals(result, "val_r") == [42]


def test_suffix_rename():
    """When both sides have overlapping non-join columns, suffix is applied."""
    left = daft.from_pydict({"id": [0], "time": [5], "value": [1]})
    right = daft.from_pydict({"id": [0], "time": [3], "value": [9]})

    result = left.asof_join(
        right,
        left_on="time",
        right_on="time",
        left_by="id",
        right_by="id",
        allow_exact_matches=True,
        suffix="_right",
    ).collect()

    assert "value" in result.column_names
    assert "value_right" in result.column_names


def test_date_asof_join():
    """Asof join on date columns (daily snapshots vs events)."""
    from datetime import date

    events = daft.from_pydict(
        {
            "user": ["alice", "alice", "bob", "bob"],
            "event_date": [
                date(2024, 3, 5),
                date(2024, 3, 15),
                date(2024, 3, 10),
                date(2024, 3, 20),
            ],
            "action": ["click", "purchase", "click", "purchase"],
        }
    )

    snapshots = daft.from_pydict(
        {
            "user": ["alice", "alice", "alice", "bob", "bob", "bob"],
            "snapshot_date": [
                date(2024, 3, 1),
                date(2024, 3, 10),
                date(2024, 3, 20),
                date(2024, 3, 1),
                date(2024, 3, 8),
                date(2024, 3, 18),
            ],
            "score": [10, 20, 30, 100, 200, 300],
        }
    )

    result = (
        events.asof_join(
            snapshots,
            left_on="event_date",
            right_on="snapshot_date",
            left_by="user",
            right_by="user",
            allow_exact_matches=True,
        )
        .sort(["user", "event_date"])
        .collect()
    )

    # alice:
    #   2024-03-05 → snapshot 2024-03-01 (score=10)
    #   2024-03-15 → snapshot 2024-03-10 (score=20)
    # bob:
    #   2024-03-10 → snapshot 2024-03-08 (score=200)
    #   2024-03-20 → snapshot 2024-03-18 (score=300)
    assert _vals(result, "score") == [10, 20, 200, 300]


def test_timestamp_asof_join():
    """Asof join on actual timestamp columns (simulating trades vs quotes)."""
    from datetime import datetime

    trades = daft.from_pydict(
        {
            "symbol": ["AAPL", "AAPL", "GOOG", "GOOG", "AAPL"],
            "timestamp": [
                datetime(2024, 1, 1, 9, 30, 0),
                datetime(2024, 1, 1, 9, 30, 5),
                datetime(2024, 1, 1, 9, 30, 2),
                datetime(2024, 1, 1, 9, 30, 7),
                datetime(2024, 1, 1, 9, 30, 10),
            ],
            "price": [150.0, 151.0, 2800.0, 2810.0, 152.0],
        }
    )

    quotes = daft.from_pydict(
        {
            "symbol": ["AAPL", "AAPL", "AAPL", "GOOG", "GOOG", "GOOG"],
            "timestamp": [
                datetime(2024, 1, 1, 9, 29, 58),
                datetime(2024, 1, 1, 9, 30, 3),
                datetime(2024, 1, 1, 9, 30, 8),
                datetime(2024, 1, 1, 9, 30, 0),
                datetime(2024, 1, 1, 9, 30, 4),
                datetime(2024, 1, 1, 9, 30, 9),
            ],
            "bid": [149.5, 150.5, 151.5, 2799.0, 2805.0, 2811.0],
        }
    )

    result = (
        trades.asof_join(
            quotes,
            left_on="timestamp",
            right_on="timestamp",
            left_by="symbol",
            right_by="symbol",
            allow_exact_matches=True,
        )
        .sort(["symbol", "timestamp"])
        .collect()
    )

    # AAPL trades:
    #   09:30:00 → quote 09:29:58 (bid=149.5)
    #   09:30:05 → quote 09:30:03 (bid=150.5)
    #   09:30:10 → quote 09:30:08 (bid=151.5)
    # GOOG trades:
    #   09:30:02 → quote 09:30:00 (bid=2799.0)
    #   09:30:07 → quote 09:30:04 (bid=2805.0)
    assert _vals(result, "bid") == [149.5, 150.5, 151.5, 2799.0, 2805.0]


def _pandas_asof(left_pd, right_pd, on, by, allow_exact_matches):
    """Ground truth using pandas merge_asof.

    pandas merge_asof requires both sides sorted by the 'on' column.
    """
    import pandas as pd

    # pandas merge_asof requires 'on' to be non-decreasing (sorted ascending).
    left_sorted = left_pd.sort_values(on).reset_index(drop=True)
    right_sorted = right_pd.sort_values(on).reset_index(drop=True)

    return pd.merge_asof(
        left_sorted,
        right_sorted,
        on=on,
        by=by,
        direction="backward",
        allow_exact_matches=allow_exact_matches,
    )


@pytest.mark.parametrize("allow_exact_matches", [True, False])
@pytest.mark.parametrize("n_groups", [1, 3, 5])
@pytest.mark.parametrize("seed", [0, 1, 2, 42])
def test_against_pandas(allow_exact_matches, n_groups, seed):
    """Compare Daft asof_join against pandas merge_asof on random data.

    Right-side timestamps are unique per group to avoid tie-breaking ambiguity
    between Daft and pandas (both are correct for ties, just not identical).
    """
    import random

    import pandas as pd

    rng = random.Random(seed * 1000 + n_groups * 100 + int(allow_exact_matches))

    n_left = 30
    # Use enough possible times so unique sampling is always possible.
    max_time = 200

    ids_l = [rng.randint(0, n_groups - 1) for _ in range(n_left)]
    times_l = [rng.randint(1, max_time) for _ in range(n_left)]
    vals_l = list(range(n_left))

    # Build right side: unique timestamps per group to avoid tie-breaking ambiguity.
    right_rows = []
    val_r_counter = 0
    for g in range(n_groups):
        n_right_per_group = rng.randint(5, 15)
        times_for_group = rng.sample(range(1, max_time + 1), n_right_per_group)
        for t in times_for_group:
            right_rows.append({"id": g, "time": t, "val_r": val_r_counter})
            val_r_counter += 1

    left_pd = pd.DataFrame({"id": ids_l, "time": times_l, "val_l": vals_l})
    right_pd = pd.DataFrame(right_rows)

    # pandas ground truth
    expected = _pandas_asof(left_pd, right_pd, on="time", by="id", allow_exact_matches=allow_exact_matches)
    expected = expected.sort_values(["id", "time", "val_l"]).reset_index(drop=True)

    # Daft result
    left_daft = daft.from_pandas(left_pd)
    right_daft = daft.from_pandas(right_pd)

    result_daft = (
        left_daft.asof_join(
            right_daft,
            left_on="time",
            right_on="time",
            left_by="id",
            right_by="id",
            allow_exact_matches=allow_exact_matches,
        )
        .sort(["id", "time", "val_l"])
        .collect()
        .to_pandas()
        .reset_index(drop=True)
    )

    pd.testing.assert_series_equal(
        result_daft["val_r"].astype("float64"),
        expected["val_r"].astype("float64"),
        check_names=False,
        check_like=False,
    )
