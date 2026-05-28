from __future__ import annotations

import pytest

import daft
from daft import Window, col
from daft.exceptions import DaftCoreException
from daft.functions import first_value, last_value

T = list(range(1, 8))  # [1, 2, 3, 4, 5, 6, 7]

# partition_by is required — all window specs include a partition column.
W_FORWARD = Window().partition_by("g").order_by("t").rows_between(Window.unbounded_preceding, Window.current_row)
W_BACKWARD = Window().partition_by("g").order_by("t").rows_between(Window.current_row, Window.unbounded_following)


def sorted_col(df: daft.DataFrame, col_name: str) -> list:
    return df.sort("t").collect().to_pydict()[col_name]


def make_df(values: list) -> daft.DataFrame:
    return daft.from_pydict({"t": T, "g": ["A"] * len(T), "v": values})


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION TESTS
# ══════════════════════════════════════════════════════════════════════════════

# ── 1. Error: rejected in aggregation context (window-only functions) ──────────


def test_last_value_rejected_in_global_agg():
    df = daft.from_pydict({"x": [1, 2, 3]})
    with pytest.raises(DaftCoreException, match="Expressions in aggregations must be composed of non-nested aggregation expressions"):
        df.agg(col("x").last_value())


def test_last_value_standalone_rejected_in_global_agg():
    df = daft.from_pydict({"x": [1, 2, 3]})
    with pytest.raises(DaftCoreException, match="Expressions in aggregations must be composed of non-nested aggregation expressions"):
        df.agg(last_value(col("x")))


def test_first_value_rejected_in_global_agg():
    df = daft.from_pydict({"x": [1, 2, 3]})
    with pytest.raises(DaftCoreException, match="Expressions in aggregations must be composed of non-nested aggregation expressions"):
        df.agg(col("x").first_value())


def test_first_value_standalone_rejected_in_global_agg():
    df = daft.from_pydict({"x": [1, 2, 3]})
    with pytest.raises(DaftCoreException, match="Expressions in aggregations must be composed of non-nested aggregation expressions"):
        df.agg(first_value(col("x")))


def test_last_value_rejected_in_groupby_agg():
    df = daft.from_pydict({"x": [1, 2, 3], "g": ["a", "b", "a"]})
    with pytest.raises(DaftCoreException, match="Expressions in aggregations must be composed of non-nested aggregation expressions"):
        df.groupby("g").agg(col("x").last_value())


def test_last_value_standalone_rejected_in_groupby_agg():
    df = daft.from_pydict({"x": [1, 2, 3], "g": ["a", "b", "a"]})
    with pytest.raises(DaftCoreException, match="Expressions in aggregations must be composed of non-nested aggregation expressions"):
        df.groupby("g").agg(last_value(col("x")))


def test_first_value_rejected_in_groupby_agg():
    df = daft.from_pydict({"x": [1, 2, 3], "g": ["a", "b", "a"]})
    with pytest.raises(DaftCoreException, match="Expressions in aggregations must be composed of non-nested aggregation expressions"):
        df.groupby("g").agg(col("x").first_value())


def test_first_value_standalone_rejected_in_groupby_agg():
    df = daft.from_pydict({"x": [1, 2, 3], "g": ["a", "b", "a"]})
    with pytest.raises(DaftCoreException, match="Expressions in aggregations must be composed of non-nested aggregation expressions"):
        df.groupby("g").agg(first_value(col("x")))


# ── 2. Error: partition_by required ───────────────────────────────────────────


def test_last_value_requires_partition_by():
    df = daft.from_pydict({"t": [1, 2, 3], "v": [10, 20, 30]})
    w = Window().order_by("t").rows_between(Window.unbounded_preceding, Window.current_row)
    with pytest.raises(DaftCoreException, match="require a partition_by"):
        df.with_column("out", col("v").last_value(ignore_nulls=True).over(w)).collect()


def test_last_value_standalone_requires_partition_by():
    df = daft.from_pydict({"t": [1, 2, 3], "v": [10, 20, 30]})
    w = Window().order_by("t").rows_between(Window.unbounded_preceding, Window.current_row)
    with pytest.raises(DaftCoreException, match="require a partition_by"):
        df.with_column("out", last_value(col("v"), ignore_nulls=True).over(w)).collect()


def test_first_value_requires_partition_by():
    df = daft.from_pydict({"t": [1, 2, 3], "v": [10, 20, 30]})
    w = Window().order_by("t").rows_between(Window.current_row, Window.unbounded_following)
    with pytest.raises(DaftCoreException, match="require a partition_by"):
        df.with_column("out", col("v").first_value(ignore_nulls=True).over(w)).collect()


def test_first_value_standalone_requires_partition_by():
    df = daft.from_pydict({"t": [1, 2, 3], "v": [10, 20, 30]})
    w = Window().order_by("t").rows_between(Window.current_row, Window.unbounded_following)
    with pytest.raises(DaftCoreException, match="require a partition_by"):
        df.with_column("out", first_value(col("v"), ignore_nulls=True).over(w)).collect()


# ── 3. Error: order_by required ───────────────────────────────────────────────


def test_last_value_requires_order_by():
    df = daft.from_pydict({"group": ["A", "A", "B"], "v": [1, 2, 3]})
    w = Window().partition_by("group")  # no order_by, no frame
    with pytest.raises(DaftCoreException, match="require an order_by"):
        df.with_column("out", col("v").last_value(ignore_nulls=True).over(w)).collect()


def test_last_value_standalone_requires_order_by():
    df = daft.from_pydict({"group": ["A", "A", "B"], "v": [1, 2, 3]})
    w = Window().partition_by("group")
    with pytest.raises(DaftCoreException, match="require an order_by"):
        df.with_column("out", last_value(col("v"), ignore_nulls=True).over(w)).collect()


def test_first_value_requires_order_by():
    df = daft.from_pydict({"group": ["A", "A", "B"], "v": [1, 2, 3]})
    w = Window().partition_by("group")  # no order_by, no frame
    with pytest.raises(DaftCoreException, match="require an order_by"):
        df.with_column("out", col("v").first_value(ignore_nulls=True).over(w)).collect()


def test_first_value_standalone_requires_order_by():
    df = daft.from_pydict({"group": ["A", "A", "B"], "v": [1, 2, 3]})
    w = Window().partition_by("group")
    with pytest.raises(DaftCoreException, match="require an order_by"):
        df.with_column("out", first_value(col("v"), ignore_nulls=True).over(w)).collect()


# ── 4. Error: frame required (order_by present but no rows_between) ───────────


def test_last_value_requires_frame():
    df = daft.from_pydict({"x": [1, 2, 3], "g": ["a", "b", "a"]})
    w = Window().partition_by("g").order_by("x")  # order_by present but no frame
    with pytest.raises(DaftCoreException, match="require a frame"):
        df.with_column("out", col("x").last_value().over(w)).collect()


def test_last_value_standalone_requires_frame():
    df = daft.from_pydict({"x": [1, 2, 3], "g": ["a", "b", "a"]})
    w = Window().partition_by("g").order_by("x")
    with pytest.raises(DaftCoreException, match="require a frame"):
        df.with_column("out", last_value(col("x")).over(w)).collect()


def test_first_value_requires_frame():
    df = daft.from_pydict({"x": [1, 2, 3], "g": ["a", "b", "a"]})
    w = Window().partition_by("g").order_by("x")  # order_by present but no frame
    with pytest.raises(DaftCoreException, match="require a frame"):
        df.with_column("out", col("x").first_value().over(w)).collect()


def test_first_value_standalone_requires_frame():
    df = daft.from_pydict({"x": [1, 2, 3], "g": ["a", "b", "a"]})
    w = Window().partition_by("g").order_by("x")
    with pytest.raises(DaftCoreException, match="require a frame"):
        df.with_column("out", first_value(col("x")).over(w)).collect()


# ── 5. Error: .over(window) required ──────────────────────────────────────────


def test_last_value_requires_over():
    df = make_df([1, 2, 3, 4, 5, 6, 7])
    with pytest.raises(DaftCoreException, match="Window expressions cannot be directly evaluated"):
        df.with_column("out", col("v").last_value(ignore_nulls=True)).collect()


def test_last_value_standalone_requires_over():
    df = make_df([1, 2, 3, 4, 5, 6, 7])
    with pytest.raises(DaftCoreException, match="Window expressions cannot be directly evaluated"):
        df.with_column("out", last_value(col("v"), ignore_nulls=True)).collect()


def test_first_value_requires_over():
    df = make_df([1, 2, 3, 4, 5, 6, 7])
    with pytest.raises(DaftCoreException, match="Window expressions cannot be directly evaluated"):
        df.with_column("out", col("v").first_value(ignore_nulls=True)).collect()


def test_first_value_standalone_requires_over():
    df = make_df([1, 2, 3, 4, 5, 6, 7])
    with pytest.raises(DaftCoreException, match="Window expressions cannot be directly evaluated"):
        df.with_column("out", first_value(col("v"), ignore_nulls=True)).collect()


# ── 6. Both entry points produce the same result ──────────────────────────────


def test_last_value_standalone_function():
    values = [10, 20, None, None, 50, 60, 70]
    df = make_df(values)
    method_result = sorted_col(df.with_column("out", col("v").last_value(ignore_nulls=True).over(W_FORWARD)), "out")
    standalone_result = sorted_col(
        df.with_column("out", last_value(col("v"), ignore_nulls=True).over(W_FORWARD)), "out"
    )
    assert method_result == standalone_result


def test_first_value_standalone_function():
    values = [None, None, 30, 40, 50, 60, 70]
    df = make_df(values)
    method_result = sorted_col(df.with_column("out", col("v").first_value(ignore_nulls=True).over(W_BACKWARD)), "out")
    standalone_result = sorted_col(
        df.with_column("out", first_value(col("v"), ignore_nulls=True).over(W_BACKWARD)), "out"
    )
    assert method_result == standalone_result


# ══════════════════════════════════════════════════════════════════════════════
# CORRECTNESS TESTS
#
# Ordered by: function (last → first) × frame (forward → backward) × ignore_nulls (True → False)
# Each parametrized test covers 6 null patterns: no_nulls, all_nulls, leading, middle, trailing, alternating
# ══════════════════════════════════════════════════════════════════════════════

# ── last_value, forward frame ──────────────────────────────────────────────────

# ── 6. last_value(ignore_nulls=True), forward — ffill ─────────────────────────


@pytest.mark.parametrize(
    "values, expected",
    [
        ([10, 20, 30, 40, 50, 60, 70], [10, 20, 30, 40, 50, 60, 70]),
        ([None, None, None, None, None, None, None], [None, None, None, None, None, None, None]),
        ([None, None, 30, 40, 50, 60, 70], [None, None, 30, 40, 50, 60, 70]),
        ([10, 20, None, None, 50, 60, 70], [10, 20, 20, 20, 50, 60, 70]),
        ([10, 20, 30, 40, None, None, None], [10, 20, 30, 40, 40, 40, 40]),
        ([10, None, 30, None, 50, None, 70], [10, 10, 30, 30, 50, 50, 70]),
    ],
    ids=["no_nulls", "all_nulls", "leading", "middle", "trailing", "alternating"],
)
def test_last_value_ignore_nulls_true_forward(values, expected):
    result = make_df(values).with_column("out", col("v").last_value(ignore_nulls=True).over(W_FORWARD))
    assert sorted_col(result, "out") == expected


# ── 7. last_value(ignore_nulls=False), forward — identity (= current row) ──────


@pytest.mark.parametrize(
    "values",
    [
        [10, 20, 30, 40, 50, 60, 70],
        [None, None, None, None, None, None, None],
        [None, None, 30, 40, 50, 60, 70],
        [10, 20, None, None, 50, 60, 70],
        [10, 20, 30, 40, None, None, None],
        [10, None, 30, None, 50, None, 70],
    ],
    ids=["no_nulls", "all_nulls", "leading", "middle", "trailing", "alternating"],
)
def test_last_value_ignore_nulls_false_forward(values):
    """The frame ends at current_row so the last value in the frame is always the current row's own value."""
    result = make_df(values).with_column("out", col("v").last_value(ignore_nulls=False).over(W_FORWARD))
    assert sorted_col(result, "out") == values


# ── 8. last_value() default — sanity check that default == ignore_nulls=False ──


def test_last_value_default_equals_ignore_nulls_false():
    values = [10, 20, None, None, 50, 60, 70]
    df = make_df(values)
    default_result = sorted_col(df.with_column("out", col("v").last_value().over(W_FORWARD)), "out")
    explicit_result = sorted_col(df.with_column("out", col("v").last_value(ignore_nulls=False).over(W_FORWARD)), "out")
    assert default_result == explicit_result


# ── last_value, backward frame ─────────────────────────────────────────────────

# ── 9. last_value(ignore_nulls=True), backward — rightmost non-null in suffix ──


@pytest.mark.parametrize(
    "values, expected",
    [
        ([10, 20, 30, 40, 50, 60, 70], [70, 70, 70, 70, 70, 70, 70]),
        ([None, None, None, None, None, None, None], [None, None, None, None, None, None, None]),
        ([None, None, 30, 40, 50, 60, 70], [70, 70, 70, 70, 70, 70, 70]),
        ([10, 20, None, None, 50, 60, 70], [70, 70, 70, 70, 70, 70, 70]),
        # Trailing nulls: rightmost non-null is 40; rows after it have no non-null remaining
        ([10, 20, 30, 40, None, None, None], [40, 40, 40, 40, None, None, None]),
        ([10, None, 30, None, 50, None, 70], [70, 70, 70, 70, 70, 70, 70]),
    ],
    ids=["no_nulls", "all_nulls", "leading", "middle", "trailing", "alternating"],
)
def test_last_value_ignore_nulls_true_backward(values, expected):
    result = make_df(values).with_column("out", col("v").last_value(ignore_nulls=True).over(W_BACKWARD))
    assert sorted_col(result, "out") == expected


# ── 10. last_value(ignore_nulls=False), backward — last element of partition ───


@pytest.mark.parametrize(
    "values, expected",
    [
        ([10, 20, 30, 40, 50, 60, 70], [70, 70, 70, 70, 70, 70, 70]),
        ([None, None, None, None, None, None, None], [None, None, None, None, None, None, None]),
        ([None, None, 30, 40, 50, 60, 70], [70, 70, 70, 70, 70, 70, 70]),
        ([10, 20, None, None, 50, 60, 70], [70, 70, 70, 70, 70, 70, 70]),
        # Trailing nulls: last element is null, so all rows see null
        ([10, 20, 30, 40, None, None, None], [None, None, None, None, None, None, None]),
        ([10, None, 30, None, 50, None, 70], [70, 70, 70, 70, 70, 70, 70]),
    ],
    ids=["no_nulls", "all_nulls", "leading", "middle", "trailing", "alternating"],
)
def test_last_value_ignore_nulls_false_backward(values, expected):
    result = make_df(values).with_column("out", col("v").last_value(ignore_nulls=False).over(W_BACKWARD))
    assert sorted_col(result, "out") == expected


# ── first_value, forward frame ─────────────────────────────────────────────────

# ── 11. first_value(ignore_nulls=True), forward — cumulative first non-null ────


@pytest.mark.parametrize(
    "values, expected",
    [
        ([10, 20, 30, 40, 50, 60, 70], [10, 10, 10, 10, 10, 10, 10]),
        ([None, None, None, None, None, None, None], [None, None, None, None, None, None, None]),
        # Leading nulls: null until 30 appears at row 3, then 30 held for all remaining rows
        ([None, None, 30, 40, 50, 60, 70], [None, None, 30, 30, 30, 30, 30]),
        ([10, 20, None, None, 50, 60, 70], [10, 10, 10, 10, 10, 10, 10]),
        ([10, 20, 30, 40, None, None, None], [10, 10, 10, 10, 10, 10, 10]),
        ([10, None, 30, None, 50, None, 70], [10, 10, 10, 10, 10, 10, 10]),
    ],
    ids=["no_nulls", "all_nulls", "leading", "middle", "trailing", "alternating"],
)
def test_first_value_ignore_nulls_true_forward(values, expected):
    """Differs from ignore_nulls=False only for leading nulls — once the first non-null is seen it is held for all subsequent rows."""
    result = make_df(values).with_column("out", col("v").first_value(ignore_nulls=True).over(W_FORWARD))
    assert sorted_col(result, "out") == expected


# ── 12. first_value(ignore_nulls=False), forward — first element of partition ──


@pytest.mark.parametrize(
    "values, expected",
    [
        ([10, 20, 30, 40, 50, 60, 70], [10, 10, 10, 10, 10, 10, 10]),
        ([None, None, None, None, None, None, None], [None, None, None, None, None, None, None]),
        ([None, None, 30, 40, 50, 60, 70], [None, None, None, None, None, None, None]),
        ([10, 20, None, None, 50, 60, 70], [10, 10, 10, 10, 10, 10, 10]),
        ([10, 20, 30, 40, None, None, None], [10, 10, 10, 10, 10, 10, 10]),
        ([10, None, 30, None, 50, None, 70], [10, 10, 10, 10, 10, 10, 10]),
    ],
    ids=["no_nulls", "all_nulls", "leading", "middle", "trailing", "alternating"],
)
def test_first_value_ignore_nulls_false_forward(values, expected):
    """The frame starts at unbounded_preceding so the first value is always the partition's first element, constant across all rows."""
    result = make_df(values).with_column("out", col("v").first_value(ignore_nulls=False).over(W_FORWARD))
    assert sorted_col(result, "out") == expected


# ── first_value, backward frame ────────────────────────────────────────────────

# ── 13. first_value(ignore_nulls=True), backward — bfill ──────────────────────


@pytest.mark.parametrize(
    "values, expected",
    [
        ([10, 20, 30, 40, 50, 60, 70], [10, 20, 30, 40, 50, 60, 70]),
        ([None, None, None, None, None, None, None], [None, None, None, None, None, None, None]),
        ([None, None, 30, 40, 50, 60, 70], [30, 30, 30, 40, 50, 60, 70]),
        ([10, 20, None, None, 50, 60, 70], [10, 20, 50, 50, 50, 60, 70]),
        ([10, 20, 30, 40, None, None, None], [10, 20, 30, 40, None, None, None]),
        ([10, None, 30, None, 50, None, 70], [10, 30, 30, 50, 50, 70, 70]),
    ],
    ids=["no_nulls", "all_nulls", "leading", "middle", "trailing", "alternating"],
)
def test_first_value_ignore_nulls_true_backward(values, expected):
    result = make_df(values).with_column("out", col("v").first_value(ignore_nulls=True).over(W_BACKWARD))
    assert sorted_col(result, "out") == expected


# ── 14. first_value(ignore_nulls=False), backward — identity (= current row) ───


@pytest.mark.parametrize(
    "values",
    [
        [10, 20, 30, 40, 50, 60, 70],
        [None, None, None, None, None, None, None],
        [None, None, 30, 40, 50, 60, 70],
        [10, 20, None, None, 50, 60, 70],
        [10, 20, 30, 40, None, None, None],
        [10, None, 30, None, 50, None, 70],
    ],
    ids=["no_nulls", "all_nulls", "leading", "middle", "trailing", "alternating"],
)
def test_first_value_ignore_nulls_false_backward(values):
    """The frame starts at current_row so the first value in the frame is always the current row's own value."""
    result = make_df(values).with_column("out", col("v").first_value(ignore_nulls=False).over(W_BACKWARD))
    assert sorted_col(result, "out") == values


# ── 15. partition_by — last_value(ignore_nulls=True), device ffill ─────────────


def test_last_value_partition_by_device_ffill():
    df = daft.from_pydict(
        {
            "device": ["A", "A", "A", "B", "B", "B"],
            "t": [1, 2, 3, 1, 2, 3],
            "temp": [20.1, None, 20.5, None, 19.0, None],
        }
    )
    w = Window().partition_by("device").order_by("t").rows_between(Window.unbounded_preceding, Window.current_row)
    result = (
        df.with_column("out", col("temp").last_value(ignore_nulls=True).over(w))
        .sort(["device", "t"])
        .collect()
        .to_pydict()
    )
    assert result["out"] == [20.1, 20.1, 20.5, None, 19.0, 19.0]


# ── 16. partition_by — first_value(ignore_nulls=True), device bfill ────────────


def test_first_value_partition_by_device_bfill():
    df = daft.from_pydict(
        {
            "device": ["A", "A", "A", "B", "B", "B"],
            "t": [1, 2, 3, 1, 2, 3],
            "temp": [20.1, None, 20.5, None, 19.0, None],
        }
    )
    w = Window().partition_by("device").order_by("t").rows_between(Window.current_row, Window.unbounded_following)
    result = (
        df.with_column("out", col("temp").first_value(ignore_nulls=True).over(w))
        .sort(["device", "t"])
        .collect()
        .to_pydict()
    )
    assert result["out"] == [20.1, 20.5, 20.5, 19.0, 19.0, None]


# ── 17. Multiple partition columns ─────────────────────────────────────────────


def test_last_value_multiple_partition_columns():
    df = daft.from_pydict(
        {
            "region": ["US", "US", "US", "EU", "EU", "EU"],
            "device": ["A", "A", "A", "A", "A", "A"],
            "t": [1, 2, 3, 1, 2, 3],
            "v": [10, None, 30, None, 20, None],
        }
    )
    w = (
        Window()
        .partition_by("region", "device")
        .order_by("t")
        .rows_between(Window.unbounded_preceding, Window.current_row)
    )
    result = (
        df.with_column("out", col("v").last_value(ignore_nulls=True).over(w))
        .sort(["region", "device", "t"])
        .collect()
        .to_pydict()
    )
    # EU rows (sorted first): [None→None, 20→20, None→20]; US rows: [10→10, None→10, 30→30]
    assert result["out"] == [None, 20, 20, 10, 10, 30]


def test_first_value_multiple_partition_columns():
    df = daft.from_pydict(
        {
            "region": ["US", "US", "US", "EU", "EU", "EU"],
            "device": ["A", "A", "A", "A", "A", "A"],
            "t": [1, 2, 3, 1, 2, 3],
            "v": [10, None, 30, None, 20, None],
        }
    )
    w = (
        Window()
        .partition_by("region", "device")
        .order_by("t")
        .rows_between(Window.current_row, Window.unbounded_following)
    )
    result = (
        df.with_column("out", col("v").first_value(ignore_nulls=True).over(w))
        .sort(["region", "device", "t"])
        .collect()
        .to_pydict()
    )
    # EU: [None→20, 20→20, None→None]; US: [10→10, None→30, 30→30]
    assert result["out"] == [20, 20, None, 10, 30, 30]


# ── 18. Single-row partitions ──────────────────────────────────────────────────


def test_last_value_single_row_partitions():
    """A single-row partition has no prior rows to carry forward, so the result equals the row's own value."""
    df = daft.from_pydict(
        {
            "group": ["A", "B", "C", "D"],
            "t": [1, 2, 3, 4],
            "v": [10, None, 30, None],
        }
    )
    w = Window().partition_by("group").order_by("t").rows_between(Window.unbounded_preceding, Window.current_row)
    result = df.with_column("out", col("v").last_value(ignore_nulls=True).over(w)).sort("t").collect().to_pydict()
    assert result["out"] == [10, None, 30, None]


# ── 19. Float NaN vs null ──────────────────────────────────────────────────────


def test_last_value_nan_not_treated_as_null():
    """NaN is a float value and must not be skipped by ignore_nulls=True — only actual nulls are skipped."""
    import math

    nan = float("nan")
    t = [1, 2, 3, 4, 5]
    df = daft.from_pydict({"t": t, "g": ["A"] * 5, "v": [1.0, nan, 3.0, None, 5.0]})
    w = Window().partition_by("g").order_by("t").rows_between(Window.unbounded_preceding, Window.current_row)
    result = (
        df.with_column("out", col("v").last_value(ignore_nulls=True).over(w)).sort("t").collect().to_pydict()["out"]
    )
    assert result[0] == 1.0
    assert math.isnan(result[1])  # NaN stays — not skipped
    assert result[2] == 3.0
    assert result[3] == 3.0  # null at row 4 is filled from row 3
    assert result[4] == 5.0


# ── Bounded frames ─────────────────────────────────────────────────────────────

# ── 20. last_value(True), rows_between(-2, current_row) — sliding look-back ───


@pytest.mark.parametrize(
    "values, expected",
    [
        ([10, 20, 30, 40, 50, 60, 70], [10, 20, 30, 40, 50, 60, 70]),
        ([None, None, None, None, None, None, None], [None, None, None, None, None, None, None]),
        ([None, None, 30, 40, 50, 60, 70], [None, None, 30, 40, 50, 60, 70]),
        ([10, 20, None, None, 50, 60, 70], [10, 20, 20, 20, 50, 60, 70]),
        # Row 7: window is [None, None, None] — no non-null in range, unlike unbounded which holds 40
        ([10, 20, 30, 40, None, None, None], [10, 20, 30, 40, 40, 40, None]),
        ([10, None, 30, None, 50, None, 70], [10, 10, 30, 30, 50, 50, 70]),
    ],
    ids=["no_nulls", "all_nulls", "leading", "middle", "trailing", "alternating"],
)
def test_last_value_ignore_nulls_true_bounded_lookback(values, expected):
    """Bounded window evicts old rows so a trailing null run can exhaust all non-nulls in the window, unlike the unbounded case."""
    w = Window().partition_by("g").order_by("t").rows_between(-2, Window.current_row)
    result = make_df(values).with_column("out", col("v").last_value(ignore_nulls=True).over(w))
    assert sorted_col(result, "out") == expected


# ── 21. first_value(True), rows_between(current_row, 2) — sliding look-ahead ──


@pytest.mark.parametrize(
    "values, expected",
    [
        ([10, 20, 30, 40, 50, 60, 70], [10, 20, 30, 40, 50, 60, 70]),
        ([None, None, None, None, None, None, None], [None, None, None, None, None, None, None]),
        ([None, None, 30, 40, 50, 60, 70], [30, 30, 30, 40, 50, 60, 70]),
        ([10, 20, None, None, 50, 60, 70], [10, 20, 50, 50, 50, 60, 70]),
        # Rows 5-7: window [None, None, None] — no non-null ahead, unlike unbounded which holds trailing value
        ([10, 20, 30, 40, None, None, None], [10, 20, 30, 40, None, None, None]),
        ([10, None, 30, None, 50, None, 70], [10, 30, 30, 50, 50, 70, 70]),
    ],
    ids=["no_nulls", "all_nulls", "leading", "middle", "trailing", "alternating"],
)
def test_first_value_ignore_nulls_true_bounded_lookahead(values, expected):
    """Bounded window evicts past rows so a leading null run can exhaust all non-nulls ahead, unlike the unbounded case."""
    w = Window().partition_by("g").order_by("t").rows_between(Window.current_row, 2)
    result = make_df(values).with_column("out", col("v").first_value(ignore_nulls=True).over(w))
    assert sorted_col(result, "out") == expected
