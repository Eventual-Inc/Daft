from __future__ import annotations

import pytest

from daft import col
from daft.datatype import DataType
from daft.recordbatch import MicroPartition


def test_list_compact_basic():
    table = MicroPartition.from_pydict({"a": [[1, None, 2, None, 3], [None, None], [1, 2], None, []]})
    result = table.eval_expression_list([col("a").list_compact()])
    assert result.to_pydict()["a"] == [[1, 2, 3], [], [1, 2], None, []]


def test_list_compact_no_nulls():
    table = MicroPartition.from_pydict({"a": [[1, 2, 3], [4, 5]]})
    result = table.eval_expression_list([col("a").list_compact()])
    assert result.to_pydict()["a"] == [[1, 2, 3], [4, 5]]


def test_list_compact_strings():
    table = MicroPartition.from_pydict({"a": [["a", None, "b"], [None]]})
    result = table.eval_expression_list([col("a").list_compact()])
    assert result.to_pydict()["a"] == [["a", "b"], []]


def test_list_compact_fixed_size():
    table = MicroPartition.from_pydict({"a": [[1, None], [None, 2], [3, 4]]})
    table = table.eval_expression_list([col("a").cast(DataType.fixed_size_list(DataType.int64(), 2))])
    result = table.eval_expression_list([col("a").list_compact()])
    assert result.to_pydict()["a"] == [[1], [2], [3, 4]]


def test_list_compact_sql_alias():
    """The Spark-compatible SQL alias `array_compact` should resolve to `list_compact`."""
    import daft

    df = daft.from_pydict({"a": [[1, None, 2], [None, None], [3]]})
    result = daft.sql("SELECT array_compact(a) AS r FROM df", df=df).to_pydict()
    assert result["r"] == [[1, 2], [], [3]]


@pytest.mark.parametrize(
    "data,target,expected",
    [
        pytest.param([[1, 2, 3], [4, 5], [3, 3, 3]], 3, [3, 0, 1], id="ints"),
        pytest.param([["a", "b", "c"], ["d", "e"]], "b", [2, 0], id="strings"),
        pytest.param([[1.0, 2.0, 3.0], [4.0]], 2.0, [2, 0], id="floats"),
        pytest.param([[True, False], [False, False]], True, [1, 0], id="bools"),
    ],
)
def test_list_position_types(data, target, expected):
    table = MicroPartition.from_pydict({"a": data})
    result = table.eval_expression_list([col("a").list_position(target)])
    assert result.to_pydict()["a"] == expected


def test_list_position_null_propagation():
    table = MicroPartition.from_pydict({"a": [[1, 2], None, [3, 4]], "item": [2, 2, None]})
    result = table.eval_expression_list([col("a").list_position(col("item"))])
    assert result.to_pydict()["a"] == [2, None, None]


def test_list_position_nulls_in_list_skipped():
    """Null elements in the list are not matched even if the search value is null-castable."""
    table = MicroPartition.from_pydict({"a": [[1, None, 3], [None, None]]})
    result = table.eval_expression_list([col("a").list_position(3)])
    assert result.to_pydict()["a"] == [3, 0]


def test_list_position_column_items():
    table = MicroPartition.from_pydict({"lists": [[1, 2, 3], [4, 5, 6]], "items": [3, 6]})
    result = table.eval_expression_list([col("lists").list_position(col("items"))])
    assert result.to_pydict()["lists"] == [3, 3]


def test_list_position_fixed_size():
    table = MicroPartition.from_pydict({"a": [[1, 2], [3, 4]]})
    table = table.eval_expression_list([col("a").cast(DataType.fixed_size_list(DataType.int64(), 2))])
    result = table.eval_expression_list([col("a").list_position(3)])
    assert result.to_pydict()["a"] == [0, 1]


def test_list_position_sql_alias():
    import daft

    df = daft.from_pydict({"a": [[10, 20, 30], [5, 5, 30]]})
    result = daft.sql("SELECT array_position(a, 30) AS r FROM df", df=df).to_pydict()
    assert result["r"] == [3, 3]


def test_list_except_basic():
    table = MicroPartition.from_pydict({"a": [[1, 2, 3], [1, 1, 2], [4, 5]], "b": [[2, 3, 4], [2], [4, 5]]})
    result = table.eval_expression_list([col("a").list_except(col("b"))])
    out = result.to_pydict()["a"]
    assert out[0] == [1]
    assert out[1] == [1]
    assert out[2] == []


def test_list_except_with_duplicates_dropped():
    table = MicroPartition.from_pydict({"a": [[1, 1, 2, 2, 3]], "b": [[2]]})
    result = table.eval_expression_list([col("a").list_except(col("b"))])
    # Result should preserve order of first occurrence and remove duplicates.
    assert result.to_pydict()["a"] == [[1, 3]]


def test_list_except_nulls_kept_when_rhs_has_no_null():
    # Null-safe-equal semantics (Spark): a null in lhs is dropped only if rhs also has a null.
    table = MicroPartition.from_pydict({"a": [[1, None, 2, None]], "b": [[None, 1]]})
    result = table.eval_expression_list([col("a").list_except(col("b"))])
    # rhs contains a null, so the lhs null is removed (matched).
    assert result.to_pydict()["a"] == [[2]]


def test_list_except_keeps_null_when_rhs_has_no_null():
    table = MicroPartition.from_pydict({"a": [[1, None, 2]], "b": [[1]]})
    result = table.eval_expression_list([col("a").list_except(col("b"))])
    # rhs has no null, so null is kept in the result.
    assert result.to_pydict()["a"] == [[None, 2]]


def test_list_except_null_list_returns_null():
    table = MicroPartition.from_pydict({"a": [[1, 2], None], "b": [[1], [2]]})
    result = table.eval_expression_list([col("a").list_except(col("b"))])
    assert result.to_pydict()["a"] == [[2], None]


def test_list_except_sql_alias():
    import daft

    df = daft.from_pydict({"a": [[1, 2, 3]], "b": [[2, 4]]})
    result = daft.sql("SELECT array_except(a, b) AS r FROM df", df=df).to_pydict()
    assert result["r"] == [[1, 3]]


def test_list_intersect_basic():
    table = MicroPartition.from_pydict({"a": [[1, 2, 3], [1, 2, 3]], "b": [[2, 3, 4], [4, 5, 6]]})
    result = table.eval_expression_list([col("a").list_intersect(col("b"))])
    assert result.to_pydict()["a"] == [[2, 3], []]


def test_list_intersect_with_duplicates():
    table = MicroPartition.from_pydict({"a": [[1, 1, 2, 2]], "b": [[1, 2, 2, 3]]})
    result = table.eval_expression_list([col("a").list_intersect(col("b"))])
    assert result.to_pydict()["a"] == [[1, 2]]


def test_list_intersect_nulls_kept_when_both_have_null():
    # Null-safe-equal semantics (Spark): null is kept only if both sides have a null.
    table = MicroPartition.from_pydict({"a": [[1, None, 2]], "b": [[None, 2]]})
    result = table.eval_expression_list([col("a").list_intersect(col("b"))])
    assert result.to_pydict()["a"] == [[None, 2]]


def test_list_intersect_null_dropped_when_only_one_side_has_null():
    table = MicroPartition.from_pydict({"a": [[1, None, 2]], "b": [[2, 3]]})
    result = table.eval_expression_list([col("a").list_intersect(col("b"))])
    assert result.to_pydict()["a"] == [[2]]


def test_list_intersect_null_list_returns_null():
    table = MicroPartition.from_pydict({"a": [[1, 2], None], "b": [[2, 3], [1]]})
    result = table.eval_expression_list([col("a").list_intersect(col("b"))])
    assert result.to_pydict()["a"] == [[2], None]


def test_list_intersect_sql_alias():
    import daft

    df = daft.from_pydict({"a": [[1, 2, 3]], "b": [[2, 3, 4]]})
    result = daft.sql("SELECT array_intersect(a, b) AS r FROM df", df=df).to_pydict()
    assert result["r"] == [[2, 3]]


def test_list_union_basic():
    table = MicroPartition.from_pydict({"a": [[1, 2, 3], [1, 2]], "b": [[2, 3, 4], [3, 4]]})
    result = table.eval_expression_list([col("a").list_union(col("b"))])
    assert result.to_pydict()["a"] == [[1, 2, 3, 4], [1, 2, 3, 4]]


def test_list_union_dedup_within_inputs():
    table = MicroPartition.from_pydict({"a": [[1, 1, 2]], "b": [[2, 2, 3]]})
    result = table.eval_expression_list([col("a").list_union(col("b"))])
    assert result.to_pydict()["a"] == [[1, 2, 3]]


def test_list_union_keeps_null_from_either_side():
    # Null-safe-equal semantics (Spark): result contains a single null if either input has a null.
    table = MicroPartition.from_pydict({"a": [[1, None, 2]], "b": [[None, 3]]})
    result = table.eval_expression_list([col("a").list_union(col("b"))])
    assert result.to_pydict()["a"] == [[1, None, 2, 3]]


def test_list_union_null_only_on_left():
    table = MicroPartition.from_pydict({"a": [[1, None, 2]], "b": [[3]]})
    result = table.eval_expression_list([col("a").list_union(col("b"))])
    assert result.to_pydict()["a"] == [[1, None, 2, 3]]


def test_list_union_null_only_on_right():
    table = MicroPartition.from_pydict({"a": [[1, 2]], "b": [[None, 3]]})
    result = table.eval_expression_list([col("a").list_union(col("b"))])
    assert result.to_pydict()["a"] == [[1, 2, None, 3]]


def test_list_union_null_list_returns_null():
    table = MicroPartition.from_pydict({"a": [[1, 2], None], "b": [[3], [4]]})
    result = table.eval_expression_list([col("a").list_union(col("b"))])
    assert result.to_pydict()["a"] == [[1, 2, 3], None]


def test_list_union_sql_alias():
    import daft

    df = daft.from_pydict({"a": [[1, 2]], "b": [[2, 3]]})
    result = daft.sql("SELECT array_union(a, b) AS r FROM df", df=df).to_pydict()
    assert result["r"] == [[1, 2, 3]]


def test_set_ops_fixed_size_list():
    """Set ops should work transparently when inputs are FixedSizeList."""
    table = MicroPartition.from_pydict({"a": [[1, 2], [3, 4]], "b": [[2, 3], [4, 5]]})
    table = table.eval_expression_list(
        [
            col("a").cast(DataType.fixed_size_list(DataType.int64(), 2)),
            col("b").cast(DataType.fixed_size_list(DataType.int64(), 2)),
        ]
    )
    res = table.eval_expression_list(
        [
            col("a").list_intersect(col("b")).alias("inter"),
            col("a").list_except(col("b")).alias("diff"),
            col("a").list_union(col("b")).alias("uni"),
        ]
    )
    out = res.to_pydict()
    assert out["inter"] == [[2], [4]]
    assert out["diff"] == [[1], [3]]
    assert out["uni"] == [[1, 2, 3], [3, 4, 5]]


def test_set_ops_type_promotion_int_float():
    """Mixed numeric element types should be promoted to a common supertype (Spark-compatible)."""
    table = MicroPartition.from_pydict({"a": [[1, 2, 3]], "b": [[2.0, 3.0, 4.0]]})
    table = table.eval_expression_list(
        [
            col("a").cast(DataType.list(DataType.int64())),
            col("b").cast(DataType.list(DataType.float64())),
        ]
    )
    res = table.eval_expression_list(
        [
            col("a").list_union(col("b")).alias("uni"),
            col("a").list_intersect(col("b")).alias("inter"),
            col("a").list_except(col("b")).alias("diff"),
        ]
    )
    out = res.to_pydict()
    assert out["uni"] == [[1.0, 2.0, 3.0, 4.0]]
    assert out["inter"] == [[2.0, 3.0]]
    assert out["diff"] == [[1.0]]
