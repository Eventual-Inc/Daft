from __future__ import annotations

import pyarrow as pa
import pytest

from daft import Series
from daft.datatype import DataType


@pytest.mark.parametrize("if_true_value", [1, None])
@pytest.mark.parametrize("if_false_value", [0, None])
@pytest.mark.parametrize(
    "if_true_length",
    [1, 3],
)
@pytest.mark.parametrize(
    "if_false_length",
    [1, 3],
)
@pytest.mark.parametrize(
    # Test supercasting.
    "true_false_types",
    [(pa.int64(), pa.int64()), (pa.int64(), pa.int8()), (pa.int8(), pa.int64())],
)
def test_series_if_else_numeric(
    if_true_value,
    if_false_value,
    if_true_length,
    if_false_length,
    true_false_types,
) -> None:
    true_type, false_type = true_false_types

    if_true_series = Series.from_arrow(pa.array([if_true_value] * if_true_length, type=true_type))
    if_false_series = Series.from_arrow(pa.array([if_false_value] * if_false_length, type=false_type))
    predicate_series = Series.from_arrow(pa.array([True, False, None]))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.int64()
    assert result.to_pylist() == [if_true_value, if_false_value, None]


@pytest.mark.parametrize("if_true_value", [object(), None])
@pytest.mark.parametrize("if_false_value", [object(), None])
@pytest.mark.parametrize(
    "if_true_length",
    [1, 3],
)
@pytest.mark.parametrize(
    "if_false_length",
    [1, 3],
)
def test_series_if_else_pyobj(
    if_true_value,
    if_false_value,
    if_true_length,
    if_false_length,
) -> None:
    if_true_series = Series.from_pylist([if_true_value] * if_true_length)
    if_false_series = Series.from_pylist([if_false_value] * if_false_length)
    predicate_series = Series.from_arrow(pa.array([True, False, None]))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.to_pylist() == [if_true_value, if_false_value, None]


@pytest.mark.parametrize("if_true_value", ["1", None])
@pytest.mark.parametrize("if_false_value", ["0", None])
@pytest.mark.parametrize(
    "if_true_length",
    [1, 3],
)
@pytest.mark.parametrize(
    "if_false_length",
    [1, 3],
)
def test_series_if_else_string(
    if_true_value,
    if_false_value,
    if_true_length,
    if_false_length,
) -> None:
    if_true_series = Series.from_arrow(pa.array([if_true_value] * if_true_length, type=pa.string()))
    if_false_series = Series.from_arrow(pa.array([if_false_value] * if_false_length, type=pa.string()))
    predicate_series = Series.from_arrow(pa.array([True, False, None]))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.string()
    assert result.to_pylist() == [if_true_value, if_false_value, None]


@pytest.mark.parametrize("if_true_value", [True, None])
@pytest.mark.parametrize("if_false_value", [None, None])
@pytest.mark.parametrize(
    "if_true_length",
    [1, 3],
)
@pytest.mark.parametrize(
    "if_false_length",
    [1, 3],
)
def test_series_if_else_bool(
    if_true_value,
    if_false_value,
    if_true_length,
    if_false_length,
) -> None:
    if_true_series = Series.from_arrow(pa.array([if_true_value] * if_true_length, type=pa.bool_()))
    if_false_series = Series.from_arrow(pa.array([if_false_value] * if_false_length, type=pa.bool_()))
    predicate_series = Series.from_arrow(pa.array([True, False, None]))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.bool()
    assert result.to_pylist() == [if_true_value, if_false_value, None]


@pytest.mark.parametrize("if_true_value", [b"Y", None])
@pytest.mark.parametrize("if_false_value", [b"N", None])
@pytest.mark.parametrize(
    "if_true_length",
    [1, 3],
)
@pytest.mark.parametrize(
    "if_false_length",
    [1, 3],
)
def test_series_if_else_binary(
    if_true_value,
    if_false_value,
    if_true_length,
    if_false_length,
) -> None:
    if_true_series = Series.from_arrow(pa.array([if_true_value] * if_true_length, type=pa.binary()))
    if_false_series = Series.from_arrow(pa.array([if_false_value] * if_false_length, type=pa.binary()))
    predicate_series = Series.from_arrow(pa.array([True, False, None]))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.binary()
    assert result.to_pylist() == [if_true_value, if_false_value, None]


@pytest.mark.parametrize(
    ["if_true", "if_false", "expected"],
    [
        # Same length, same type
        (
            pa.array([[1, 2], [None], None, [5, 6, 7, 8]], type=pa.list_(pa.int64())),
            pa.array([[9], [10, None, 12, 13], None, [15, 16]], type=pa.list_(pa.int64())),
            [[1, 2], [10, None, 12, 13], None, [5, 6, 7, 8]],
        ),
        # Same length, different super-castable data type
        (
            pa.array([[1, 2], [None], None, [5, 6, 7, 8]], type=pa.list_(pa.int32())),
            pa.array([[9], [10, None, 12, 13], None, [15, 16]], type=pa.list_(pa.int64())),
            [[1, 2], [10, None, 12, 13], None, [5, 6, 7, 8]],
        ),
        # TODO(Clark): Uncomment this case when Arrow2 supports casting between FixedSizeList and LargeLists.
        # # Same length, different super-castable list type (FixedSizeList + List)
        # (
        #     pa.array([[1, 2], [None, 4], None, [7, 8]], type=pa.list_(pa.int64(), 2)),
        #     pa.array([[9], [10, None, 12, 13], None, [15, 16]], type=pa.list_(pa.int64())),
        #     [[1, 2], [10, None, 12, 13], None, [5, 6, 7, 8]],
        # ),
        # Broadcast left
        (
            pa.array([[1, 2]], type=pa.list_(pa.int64())),
            pa.array([[9], [10, None, 12, 13], None, [15, 16]], type=pa.list_(pa.int64())),
            [[1, 2], [10, None, 12, 13], None, [1, 2]],
        ),
        # Broadcast right
        (
            pa.array([[1, 2], [None], None, [5, 6, 7, 8]], type=pa.list_(pa.int64())),
            pa.array([[9]], type=pa.list_(pa.int64())),
            [[1, 2], [9], None, [5, 6, 7, 8]],
        ),
        # Broadcast both
        (
            pa.array([[1, 2]], type=pa.list_(pa.int64())),
            pa.array([[9]], type=pa.list_(pa.int64())),
            [[1, 2], [9], None, [1, 2]],
        ),
    ],
)
def test_series_if_else_list(if_true, if_false, expected) -> None:
    if_true_series = Series.from_arrow(if_true)
    if_false_series = Series.from_arrow(if_false)
    predicate_series = Series.from_arrow(pa.array([True, False, None, True]))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.list("item", DataType.int64())
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["if_true", "if_false", "expected"],
    [
        # Same length, same type
        (
            pa.array([[1, 2], [None, 4], None, [7, 8]], type=pa.list_(pa.int64(), 2)),
            pa.array([[9, 10], [None, 12], None, [15, 16]], type=pa.list_(pa.int64(), 2)),
            [[1, 2], [None, 12], None, [7, 8]],
        ),
        # TODO(Clark): Uncomment this case when Arrow2 supports broadcasting between different FixedSizeListArrays.
        # Same length, different super-castable type
        # (pa.array([[1, 2], [None, 4], None, [7, 8]], type=pa.list_(pa.int64(), 2)), pa.array([[9, 10], [None, 12], None, [15, 16]], type=pa.list_(pa.int32(), 2))),
        # Broadcast left
        (
            pa.array([[1, 2]], type=pa.list_(pa.int64(), 2)),
            pa.array([[9, 10], [None, 12], None, [15, 16]], type=pa.list_(pa.int64(), 2)),
            [[1, 2], [None, 12], None, [1, 2]],
        ),
        # Broadcast right
        (
            pa.array([[1, 2], [None, 4], None, [7, 8]], type=pa.list_(pa.int64(), 2)),
            pa.array([[9, 10]], type=pa.list_(pa.int64(), 2)),
            [[1, 2], [9, 10], None, [7, 8]],
        ),
        # Broadcast both
        (
            pa.array([[1, 2]], type=pa.list_(pa.int64(), 2)),
            pa.array([[9, 10]], type=pa.list_(pa.int64(), 2)),
            [[1, 2], [9, 10], None, [1, 2]],
        ),
    ],
)
def test_series_if_else_fixed_size_list(if_true, if_false, expected) -> None:
    if_true_series = Series.from_arrow(if_true)
    if_false_series = Series.from_arrow(if_false)
    predicate_series = Series.from_arrow(pa.array([True, False, None, True]))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.fixed_size_list("item", DataType.int64(), 2)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["if_true", "if_false", "expected"],
    [
        # Same length, same type
        (
            pa.array(
                [{"a": 1, "b": 2}, {"b": 3, "c": "4"}, None, {"a": 5, "b": 6, "c": "7"}],
                type=pa.struct({"a": pa.int64(), "b": pa.float64(), "c": pa.string()}),
            ),
            pa.array(
                [{"a": 8, "b": 9, "c": "10"}, {"c": "11"}, None, {"a": 12, "b": 13}],
                type=pa.struct({"a": pa.int64(), "b": pa.float64(), "c": pa.string()}),
            ),
            [{"a": 1, "b": 2.0, "c": None}, {"a": None, "b": None, "c": "11"}, None, {"a": 5, "b": 6.0, "c": "7"}],
        ),
        # TODO(Clark): Uncomment this case when Arrow2 supports casting struct types.
        # # Same length, different super-castable data type
        # (
        #     pa.array([{"a": 1, "b": 2}, {"b": 3, "c": 4}, None, {"a": 5, "b": 6, "c": 7}]).cast(pa.struct([pa.int64(), pa.float64(), pa.string()])),
        #     pa.array([{"a": 8, "b": 9, "c": 10}, {"c": 11}, None, {"a": 12, "b": 13}]).cast(pa.struct([pa.int32(), pa.float32(), pa.large_string()])),
        #     [{"a": 1, "b": 2}, {"c": 11}, None, {"a": 5, "b": 6, "c": 7}],
        # ),
        # Broadcast left
        (
            pa.array(
                [{"a": 1, "b": 2, "c": None}], type=pa.struct({"a": pa.int64(), "b": pa.float64(), "c": pa.string()})
            ),
            pa.array(
                [{"a": 8, "b": 9, "c": "10"}, {"c": "11"}, None, {"a": 12, "b": 13}],
                type=pa.struct({"a": pa.int64(), "b": pa.float64(), "c": pa.string()}),
            ),
            [{"a": 1, "b": 2.0, "c": None}, {"a": None, "b": None, "c": "11"}, None, {"a": 1, "b": 2.0, "c": None}],
        ),
        # Broadcast right
        (
            pa.array(
                [{"a": 1, "b": 2}, {"b": 3, "c": "4"}, None, {"a": 5, "b": 6, "c": "7"}],
                type=pa.struct({"a": pa.int64(), "b": pa.float64(), "c": pa.string()}),
            ),
            pa.array(
                [{"a": 8, "b": 9, "c": "10"}], type=pa.struct({"a": pa.int64(), "b": pa.float64(), "c": pa.string()})
            ),
            [{"a": 1, "b": 2.0, "c": None}, {"a": 8, "b": 9.0, "c": "10"}, None, {"a": 5, "b": 6.0, "c": "7"}],
        ),
        # Broadcast both
        (
            pa.array(
                [{"a": 1, "b": 2, "c": None}], type=pa.struct({"a": pa.int64(), "b": pa.float64(), "c": pa.string()})
            ),
            pa.array(
                [{"a": 8, "b": 9, "c": "10"}], type=pa.struct({"a": pa.int64(), "b": pa.float64(), "c": pa.string()})
            ),
            [{"a": 1, "b": 2.0, "c": None}, {"a": 8, "b": 9.0, "c": "10"}, None, {"a": 1, "b": 2.0, "c": None}],
        ),
    ],
)
def test_series_if_else_struct(if_true, if_false, expected) -> None:
    if_true_series = Series.from_arrow(if_true)
    if_false_series = Series.from_arrow(if_false)
    predicate_series = Series.from_arrow(pa.array([True, False, None, True]))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.struct(
        {"a": DataType.int64(), "b": DataType.float64(), "c": DataType.string()}
    )
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    "if_true_length",
    [1, 3],
)
@pytest.mark.parametrize(
    "if_false_length",
    [1, 3],
)
def test_series_if_else_nulls(
    if_true_length,
    if_false_length,
) -> None:
    if_true_series = Series.from_arrow(pa.array([None] * if_true_length, type=pa.null()))
    if_false_series = Series.from_arrow(pa.array([None] * if_false_length, type=pa.null()))
    predicate_series = Series.from_arrow(pa.array([True, False, None]))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.null()
    assert result.to_pylist() == [None, None, None]


@pytest.mark.parametrize(
    ["if_true", "if_false"],
    [
        # Same length, same type
        ([object(), object(), object()], [object(), object(), object()]),
        # Broadcast left
        ([object()], [object(), object(), object()]),
        # Broadcast right
        ([object(), object(), object()], [object()]),
        # Broadcast both
        ([object()], [object()]),
    ],
)
def test_series_if_else_python(if_true, if_false) -> None:
    if_true_series = Series.from_pylist(if_true)
    if_false_series = Series.from_pylist(if_false)
    predicate_series = Series.from_arrow(pa.array([True, False, None]))

    result = predicate_series.if_else(if_true_series, if_false_series)

    left_expected = if_true[0]
    right_expected = if_false[1] if len(if_false) > 1 else if_false[0]
    assert result.datatype() == DataType.python()
    assert result.to_pylist() == [left_expected, right_expected, None]


@pytest.mark.parametrize(
    ["predicate_value", "expected_results"], [(True, [1, 1, 1]), (False, [0, 0, 0]), (None, [None, None, None])]
)
def test_series_if_else_predicate_broadcast_numeric(predicate_value, expected_results) -> None:
    if_true_series = Series.from_arrow(pa.array([1, 1, 1], type=pa.int64()))
    if_false_series = Series.from_arrow(pa.array([0, 0, 0], type=pa.int64()))
    predicate_series = Series.from_arrow(pa.array([predicate_value], type=pa.bool_()))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.int64()
    assert result.to_pylist() == expected_results


@pytest.mark.parametrize(
    ["predicate_value", "expected_results"],
    [(True, ["1", "1", "1"]), (False, ["0", "0", "0"]), (None, [None, None, None])],
)
def test_series_if_else_predicate_broadcast_strings(predicate_value, expected_results) -> None:
    if_true_series = Series.from_arrow(pa.array(["1", "1", "1"], type=pa.string()))
    if_false_series = Series.from_arrow(pa.array(["0", "0", "0"], type=pa.string()))
    predicate_series = Series.from_arrow(pa.array([predicate_value], type=pa.bool_()))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.string()
    assert result.to_pylist() == expected_results


@pytest.mark.parametrize(
    ["predicate_value", "expected_results"],
    [(True, [True, True, True]), (False, [False, False, False]), (None, [None, None, None])],
)
def test_series_if_else_predicate_broadcast_bools(predicate_value, expected_results) -> None:
    if_true_series = Series.from_arrow(pa.array([True, True, True], type=pa.bool_()))
    if_false_series = Series.from_arrow(pa.array([False, False, False], type=pa.bool_()))
    predicate_series = Series.from_arrow(pa.array([predicate_value], type=pa.bool_()))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.bool()
    assert result.to_pylist() == expected_results


@pytest.mark.parametrize(
    ["predicate_value", "expected_results"],
    [(True, [b"Y", b"Y", b"Y"]), (False, [b"N", b"N", b"N"]), (None, [None, None, None])],
)
def test_series_if_else_predicate_broadcast_binary(predicate_value, expected_results) -> None:
    if_true_series = Series.from_arrow(pa.array([b"Y", b"Y", b"Y"], type=pa.binary()))
    if_false_series = Series.from_arrow(pa.array([b"N", b"N", b"N"], type=pa.binary()))
    predicate_series = Series.from_arrow(pa.array([predicate_value], type=pa.bool_()))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.binary()
    assert result.to_pylist() == expected_results


@pytest.mark.parametrize(
    ["predicate_value", "expected_results"],
    [(True, [None, None, None]), (False, [None, None, None]), (None, [None, None, None])],
)
def test_series_if_else_predicate_broadcast_null(predicate_value, expected_results) -> None:
    if_true_series = Series.from_arrow(pa.array([None, None, None], type=pa.null()))
    if_false_series = Series.from_arrow(pa.array([None, None, None], type=pa.null()))
    predicate_series = Series.from_arrow(pa.array([predicate_value], type=pa.bool_()))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.null()
    assert result.to_pylist() == expected_results


def test_series_if_else_wrong_types() -> None:
    if_true_series = Series.from_arrow(pa.array([1, 1, 1], type=pa.int64()))
    if_false_series = Series.from_arrow(pa.array([0, 0, 0], type=pa.int64()))
    predicate_series = Series.from_arrow(pa.array([None], type=pa.bool_()))

    with pytest.raises(ValueError):
        predicate_series.if_else(object(), if_false_series)

    with pytest.raises(ValueError):
        predicate_series.if_else(if_true_series, object())
