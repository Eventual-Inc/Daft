from __future__ import annotations

import pyarrow as pa
import pytest

from daft import Series
from daft.datatype import DataType


@pytest.mark.parametrize(
    ["if_true", "if_false"],
    [
        # Same length, same type
        (pa.array([1, 1, 1], type=pa.int64()), pa.array([0, 0, 0], type=pa.int64())),
        # Same length, different super-castable type
        (pa.array([1, 1, 1], type=pa.int64()), pa.array([0, 0, 0], type=pa.int8())),
        # Broadcast left
        (pa.array([1], type=pa.int64()), pa.array([0, 0, 0], type=pa.int64())),
        # Broadcast right
        (pa.array([1, 1, 1], type=pa.int64()), pa.array([0], type=pa.int64())),
    ],
)
def test_series_if_else_numeric(if_true, if_false) -> None:
    if_true_series = Series.from_arrow(if_true)
    if_false_series = Series.from_arrow(if_false)
    predicate_series = Series.from_arrow(pa.array([True, False, None]))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.int64()
    assert result.to_pylist() == [1, 0, None]


@pytest.mark.parametrize(
    ["if_true", "if_false"],
    [
        # Same length, same type
        (pa.array(["1", "1", "1"], type=pa.string()), pa.array(["0", "0", "0"], type=pa.string())),
        # Same length, different super-castable type
        (pa.array(["1", "1", "1"], type=pa.string()), pa.array([0, 0, 0], type=pa.int8())),
        # Broadcast left
        (pa.array(["1"], type=pa.string()), pa.array(["0", "0", "0"], type=pa.string())),
        # Broadcast right
        (pa.array(["1", "1", "1"], type=pa.string()), pa.array(["0"], type=pa.string())),
    ],
)
def test_series_if_else_string(if_true, if_false) -> None:
    if_true_series = Series.from_arrow(if_true)
    if_false_series = Series.from_arrow(if_false)
    predicate_series = Series.from_arrow(pa.array([True, False, None]))
    result = predicate_series.if_else(if_true_series, if_false_series)
    assert result.datatype() == DataType.string()
    assert result.to_pylist() == ["1", "0", None]


@pytest.mark.parametrize(
    ["predicate_value", "expected_results"], [(True, [1, 1, 1]), (False, [0, 0, 0]), (None, [None, None, None])]
)
def test_series_if_else_predicate_broadcast(predicate_value, expected_results) -> None:
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


def test_series_if_else_wrong_types() -> None:
    if_true_series = Series.from_arrow(pa.array([1, 1, 1], type=pa.int64()))
    if_false_series = Series.from_arrow(pa.array([0, 0, 0], type=pa.int64()))
    predicate_series = Series.from_arrow(pa.array([None], type=pa.bool_()))

    with pytest.raises(ValueError):
        predicate_series.if_else(object(), if_false_series)

    with pytest.raises(ValueError):
        predicate_series.if_else(if_true_series, object())
