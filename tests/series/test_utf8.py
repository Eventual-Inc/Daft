from __future__ import annotations

import pyarrow as pa
import pytest

from daft import Series


def test_series_endswith() -> None:
    s = Series.from_arrow(pa.array(["x_foo", "y_foo", "z_bar"]))
    pattern = Series.from_arrow(pa.array(["foo", "bar", "bar"]))
    result = s.str.endswith(pattern)
    assert result.to_pylist() == [True, False, True]


def test_series_endswith_pattern_broadcast() -> None:
    s = Series.from_arrow(pa.array(["x_foo", "y_foo", "z_bar"]))
    pattern = Series.from_arrow(pa.array(["foo"]))
    result = s.str.endswith(pattern)
    assert result.to_pylist() == [True, True, False]


def test_series_endswith_pattern_broadcast_null() -> None:
    s = Series.from_arrow(pa.array(["x_foo", "y_foo", "z_bar"]))
    pattern = Series.from_arrow(pa.array([None], type=pa.string()))
    result = s.str.endswith(pattern)
    assert result.to_pylist() == [None, None, None]


def test_series_endswith_data_broadcast() -> None:
    s = Series.from_arrow(pa.array(["x_foo"]))
    pattern = Series.from_arrow(pa.array(["foo", "bar", "baz"]))
    result = s.str.endswith(pattern)
    assert result.to_pylist() == [True, False, False]


def test_series_endswith_data_broadcast_null() -> None:
    s = Series.from_arrow(pa.array([None], type=pa.string()))
    pattern = Series.from_arrow(pa.array(["foo", "bar", "baz"]))
    result = s.str.endswith(pattern)
    assert result.to_pylist() == [None, None, None]


def test_series_endswith_nulls() -> None:
    s = Series.from_arrow(pa.array([None, None, "z_bar"]))
    pattern = Series.from_arrow(pa.array([None, "bar", None]))
    result = s.str.endswith(pattern)
    assert result.to_pylist() == [None, None, None]


def test_series_endswith_empty() -> None:
    s = Series.from_arrow(pa.array([], type=pa.string()))
    pattern = Series.from_arrow(pa.array([], type=pa.string()))
    result = s.str.endswith(pattern)
    assert result.to_pylist() == []


@pytest.mark.parametrize(
    "bad_series",
    [
        # Wrong number of elements, not broadcastable
        Series.from_arrow(pa.array([], type=pa.string())),
        Series.from_arrow(pa.array(["foo", "bar"], type=pa.string())),
        # Bad input type
        object(),
    ],
)
def test_series_endswith_invalid_inputs(bad_series) -> None:
    s = Series.from_arrow(pa.array(["x_foo", "y_foo", "z_bar"]))
    with pytest.raises(ValueError):
        s.str.endswith(bad_series)
