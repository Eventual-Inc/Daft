from __future__ import annotations

import pyarrow as pa
import pytest

from daft import Series


@pytest.mark.parametrize(
    ["funcname", "data"],
    [
        ("endswith", ["x_foo", "y_foo", "z_bar"]),
        ("startswith", ["foo_x", "foo_y", "bar_z"]),
        ("contains", ["x_foo_x", "y_foo_y", "z_bar_z"]),
    ],
)
def test_series_utf8_compare(funcname, data) -> None:
    s = Series.from_arrow(pa.array(data))
    pattern = Series.from_arrow(pa.array(["foo", "bar", "bar"]))
    result = getattr(s.str, funcname)(pattern)
    assert result.to_pylist() == [True, False, True]


@pytest.mark.parametrize(
    ["funcname", "data"],
    [
        ("endswith", ["x_foo", "y_foo", "z_bar"]),
        ("startswith", ["foo_x", "foo_y", "bar_z"]),
        ("contains", ["x_foo_x", "y_foo_y", "z_bar_z"]),
    ],
)
def test_series_utf8_compare_pattern_broadcast(funcname, data) -> None:
    s = Series.from_arrow(pa.array(data))
    pattern = Series.from_arrow(pa.array(["foo"]))
    result = getattr(s.str, funcname)(pattern)
    assert result.to_pylist() == [True, True, False]


@pytest.mark.parametrize(
    ["funcname", "data"],
    [
        ("endswith", ["x_foo", "y_foo", "z_bar"]),
        ("startswith", ["foo_x", "foo_y", "bar_z"]),
        ("contains", ["x_foo_x", "y_foo_y", "z_bar_z"]),
    ],
)
def test_series_utf8_compare_pattern_broadcast_null(funcname, data) -> None:
    s = Series.from_arrow(pa.array(["x_foo", "y_foo", "z_bar"]))
    pattern = Series.from_arrow(pa.array([None], type=pa.string()))
    result = getattr(s.str, funcname)(pattern)
    assert result.to_pylist() == [None, None, None]


@pytest.mark.parametrize(
    ["funcname", "data"], [("endswith", ["x_foo"]), ("startswith", ["foo_x"]), ("contains", ["x_foo_x"])]
)
def test_series_utf8_compare_data_broadcast(funcname, data) -> None:
    s = Series.from_arrow(pa.array(data))
    pattern = Series.from_arrow(pa.array(["foo", "bar", "baz"]))
    result = getattr(s.str, funcname)(pattern)
    assert result.to_pylist() == [True, False, False]


@pytest.mark.parametrize("funcname", ["endswith", "startswith", "contains"])
def test_series_utf8_compare_data_broadcast_null(funcname) -> None:
    s = Series.from_arrow(pa.array([None], type=pa.string()))
    pattern = Series.from_arrow(pa.array(["foo", "bar", "baz"]))
    result = getattr(s.str, funcname)(pattern)
    assert result.to_pylist() == [None, None, None]


@pytest.mark.parametrize("funcname", ["endswith", "startswith", "contains"])
def test_series_utf8_compare_nulls(funcname) -> None:
    s = Series.from_arrow(pa.array([None, None, "z_bar"]))
    pattern = Series.from_arrow(pa.array([None, "bar", None]))
    result = getattr(s.str, funcname)(pattern)
    assert result.to_pylist() == [None, None, None]


@pytest.mark.parametrize("funcname", ["endswith", "startswith", "contains"])
def test_series_utf8_compare_empty(funcname) -> None:
    s = Series.from_arrow(pa.array([], type=pa.string()))
    pattern = Series.from_arrow(pa.array([], type=pa.string()))
    result = getattr(s.str, funcname)(pattern)
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
@pytest.mark.parametrize("funcname", ["endswith", "startswith", "contains"])
def test_series_utf8_compare_invalid_inputs(funcname, bad_series) -> None:
    s = Series.from_arrow(pa.array(["x_foo", "y_foo", "z_bar"]))
    with pytest.raises(ValueError):
        getattr(s.str, funcname)(bad_series)


def test_series_utf8_length() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    result = s.str.length()
    assert result.to_pylist() == [3, 6, 4]


def test_series_utf8_length_with_nulls() -> None:
    s = Series.from_arrow(pa.array(["foo", None, "barbaz", "quux"]))
    result = s.str.length()
    assert result.to_pylist() == [3, None, 6, 4]


def test_series_utf8_length_empty() -> None:
    s = Series.from_arrow(pa.array([], type=pa.string()))
    result = s.str.length()
    assert result.to_pylist() == []


def test_series_utf8_length_all_null() -> None:
    s = Series.from_arrow(pa.array([None, None, None]))
    result = s.str.length()
    assert result.to_pylist() == [None, None, None]
