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


@pytest.mark.parametrize(
    ["data", "patterns", "expected"],
    [
        # Single-character pattern.
        (["a,b,c", "d,e", "f", "g,h"], [","], [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]]),
        # Multi-character pattern.
        (["abbcbbd", "bb", "bbe", "fbb"], ["bb"], [["a", "c", "d"], ["", ""], ["", "e"], ["f", ""]]),
        # Empty pattern (character-splitting).
        (["foo", "bar"], [""], [["", "f", "o", "o", ""], ["", "b", "a", "r", ""]]),
    ],
)
def test_series_utf8_split_broadcast_pattern(data, patterns, expected) -> None:
    s = Series.from_arrow(pa.array(data))
    patterns = Series.from_arrow(pa.array(patterns))
    result = s.str.split(patterns)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "patterns", "expected"],
    [
        (["a,b,c", "a:b:c", "a;b;c", "a.b.c"], [",", ":", ";", "."], [["a", "b", "c"]] * 4),
        (["aabbccdd"] * 4, ["aa", "bb", "cc", "dd"], [["", "bbccdd"], ["aa", "ccdd"], ["aabb", "dd"], ["aabbcc", ""]]),
    ],
)
def test_series_utf8_split_multi_pattern(data, patterns, expected) -> None:
    s = Series.from_arrow(pa.array(data))
    patterns = Series.from_arrow(pa.array(patterns))
    result = s.str.split(patterns)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "patterns", "expected"],
    [
        (["aabbccdd"], ["aa", "bb", "cc", "dd"], [["", "bbccdd"], ["aa", "ccdd"], ["aabb", "dd"], ["aabbcc", ""]]),
    ],
)
def test_series_utf8_split_broadcast_arr(data, patterns, expected) -> None:
    s = Series.from_arrow(pa.array(data))
    patterns = Series.from_arrow(pa.array(patterns))
    result = s.str.split(patterns)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "patterns", "expected"],
    [
        # Mixed-in nulls.
        (["a,b,c", None, "a;b;c", "a.b.c"], [",", ":", None, "."], [["a", "b", "c"], None, None, ["a", "b", "c"]]),
        # All null data.
        ([None] * 4, [","] * 4, [None] * 4),
        # All null patterns.
        (["foo"] * 4, [None] * 4, [None] * 4),
        # Broadcasted null data.
        ([None], [","] * 4, [None] * 4),
        # Broadcasted null pattern.
        (["foo"] * 4, [None], [None] * 4),
    ],
)
def test_series_utf8_split_nulls(data, patterns, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(patterns, type=pa.string()))
    result = s.str.split(patterns)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "patterns", "expected"],
    [
        # Empty data.
        ([[], [","] * 4, []]),
        # Empty patterns.
        ([["foo"] * 4, [], []]),
    ],
)
def test_series_utf8_split_empty_arrs(data, patterns, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(patterns, type=pa.string()))
    result = s.str.split(patterns)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    "patterns",
    [
        # Wrong number of elements, not broadcastable
        Series.from_arrow(pa.array([",", "."], type=pa.string())),
        # Bad input type
        object(),
    ],
)
def test_series_utf8_split_invalid_inputs(patterns) -> None:
    s = Series.from_arrow(pa.array(["a,b,c", "d, e", "f"]))
    with pytest.raises(ValueError):
        s.str.split(patterns)


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
