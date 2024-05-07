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
    ["data", "patterns", "expected", "regex"],
    [
        # Single-character pattern.
        (["a,b,c", "d,e", "f", "g,h"], [","], [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]], False),
        # Multi-character pattern.
        (["abbcbbd", "bb", "bbe", "fbb"], ["bb"], [["a", "c", "d"], ["", ""], ["", "e"], ["f", ""]], False),
        # Empty pattern (character-splitting).
        (["foo", "bar"], [""], [["", "f", "o", "o", ""], ["", "b", "a", "r", ""]], False),
        # Single-character pattern (regex).
        (["a,b,c", "d,e", "f", "g,h"], [r","], [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]], True),
        # Multi-character pattern (regex).
        (["abbcbbd", "bb", "bbe", "fbb"], [r"b+"], [["a", "c", "d"], ["", ""], ["", "e"], ["f", ""]], True),
        # Empty pattern (regex).
        (["foo", "bar"], [r""], [["", "f", "o", "o", ""], ["", "b", "a", "r", ""]], True),
    ],
)
def test_series_utf8_split_broadcast_pattern(data, patterns, expected, regex) -> None:
    s = Series.from_arrow(pa.array(data))
    patterns = Series.from_arrow(pa.array(patterns))
    result = s.str.split(patterns, regex=regex)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "patterns", "expected", "regex"],
    [
        (["a,b,c", "a:b:c", "a;b;c", "a.b.c"], [",", ":", ";", "."], [["a", "b", "c"]] * 4, False),
        (
            ["aabbccdd"] * 4,
            ["aa", "bb", "cc", "dd"],
            [["", "bbccdd"], ["aa", "ccdd"], ["aabb", "dd"], ["aabbcc", ""]],
            False,
        ),
        (["a,b,c", "a:b:c", "a;b;c", "a.b.c"], [r",", r":", r";", r"\."], [["a", "b", "c"]] * 4, True),
        (
            ["aabbccdd"] * 4,
            [r"a+", r"b+", r"c+", r"d+"],
            [["", "bbccdd"], ["aa", "ccdd"], ["aabb", "dd"], ["aabbcc", ""]],
            True,
        ),
    ],
)
def test_series_utf8_split_multi_pattern(data, patterns, expected, regex) -> None:
    s = Series.from_arrow(pa.array(data))
    patterns = Series.from_arrow(pa.array(patterns))
    result = s.str.split(patterns, regex=regex)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "patterns", "expected", "regex"],
    [
        (
            ["aabbccdd"],
            ["aa", "bb", "cc", "dd"],
            [["", "bbccdd"], ["aa", "ccdd"], ["aabb", "dd"], ["aabbcc", ""]],
            False,
        ),
        (
            ["aabbccdd"],
            [r"a+", r"b+", r"c+", r"d+"],
            [["", "bbccdd"], ["aa", "ccdd"], ["aabb", "dd"], ["aabbcc", ""]],
            True,
        ),
    ],
)
def test_series_utf8_split_broadcast_arr(data, patterns, expected, regex) -> None:
    s = Series.from_arrow(pa.array(data))
    patterns = Series.from_arrow(pa.array(patterns))
    result = s.str.split(patterns, regex=regex)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "patterns", "expected"],
    [
        # Mixed-in nulls.
        (["a,b,c", None, "a;b;c", "a/b/c"], [",", ":", None, "/"], [["a", "b", "c"], None, None, ["a", "b", "c"]]),
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
@pytest.mark.parametrize("regex", [False, True])
def test_series_utf8_split_nulls(data, patterns, expected, regex) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(patterns, type=pa.string()))
    result = s.str.split(patterns, regex=regex)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "patterns"],
    [
        # Empty patterns.
        ([["foo"] * 4, []]),
    ],
)
@pytest.mark.parametrize("regex", [False, True])
def test_series_utf8_split_empty_arrs(data, patterns, regex) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(patterns, type=pa.string()))
    with pytest.raises(ValueError):
        s.str.split(patterns, regex=regex)


def test_series_utf8_split_empty_input() -> None:
    s = Series.from_arrow(pa.array([], type=pa.string()))
    patterns = Series.from_arrow(pa.array([","], type=pa.string()))
    assert len(s.str.split(patterns)) == 0


@pytest.mark.parametrize(
    "patterns",
    [
        # Wrong number of elements, not broadcastable
        Series.from_arrow(pa.array([",", "."], type=pa.string())),
        # Bad input type
        object(),
    ],
)
@pytest.mark.parametrize("regex", [False, True])
def test_series_utf8_split_invalid_inputs(patterns, regex) -> None:
    s = Series.from_arrow(pa.array(["a,b,c", "d, e", "f"]))
    with pytest.raises(ValueError):
        s.str.split(patterns, regex=regex)


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


@pytest.mark.parametrize(
    ["data", "expected"],
    [
        (["Foo", "BarBaz", "QUUX"], ["foo", "barbaz", "quux"]),
        # With at least one null
        (["Foo", None, "BarBaz", "QUUX"], ["foo", None, "barbaz", "quux"]),
        # With all nulls
        ([None] * 4, [None] * 4),
        # With at least one numeric strings
        (["Foo", "BarBaz", "QUUX", "2"], ["foo", "barbaz", "quux", "2"]),
        # With all numeric strings
        (["1", "2", "3"], ["1", "2", "3"]),
    ],
)
def test_series_utf8_lower(data, expected) -> None:
    s = Series.from_arrow(pa.array(data))
    result = s.str.lower()
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "expected"],
    [
        (["Foo", "BarBaz", "quux"], ["FOO", "BARBAZ", "QUUX"]),
        # With at least one null
        (["Foo", None, "BarBaz", "quux"], ["FOO", None, "BARBAZ", "QUUX"]),
        # With all nulls
        ([None] * 4, [None] * 4),
        # With at least one numeric strings
        (["Foo", "BarBaz", "quux", "2"], ["FOO", "BARBAZ", "QUUX", "2"]),
        # With all numeric strings
        (["1", "2", "3"], ["1", "2", "3"]),
    ],
)
def test_series_utf8_upper(data, expected) -> None:
    s = Series.from_arrow(pa.array(data))
    result = s.str.upper()
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "expected"],
    [
        (["\ta\t", "\nb\n", "\vc\t", "\td ", "e"], ["a\t", "b\n", "c\t", "d ", "e"]),
        # With at least one null
        (["\ta\t", None, "\vc\t", "\td ", "e"], ["a\t", None, "c\t", "d ", "e"]),
        # With all nulls
        ([None] * 4, [None] * 4),
    ],
)
def test_series_utf8_lstrip(data, expected) -> None:
    s = Series.from_arrow(pa.array(data))
    result = s.str.lstrip()
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "expected"],
    [
        (["\ta\t", "\nb\n", "\vc\t", "\td ", "e"], ["\ta", "\nb", "\vc", "\td", "e"]),
        # With at least one null
        (["\ta\t", None, "\vc\t", "\td ", "e"], ["\ta", None, "\vc", "\td", "e"]),
        # With all nulls
        ([None] * 4, [None] * 4),
    ],
)
def test_series_utf8_rstrip(data, expected) -> None:
    s = Series.from_arrow(pa.array(data))
    result = s.str.rstrip()
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "expected"],
    [
        (["abc", "def", "ghi"], ["cba", "fed", "ihg"]),
        # With at least one null
        (["abc", None, "def", "ghi"], ["cba", None, "fed", "ihg"]),
        # With all nulls
        ([None] * 4, [None] * 4),
        # With emojis
        (["😃😌😝", "abc😃😄😅"], ["😝😌😃", "😅😄😃cba"]),
        # With non-latin alphabet
        (["こんにちは", "こんにちはa", "こんにちはa😄😃"], ["はちにんこ", "aはちにんこ", "😃😄aはちにんこ"]),
    ],
)
def test_series_utf8_reverse(data, expected) -> None:
    s = Series.from_arrow(pa.array(data))
    result = s.str.reverse()
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "expected"],
    [
        (["Foo", "BarBaz", "quux"], ["Foo", "Barbaz", "Quux"]),
        # With at least one null
        (["Foo", None, "BarBaz", "quux"], ["Foo", None, "Barbaz", "Quux"]),
        # With all nulls
        ([None] * 4, [None] * 4),
        # With at least one numeric strings
        (["Foo", "BarBaz", "quux", "2"], ["Foo", "Barbaz", "Quux", "2"]),
        # With all numeric strings
        (["1", "2", "3"], ["1", "2", "3"]),
        # With empty string
        (["", "Foo", "BarBaz", "quux"], ["", "Foo", "Barbaz", "Quux"]),
        # With emojis
        (["😃😌😝", "abc😃😄😅"], ["😃😌😝", "Abc😃😄😅"]),
    ],
)
def test_series_utf8_capitalize(data, expected) -> None:
    s = Series.from_arrow(pa.array(data))
    result = s.str.capitalize()
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "pattern", "expected"],
    [
        # No broadcast
        (["foo", "barbaz", "quux"], ["foo", "bar", "baz"], [True, True, False]),
        # Broadcast pattern
        (["foo", "barbaz", "quux"], ["foo"], [True, False, False]),
        # Broadcast data
        (["foo"], ["foo", "bar", "baz"], [True, False, False]),
        # Broadcast null data
        ([None], ["foo", "bar", "baz"], [None, None, None]),
        # Broadcast null pattern
        (["foo", "barbaz", "quux"], [None], [None, None, None]),
        # Mixed-in nulls
        (["foo", None, "barbaz", "quux"], ["foo", "bar", "boo", None], [True, None, False, None]),
    ],
)
def test_series_utf8_match(data, pattern, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(pattern, type=pa.string()))
    result = s.str.match(patterns)
    assert result.to_pylist() == expected


def test_series_utf8_match_bad_pattern() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    pattern = Series.from_arrow(pa.array(["["]))
    with pytest.raises(ValueError):
        s.str.match(pattern)


@pytest.mark.parametrize(
    ["data", "nchars", "expected"],
    [
        # No Broadcast
        (["foo", "barbaz", "quux"], [0, 1, 2], ["", "b", "qu"]),
        # Broadcast nchars
        (["foo", "barbaz", "quux"], [1], ["f", "b", "q"]),
        # Broadcast data
        (["foo"], [0, 1, 2], ["", "f", "fo"]),
        # Broadcast null data
        ([None], [0, 1, 2], [None, None, None]),
        # Broadcast null nchars
        (["foo", "barbaz", "quux"], [None], [None, None, None]),
        # All Empty
        ([[], [], []]),
        # Mixed-in nulls
        (["foo", None, "barbaz", "quux"], [0, 1, 1, None], ["", None, "b", None]),
        # All null data.
        ([None] * 4, [1] * 4, [None] * 4),
        # All null nchars
        (["foo"] * 4, [None] * 4, [None] * 4),
        # Broadcasted null data.
        ([None], [1] * 4, [None] * 4),
        # Broadcasted null nchars
        (["foo"] * 4, [None], [None] * 4),
        # with emojis
        (["😃😌😝", "abc😃😄😅"], [1, 6], ["😃", "abc😃😄😅"]),
    ],
)
def test_series_utf8_left(data, nchars, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    nchars = Series.from_arrow(pa.array(nchars, type=pa.uint32()))
    result = s.str.left(nchars)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "nchars"],
    [
        # empty nchars
        ([["foo"] * 4, []]),
    ],
)
def test_series_utf8_left_empty_arrs(data, nchars) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    nchars = Series.from_arrow(pa.array(nchars, type=pa.uint32()))
    with pytest.raises(ValueError):
        s.str.left(nchars)


def test_series_utf8_left_empty_inputs() -> None:
    s = Series.from_arrow(pa.array([], type=pa.string()))
    nchars = Series.from_arrow(pa.array([5], type=pa.uint32()))
    assert len(s.str.left(nchars)) == 0


def test_series_utf8_left_mismatch_len() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    nchars = Series.from_arrow(pa.array([1, 2], type=pa.uint32()))
    with pytest.raises(ValueError):
        s.str.left(nchars)


def test_series_utf8_left_bad_nchars() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    with pytest.raises(ValueError):
        s.str.left(1)


def test_series_utf8_left_bad_nchars_dtype() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    nchars = Series.from_arrow(pa.array(["1", "2", "3"]))
    with pytest.raises(ValueError):
        s.str.left(nchars)


def test_series_utf8_left_bad_dtype() -> None:
    s = Series.from_arrow(pa.array([1, 2, 3]))
    nchars = Series.from_arrow(pa.array([1, 2, 3]))
    with pytest.raises(ValueError):
        s.str.left(nchars)


@pytest.mark.parametrize(
    ["data", "nchars", "expected"],
    [
        (["foo", "barbaz", "quux"], [0, 1, 2], ["", "z", "ux"]),
        (["foo", "barbaz", "quux"], [1], ["o", "z", "x"]),
        (["foo"], [0, 1, 2], ["", "o", "oo"]),
        ([None], [0, 1, 2], [None, None, None]),
        (["foo", "barbaz", "quux"], [None], [None, None, None]),
        ([[], [], []]),
        (["foo", None, "barbaz", "quux"], [0, 1, 1, None], ["", None, "z", None]),
        ([None] * 4, [1] * 4, [None] * 4),
        (["foo"] * 4, [None] * 4, [None] * 4),
        ([None], [1] * 4, [None] * 4),
        (["foo"] * 4, [None], [None] * 4),
        (["😃😌😝", "abc😃😄😅"], [1, 6], ["😝", "abc😃😄😅"]),
    ],
)
def test_series_utf8_right(data, nchars, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    nchars = Series.from_arrow(pa.array(nchars, type=pa.uint32()))
    result = s.str.right(nchars)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "nchars"],
    [
        # empty nchars
        ([["foo"] * 4, []]),
    ],
)
def test_series_utf8_right_empty_arrs(data, nchars) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    nchars = Series.from_arrow(pa.array(nchars, type=pa.uint32()))
    with pytest.raises(ValueError):
        s.str.right(nchars)


def test_series_utf8_right_mismatch_len() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    nchars = Series.from_arrow(pa.array([1, 2], type=pa.uint32()))
    with pytest.raises(ValueError):
        s.str.right(nchars)


def test_series_utf8_right_bad_nchars() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    with pytest.raises(ValueError):
        s.str.right(1)


def test_series_utf8_right_bad_nchars_dtype() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    nchars = Series.from_arrow(pa.array(["1", "2", "3"]))
    with pytest.raises(ValueError):
        s.str.right(nchars)


def test_series_utf8_right_bad_dtype() -> None:
    s = Series.from_arrow(pa.array([1, 2, 3]))
    nchars = Series.from_arrow(pa.array([1, 2, 3]))
    with pytest.raises(ValueError):
        s.str.right(nchars)


@pytest.mark.parametrize(
    ["data", "pattern", "expected"],
    [
        # No broadcast
        (["123", "456", "789"], [r"\d+", r"\d+", r"\d"], ["123", "456", "7"]),
        # Broadcast pattern
        (["123", "456", "789"], [r"\d+"], ["123", "456", "789"]),
        # Broadcast data
        (["123"], [r"\d+", r"\d{2}", r"\d"], ["123", "12", "1"]),
        # Broadcast null data
        ([None], [r"\d+", r"\d{2}", r"\d"], [None, None, None]),
        # Broadcast null pattern
        (["123", "456", "789"], [None], [None, None, None]),
        # Mixed in nulls
        (["123", None, "789"], [None, r"\d+", r"\d"], [None, None, "7"]),
    ],
)
def test_series_utf8_extract(data, pattern, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(pattern, type=pa.string()))
    result = s.str.extract(patterns)
    assert result.to_pylist() == expected


@pytest.mark.parametrize("data", [["abc abc"]])
@pytest.mark.parametrize("pattern", [["(a)bc"]])
@pytest.mark.parametrize(
    "index, expected",
    [
        (0, ["abc"]),
        (1, ["a"]),
        (2, [None]),
    ],
)
def test_series_utf8_extract_index(data, pattern, index, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(pattern, type=pa.string()))
    result = s.str.extract(patterns, index)
    assert result.to_pylist() == expected


def test_series_utf8_extract_bad_pattern() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    pattern = Series.from_arrow(pa.array(["["]))
    with pytest.raises(ValueError):
        s.str.extract(pattern)


@pytest.mark.parametrize(
    ["data", "pattern", "expected"],
    [
        # No broadcast
        (["1 2 3", "45 6", "789"], [r"\d+", r"\d+", r"\d"], [["1", "2", "3"], ["45", "6"], ["7", "8", "9"]]),
        # Broadcast pattern
        (["1 2 3", "45 6", "789"], [r"\d+"], [["1", "2", "3"], ["45", "6"], ["789"]]),
        # Broadcast data
        (["123"], [r"\d+", r"\d", r"\d+"], [["123"], ["1", "2", "3"], ["123"]]),
        # Broadcast null data
        ([None], [r"\d+", r"\d", r"\d+"], [None, None, None]),
        # Broadcast null pattern
        (["1 2 3", "45 6", "789"], [None], [None, None, None]),
        # Mixed in nulls
        (["1 2 3", None, "789"], [None, r"\d+", r"\d"], [None, None, ["7", "8", "9"]]),
    ],
)
def test_series_utf8_extract_all(data, pattern, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(pattern, type=pa.string()))
    result = s.str.extract_all(patterns)
    assert result.to_pylist() == expected


@pytest.mark.parametrize("data", [["abc abc"]])
@pytest.mark.parametrize("pattern", [["(a)bc"]])
@pytest.mark.parametrize(
    "index, expected",
    [
        (0, [["abc", "abc"]]),
        (1, [["a", "a"]]),
        (2, [[]]),
    ],
)
def test_series_utf8_extract_all_index(data, pattern, index, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(pattern, type=pa.string()))
    result = s.str.extract_all(patterns, index)
    assert result.to_pylist() == expected


def test_series_utf8_extract_all_bad_pattern() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    pattern = Series.from_arrow(pa.array(["["]))
    with pytest.raises(ValueError):
        s.str.extract_all(pattern)


@pytest.mark.parametrize(
    ["data", "substrs", "expected"],
    [
        # No broadcast
        (["foo", "barbaz", "quux"], ["foo", "baz", "baz"], [0, 3, -1]),
        # Broadcast substrs
        (["foo", None, "quux"], ["foo"], [0, None, -1]),
        # Broadcast data
        (["foo"], ["foo", None, "baz"], [0, None, -1]),
        # Broadcast null data
        ([None], ["foo", "bar", "baz"], [None, None, None]),
        # Broadcast null substrs
        (["foo", "barbaz", "quux"], [None], [None, None, None]),
        # All Empty
        ([[], [], []]),
        # Mixed-in nulls
        (["foo", None, "barbaz", "quux"], ["oo", "bar", "baz", None], [1, None, 3, None]),
        # All null data.
        ([None] * 4, ["foo"] * 4, [None] * 4),
        # All null substrs
        (["foo"] * 4, [None] * 4, [None] * 4),
    ],
)
def test_series_utf8_find(data, substrs, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    substrs = Series.from_arrow(pa.array(substrs, type=pa.string()))
    result = s.str.find(substrs)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "substrs"],
    [
        # Empty substrs
        ([["foo"] * 4, []]),
    ],
)
def test_series_utf8_find_empty_arrs(data, substrs) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    substrs = Series.from_arrow(pa.array(substrs, type=pa.string()))
    with pytest.raises(ValueError):
        s.str.find(substrs)


def test_series_utf8_find_mismatch_len() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    substrs = Series.from_arrow(pa.array(["foo", "baz"], type=pa.string()))
    with pytest.raises(ValueError):
        s.str.find(substrs)


def test_series_utf8_find_bad_dtype() -> None:
    s = Series.from_arrow(pa.array([1, 2, 3]))
    substrs = Series.from_arrow(pa.array(["foo", "baz", "quux"]))
    with pytest.raises(ValueError):
        s.str.find(substrs)


@pytest.mark.parametrize(
    ["data", "pattern", "replacement", "expected"],
    [
        # No broadcast
        (["foo", "barbaz", "quux"], ["o", "a", "u"], ["O", "A", "U"], ["fOO", "bArbAz", "qUUx"]),
        # Broadcast data
        (["foobar"], ["f", "o", "b"], ["F", "O", "B"], ["Foobar", "fOObar", "fooBar"]),
        # Broadcast pattern
        (["123", "12", "1"], ["1"], ["2", "3", "4"], ["223", "32", "4"]),
        # Broadcast replacement
        (["foo", "barbaz", "quux"], ["o", "a", "u"], [" "], ["f  ", "b rb z", "q  x"]),
        # Broadcast data and pattern
        (["foo"], ["o"], ["O", "A", "U"], ["fOO", "fAA", "fUU"]),
        # Broadcast data and replacement
        (["foo"], ["o", "f", " "], ["O"], ["fOO", "Ooo", "foo"]),
        # Broadcast pattern and replacement
        (["123", "12", "1"], ["1"], ["A"], ["A23", "A2", "A"]),
        # All empty
        ([], [], [], []),
    ],
)
@pytest.mark.parametrize("regex", [True, False])
def test_series_utf8_replace(data, pattern, replacement, expected, regex) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(pattern, type=pa.string()))
    replacements = Series.from_arrow(pa.array(replacement, type=pa.string()))
    result = s.str.replace(patterns, replacements, regex=regex)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "pattern", "replacement", "expected"],
    [
        # Start of string.
        (["foo", "barbaz", "quux"], ["^f", "^b", "^q"], ["F", "B", "Q"], ["Foo", "Barbaz", "Quux"]),
        # Multi-character pattern.
        (["aabbaa", "abddaaaaaaa", "acaca"], [r"a+"], ["foo"], ["foobbfoo", "foobddfoo", "foocfoocfoo"]),
    ],
)
def test_series_utf8_replace_regex(data, pattern, replacement, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(pattern, type=pa.string()))
    replacements = Series.from_arrow(pa.array(replacement, type=pa.string()))
    result = s.str.replace(patterns, replacements, regex=True)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "pattern", "replacement", "expected"],
    [
        # Broadcast null data
        ([None], ["o", "a", "u"], ["O", "A", "U"], [None, None, None]),
        # Broadcast null pattern
        (["foo", "barbaz", "quux"], [None], [" "], [None, None, None]),
        # Broadcast null replacement
        (["foo", "barbaz", "quux"], ["o", "a", "u"], [None], [None, None, None]),
        # Mixed-in nulls
        ([None, "barbaz", "quux", "1"], ["o", None, "u", "1"], ["O", "A", None, "2"], [None, None, None, "2"]),
    ],
)
@pytest.mark.parametrize("regex", [True, False])
def test_series_utf8_replace_nulls(data, pattern, replacement, expected, regex) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    patterns = Series.from_arrow(pa.array(pattern, type=pa.string()))
    replacements = Series.from_arrow(pa.array(replacement, type=pa.string()))
    result = s.str.replace(patterns, replacements, regex=regex)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "pattern", "replacement"],
    [
        # Mismatched number of patterns and replacements
        (["foo", "barbaz", "quux"], ["o", "a"], ["O"]),
        (["foo", "barbaz", "quux"], [], ["O", "A"]),
        (["foo", "barbaz", "quux"], ["o", "a"], []),
        # bad input type
        ([1, 2, 3], ["o", "a"], ["O", "A"]),
    ],
)
@pytest.mark.parametrize("regex", [True, False])
def test_series_utf8_replace_bad_inputs(data, pattern, replacement, regex) -> None:
    s = Series.from_arrow(pa.array(data))
    with pytest.raises(ValueError):
        s.str.replace(pattern, replacement, regex=regex)

    pattern = Series.from_arrow(pa.array(pattern, type=pa.string()))
    replacement = Series.from_arrow(pa.array(replacement, type=pa.string()))
    with pytest.raises(ValueError):
        s.str.replace(pattern, replacement, regex=regex)


def test_series_utf8_replace_bad_regex_pattern() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    pattern = Series.from_arrow(pa.array(["["]))
    replacement = Series.from_arrow(pa.array([" "]))
    with pytest.raises(ValueError):
        s.str.replace(pattern, replacement, regex=True)


@pytest.mark.parametrize(
    ["data", "length", "pad", "expected"],
    [
        # No Broadcast
        (["foo", "barbaz", "quux"], [5, 6, 7], [" ", ".", "-"], ["foo  ", "barbaz", "quux---"]),
        (["foo", "barbaz", "quux"], [5, 6, 7], ["-", ".", " "], ["foo--", "barbaz", "quux   "]),
        # Broadcast length
        (["foo", "barbaz", "quux"], [5], ["-", ".", " "], ["foo--", "barba", "quux "]),
        # Broadcast data
        (["foo"], [5, 6, 7], ["-", ".", " "], ["foo--", "foo...", "foo    "]),
        # Broadcast pad
        (["foo", "barbaz", "quux"], [5, 6, 7], ["-"], ["foo--", "barbaz", "quux---"]),
        # Broadcast null data
        ([None], [5, 6, 7], ["-", ".", "-"], [None, None, None]),
        # Broadcast null length
        (["foo", "barbaz", "quux"], [None], ["-", ".", " "], [None, None, None]),
        # Broadcast null pad
        (["foo", "barbaz", "quux"], [5, 6, 7], [None], [None, None, None]),
        # All Empty
        ([[], [], [], []]),
        # Mixed-in nulls
        (["foo", None, "barbaz", "quux"], [5, 6, 7, 8], ["-", ".", " ", None], ["foo--", None, "barbaz ", None]),
        # All null data.
        ([None] * 4, [5] * 4, ["-", ".", " ", "_"], [None] * 4),
        # All null length
        (["foo"] * 4, [None] * 4, ["-", ".", " ", "_"], [None] * 4),
        # All null pad
        (["foo"] * 4, [5] * 4, [None] * 4, [None] * 4),
        # With emojis
        (["😃😌😝", "abc"], [5, 6], ["😅"], ["😃😌😝😅😅", "abc😅😅😅"]),
    ],
)
def test_series_utf8_rpad(data, length, pad, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    lengths = Series.from_arrow(pa.array(length, type=pa.uint32()))
    pads = Series.from_arrow(pa.array(pad, type=pa.string()))
    result = s.str.rpad(lengths, pads)
    assert result.to_pylist() == expected


def test_series_utf8_rpad_mismatch_len() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    lengths = Series.from_arrow(pa.array([5, 6], type=pa.uint32()))
    pads = Series.from_arrow(pa.array(["-", "."], type=pa.string()))
    with pytest.raises(ValueError):
        s.str.rpad(lengths, pads)


def test_series_utf8_rpad_bad_dtype() -> None:
    s = Series.from_arrow(pa.array([1, 2, 3]))
    lengths = Series.from_arrow(pa.array([5, 6, 7], type=pa.uint32()))
    pads = Series.from_arrow(pa.array(["-", ".", " "], type=pa.string()))
    with pytest.raises(ValueError):
        s.str.rpad(lengths, pads)


def test_series_utf8_rpad_bad_lengths_dtype() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    lengths = Series.from_arrow(pa.array(["5", "6", "7"]))
    pads = Series.from_arrow(pa.array(["-", ".", " "], type=pa.string()))
    with pytest.raises(ValueError):
        s.str.rpad(lengths, pads)


def test_series_utf8_rpad_bad_pads_dtype() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    lengths = Series.from_arrow(pa.array([5, 6, 7], type=pa.uint32()))
    pads = Series.from_arrow(pa.array([1, 2, 3]))
    with pytest.raises(ValueError):
        s.str.rpad(lengths, pads)


def test_series_utf8_rpad_multichar_pad() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    lengths = Series.from_arrow(pa.array([5, 6, 7], type=pa.uint32()))
    pads = Series.from_arrow(pa.array(["--", "..", "  "], type=pa.string()))
    with pytest.raises(ValueError):
        s.str.rpad(lengths, pads)


@pytest.mark.parametrize(
    ["data", "length", "pad"],
    [
        # empty length
        (["foo", "barbaz", "quux"], [], ["-", ".", " "]),
        # empty pad
        (["foo", "barbaz", "quux"], [5, 6, 7], []),
    ],
)
def test_series_utf8_rpad_empty_arrs(data, length, pad) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    lengths = Series.from_arrow(pa.array(length, type=pa.uint32()))
    pads = Series.from_arrow(pa.array(pad, type=pa.string()))
    with pytest.raises(ValueError):
        s.str.rpad(lengths, pads)


@pytest.mark.parametrize(
    ["data", "length", "pad", "expected"],
    [
        # No Broadcast
        (["foo", "barbaz", "quux"], [5, 6, 7], [" ", ".", "-"], ["  foo", "barbaz", "---quux"]),
        (["foo", "barbaz", "quux"], [5, 4, 7], ["-", ".", " "], ["--foo", "barb", "   quux"]),
        # Broadcast length
        (["foo", "barbaz", "quux"], [5], ["-", ".", " "], ["--foo", "barba", " quux"]),
        # Broadcast data
        (["foo"], [5, 6, 7], ["-", ".", " "], ["--foo", "...foo", "    foo"]),
        # Broadcast pad
        (["foo", "barbaz", "quux"], [5, 4, 7], ["-"], ["--foo", "barb", "---quux"]),
        # Broadcast null data
        ([None], [5, 6, 7], ["-", ".", "-"], [None, None, None]),
        # Broadcast null length
        (["foo", "barbaz", "quux"], [None], ["-", ".", " "], [None, None, None]),
        # Broadcast null pad
        (["foo", "barbaz", "quux"], [5, 6, 7], [None], [None, None, None]),
        # All Empty
        ([[], [], [], []]),
        # Mixed-in nulls
        (["foo", None, "barbaz", "quux"], [5, 6, 7, 8], ["-", ".", " ", None], ["--foo", None, " barbaz", None]),
        # All null data.
        ([None] * 4, [5] * 4, ["-", ".", " ", "_"], [None] * 4),
        # All null length
        (["foo"] * 4, [None] * 4, ["-", ".", " ", "_"], [None] * 4),
        # All null pad
        (["foo"] * 4, [5] * 4, [None] * 4, [None] * 4),
        # With emojis
        (["😃😌😝", "abc"], [4, 6], ["😅"], ["😅😃😌😝", "😅😅😅abc"]),
    ],
)
def test_series_utf8_lpad(data, length, pad, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    lengths = Series.from_arrow(pa.array(length, type=pa.uint32()))
    pads = Series.from_arrow(pa.array(pad, type=pa.string()))
    result = s.str.lpad(lengths, pads)
    assert result.to_pylist() == expected


def test_series_utf8_lpad_mismatch_len() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    lengths = Series.from_arrow(pa.array([5, 6], type=pa.uint32()))
    pads = Series.from_arrow(pa.array(["-", "."], type=pa.string()))
    with pytest.raises(ValueError):
        s.str.lpad(lengths, pads)


def test_series_utf8_lpad_bad_dtype() -> None:
    s = Series.from_arrow(pa.array([1, 2, 3]))
    lengths = Series.from_arrow(pa.array([5, 6, 7], type=pa.uint32()))
    pads = Series.from_arrow(pa.array(["-", ".", " "], type=pa.string()))
    with pytest.raises(ValueError):
        s.str.lpad(lengths, pads)


def test_series_utf8_lpad_bad_lengths_dtype() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    lengths = Series.from_arrow(pa.array(["5", "6", "7"]))
    pads = Series.from_arrow(pa.array(["-", ".", " "], type=pa.string()))
    with pytest.raises(ValueError):
        s.str.lpad(lengths, pads)


def test_series_utf8_lpad_bad_pads_dtype() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    lengths = Series.from_arrow(pa.array([5, 6, 7], type=pa.uint32()))
    pads = Series.from_arrow(pa.array([1, 2, 3]))
    with pytest.raises(ValueError):
        s.str.lpad(lengths, pads)


def test_series_utf8_lpad_multichar_pad() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    lengths = Series.from_arrow(pa.array([5, 6, 7], type=pa.uint32()))
    pads = Series.from_arrow(pa.array(["--", "..", "  "], type=pa.string()))
    with pytest.raises(ValueError):
        s.str.lpad(lengths, pads)


@pytest.mark.parametrize(
    ["data", "length", "pad"],
    [
        # empty length
        (["foo", "barbaz", "quux"], [], ["-", ".", " "]),
        # empty pad
        (["foo", "barbaz", "quux"], [5, 6, 7], []),
    ],
)
def test_series_utf8_lpad_empty_arrs(data, length, pad) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    lengths = Series.from_arrow(pa.array(length, type=pa.uint32()))
    pads = Series.from_arrow(pa.array(pad, type=pa.string()))
    with pytest.raises(ValueError):
        s.str.lpad(lengths, pads)


@pytest.mark.parametrize(
    ["data", "n", "expected"],
    [
        # No Broadcast
        (["foo", "barbaz", "quux"], [0, 1, 2], ["", "barbaz", "quuxquux"]),
        # Broadcast n
        (["foo", "barbaz", "quux"], [1], ["foo", "barbaz", "quux"]),
        # Broadcast data
        (["foo"], [0, 1, 2], ["", "foo", "foofoo"]),
        # Broadcast null data
        ([None], [0, 1, 2], [None, None, None]),
        # Broadcast null n
        (["foo", "barbaz", "quux"], [None], [None, None, None]),
        # All Empty
        ([[], [], []]),
        # Mixed-in nulls
        (["foo", None, "barbaz", "quux"], [0, 1, 1, None], ["", None, "barbaz", None]),
        # All null data.
        ([None] * 4, [1] * 4, [None] * 4),
        # All null n
        (["foo"] * 4, [None] * 4, [None] * 4),
        # with emojis
        (["😃😌😝", "abc😃😄😅"], [3, 2], ["😃😌😝😃😌😝😃😌😝", "abc😃😄😅abc😃😄😅"]),
    ],
)
def test_series_utf8_repeat(data, n, expected) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    ntimes = Series.from_arrow(pa.array(n, type=pa.uint32()))
    result = s.str.repeat(ntimes)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    ["data", "n"],
    [
        # empty n
        ([["foo"] * 4, []]),
    ],
)
def test_series_utf8_repeat_empty_arrs(data, n) -> None:
    s = Series.from_arrow(pa.array(data, type=pa.string()))
    n = Series.from_arrow(pa.array(n, type=pa.uint32()))
    with pytest.raises(ValueError):
        s.str.repeat(n)


def test_series_utf8_repeat_mismatch_len() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    n = Series.from_arrow(pa.array([1, 2], type=pa.uint32()))
    with pytest.raises(ValueError):
        s.str.repeat(n)


def test_series_utf8_repeat_bad_ntype() -> None:
    s = Series.from_arrow(pa.array(["foo", "barbaz", "quux"]))
    n = Series.from_arrow(pa.array(["1", "2", "3"]))
    with pytest.raises(ValueError):
        s.str.repeat(n)


def test_series_utf8_repeat_bad_dtype() -> None:
    s = Series.from_arrow(pa.array([1, 2, 3]))
    n = Series.from_arrow(pa.array([1, 2, 3]))
    with pytest.raises(ValueError):
        s.str.repeat(n)
