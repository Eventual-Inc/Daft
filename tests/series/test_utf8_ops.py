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
    ["data", "patterns", "expected"],
    [
        # Empty data.
        ([[], [","] * 4, []]),
        # Empty patterns.
        ([["foo"] * 4, [], []]),
    ],
)
@pytest.mark.parametrize("regex", [False, True])
def test_series_utf8_split_empty_arrs(data, patterns, expected, regex) -> None:
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
        # Empty data.
        ([[], [0, 1], []]),
        # Empty nchars
        ([["foo"] * 4, [], []]),
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
        ([[], [0, 1], []]),
        ([["foo"] * 4, [], []]),
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
        # Empty data.
        ([[], ["foo", "bar"], []]),
        # Empty substrs
        ([["foo"] * 4, [], []]),
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
