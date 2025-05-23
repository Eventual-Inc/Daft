from __future__ import annotations

import daft


def test_to_pylist() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [2, 4, 3, 1]})
    res = df.to_pylist()
    assert res == [{"a": 1, "b": 2}, {"a": 2, "b": 4}, {"a": 3, "b": 3}, {"a": 4, "b": 1}]


def test_to_pylist_with_None() -> None:
    df = daft.from_pydict({"a": [None], "b": [None]})
    assert df.to_pylist() == [{"a": None, "b": None}]


def test_to_pylist_with_string() -> None:
    df = daft.from_pydict({"a": ["Hello"], "b": ["World"]})
    assert df.to_pylist() == [{"a": "Hello", "b": "World"}]


def test_to_pylist_with_int() -> None:
    df = daft.from_pydict({"a": [1, 2], "b": [3, 4]})
    assert df.to_pylist() == [{"a": 1, "b": 3}, {"a": 2, "b": 4}]


def test_to_pylist_with_int_None() -> None:
    df = daft.from_pydict({"a": [1, None, 2], "b": [3, 4, None]})
    assert df.to_pylist() == [{"a": 1, "b": 3}, {"a": None, "b": 4}, {"a": 2, "b": None}]


def test_to_pylist_with_float() -> None:
    df = daft.from_pydict({"a": [1.1, 2.2], "b": [3.3, 4.4]})
    assert df.to_pylist() == [{"a": 1.1, "b": 3.3}, {"a": 2.2, "b": 4.4}]


def test_to_pylist_with_float_None() -> None:
    df = daft.from_pydict({"a": [1.1, None, 2.2], "b": [3.3, 4.4, None]})
    assert df.to_pylist() == [{"a": 1.1, "b": 3.3}, {"a": None, "b": 4.4}, {"a": 2.2, "b": None}]


def test_to_pylist_with_empty() -> None:
    df = daft.from_pydict({"a": [], "b": []})
    assert df.to_pylist() == []
