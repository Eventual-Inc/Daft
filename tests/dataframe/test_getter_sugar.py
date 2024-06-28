from __future__ import annotations

import daft


def test_getter_sugar():
    df = daft.from_pydict({"a": [{"x": 1, "y": "one"}, {"x": 2, "y": "two"}]})

    df = df.select("a.x", "a.y")

    assert df.to_pydict() == {"x": [1, 2], "y": ["one", "two"]}


def test_getter_sugar_nested():
    df = daft.from_pydict({"a": [{"b": {"c": 1}}, {"b": {"c": 2}}]})

    df = df.select("a.b", "a.b.c")

    assert df.to_pydict() == {"b": [{"c": 1}, {"c": 2}], "c": [1, 2]}


def test_getter_sugar_nested_multiple():
    df = daft.from_pydict({"a.b": [1, 2, 3], "a": [{"b": 1}, {"b": 2}, {"b": 3}]})

    df = df.select("a", "a.b")

    assert df.to_pydict() == {"a": [{"b": 1}, {"b": 2}, {"b": 3}], "a.b": [1, 2, 3]}
