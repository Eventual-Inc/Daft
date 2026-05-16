from __future__ import annotations

import pytest

import daft
from daft.functions import json_array_length, json_object_keys, json_tuple

# ---------------------------------------------------------------------------
# json_array_length
# ---------------------------------------------------------------------------


def test_json_array_length_basic():
    df = daft.from_pydict(
        {
            "col": [
                "[1, 2, 3]",
                "[]",
                '["a", "b", "c", "d"]',
                "[null, null]",
            ]
        }
    )
    result = df.select(json_array_length(df["col"]).alias("len")).to_pydict()
    assert result["len"] == [3, 0, 4, 2]


def test_json_array_length_non_array_returns_null():
    df = daft.from_pydict(
        {
            "col": [
                '{"a": 1}',  # object, not array
                '"plain string"',  # string scalar
                "42",  # number scalar
                "true",  # bool scalar
            ]
        }
    )
    result = df.select(json_array_length(df["col"]).alias("len")).to_pydict()
    assert result["len"] == [None, None, None, None]


def test_json_array_length_invalid_and_null():
    df = daft.from_pydict({"col": [None, "not json", "[", "[1, 2]"]})
    result = df.select(json_array_length(df["col"]).alias("len")).to_pydict()
    assert result["len"] == [None, None, None, 2]


def test_json_array_length_nested_array_outermost_only():
    df = daft.from_pydict({"col": ["[[1, 2], [3, 4, 5], []]"]})
    result = df.select(json_array_length(df["col"]).alias("len")).to_pydict()
    assert result["len"] == [3]


def test_json_array_length_sql():
    df = daft.from_pydict({"col": ["[1, 2, 3]", '{"a": 1}', None]})
    result = daft.sql("SELECT json_array_length(col) AS len FROM df", df=df).to_pydict()
    assert result["len"] == [3, None, None]


# ---------------------------------------------------------------------------
# json_object_keys
# ---------------------------------------------------------------------------


def test_json_object_keys_basic():
    df = daft.from_pydict(
        {
            "col": [
                '{"a": 1, "b": 2, "c": 3}',
                "{}",
                '{"x": "hello"}',
            ]
        }
    )
    result = df.select(json_object_keys(df["col"]).alias("keys")).to_pydict()
    # JSON object key order is preserved by serde_json (insertion-order is not
    # guaranteed by default; rely on set equality for the multi-key case).
    assert sorted(result["keys"][0]) == ["a", "b", "c"]
    assert result["keys"][1] == []
    assert result["keys"][2] == ["x"]


def test_json_object_keys_non_object_returns_null():
    df = daft.from_pydict(
        {
            "col": [
                "[1, 2, 3]",  # array
                '"plain"',  # string
                "42",  # number
                "true",  # bool
            ]
        }
    )
    result = df.select(json_object_keys(df["col"]).alias("keys")).to_pydict()
    assert result["keys"] == [None, None, None, None]


def test_json_object_keys_invalid_and_null():
    df = daft.from_pydict({"col": [None, "not json", '{"a": 1}']})
    result = df.select(json_object_keys(df["col"]).alias("keys")).to_pydict()
    assert result["keys"][0] is None
    assert result["keys"][1] is None
    assert result["keys"][2] == ["a"]


def test_json_object_keys_nested_only_top_level():
    df = daft.from_pydict({"col": ['{"a": {"b": 1, "c": 2}, "d": [1, 2]}']})
    result = df.select(json_object_keys(df["col"]).alias("keys")).to_pydict()
    assert sorted(result["keys"][0]) == ["a", "d"]


def test_json_object_keys_sql():
    df = daft.from_pydict({"col": ['{"a": 1, "b": 2}', "[]", None]})
    result = daft.sql("SELECT json_object_keys(col) AS keys FROM df", df=df).to_pydict()
    assert sorted(result["keys"][0]) == ["a", "b"]
    assert result["keys"][1] is None
    assert result["keys"][2] is None


# ---------------------------------------------------------------------------
# json_tuple
# ---------------------------------------------------------------------------


def test_json_tuple_basic():
    df = daft.from_pydict(
        {
            "col": [
                '{"a": 1, "b": "x", "c": true}',
                '{"a": 2, "b": "y"}',
                '{"a": 3}',
            ]
        }
    )
    df = df.with_column("t", json_tuple(df["col"], "a", "b", "c"))
    df = df.select(
        df["t"].get("a").alias("a"),
        df["t"].get("b").alias("b"),
        df["t"].get("c").alias("c"),
    )
    result = df.to_pydict()
    assert result["a"] == ["1", "2", "3"]
    assert result["b"] == ["x", "y", None]
    assert result["c"] == ["true", None, None]


def test_json_tuple_string_values_unquoted():
    """Spark-compatible behavior: string values are returned without surrounding quotes."""
    df = daft.from_pydict({"col": ['{"name": "Alice"}']})
    df = df.with_column("t", json_tuple(df["col"], "name"))
    result = df.select(df["t"].get("name").alias("name")).to_pydict()
    assert result["name"] == ["Alice"]


def test_json_tuple_nested_returned_as_json():
    """Nested objects/arrays are returned as compact JSON strings."""
    df = daft.from_pydict({"col": ['{"obj": {"x": 1}, "arr": [1, 2, 3]}']})
    df = df.with_column("t", json_tuple(df["col"], "obj", "arr"))
    df = df.select(
        df["t"].get("obj").alias("obj"),
        df["t"].get("arr").alias("arr"),
    )
    result = df.to_pydict()
    assert result["obj"] == ['{"x":1}']
    assert result["arr"] == ["[1,2,3]"]


def test_json_tuple_invalid_and_null():
    df = daft.from_pydict(
        {
            "col": [
                None,
                "not json",
                "[1, 2]",  # not object
                '{"a": 1}',
            ]
        }
    )
    df = df.with_column("t", json_tuple(df["col"], "a", "b"))
    df = df.select(
        df["t"].get("a").alias("a"),
        df["t"].get("b").alias("b"),
    )
    result = df.to_pydict()
    assert result["a"] == [None, None, None, "1"]
    assert result["b"] == [None, None, None, None]


def test_json_tuple_requires_at_least_one_field():
    df = daft.from_pydict({"col": ['{"a": 1}']})
    with pytest.raises(ValueError, match="at least one field name"):
        df.select(json_tuple(df["col"]))


def test_json_tuple_sql():
    df = daft.from_pydict({"col": ['{"a": 1, "b": "x"}']})
    result = daft.sql("SELECT json_tuple(col, 'a', 'b') AS t FROM df", df=df).to_pydict()
    # Result is a struct column with fields "a" and "b".
    assert result["t"][0] == {"a": "1", "b": "x"}
