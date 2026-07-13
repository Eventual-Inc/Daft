from __future__ import annotations

import pytest

import daft
from daft import col
from daft.functions import to_map


def test_map_constructor_empty():
    with pytest.raises(ValueError, match="Map constructor requires at least one key/value pair"):
        to_map()


def test_map_constructor_odd_positional_args():
    with pytest.raises(ValueError, match="Map constructor requires an even number of positional key/value arguments"):
        to_map("a", col("x"), "b")


def test_map_constructor_positional_pairs():
    df = daft.from_pydict({"k1": ["a", "b"], "v1": [1, 2], "k2": ["c", "d"], "v2": [3, 4]})

    result = df.select(to_map(col("k1"), col("v1"), col("k2"), col("v2")).alias("m")).to_pydict(
        maps_as_pydicts="lossy"
    )

    assert result == {"m": [{"a": 1, "c": 3}, {"b": 2, "d": 4}]}


def test_map_constructor_keyword_pairs():
    df = daft.from_pydict({"x": [1, 2], "y": [3, 4]})

    result = df.select(to_map(a=col("x"), b=col("y")).alias("m")).to_pydict(maps_as_pydicts="lossy")

    assert result == {"m": [{"a": 1, "b": 3}, {"a": 2, "b": 4}]}


def test_map_constructor_mixed_pairs_and_map_get():
    df = daft.from_pydict({"k": ["a", "b"], "v": [1, 2], "lookup": ["c", "static"]})
    map_expr = to_map(col("k"), col("v"), c=col("v") * 10, static=5)

    result = df.select(map_expr.map_get(col("lookup")).alias("value")).to_pydict()

    assert result == {"value": [10, 5]}


def test_map_constructor_value_coercion():
    df = daft.from_pydict({"k": ["a", "b"], "x": [1, 2], "y": [1.5, 2.5]})

    result = df.select(to_map(col("k"), col("x"), "other", col("y")).alias("m")).to_pydict(
        maps_as_pydicts="lossy"
    )

    assert result == {"m": [{"a": 1.0, "other": 1.5}, {"b": 2.0, "other": 2.5}]}


def test_map_constructor_literal_entries_empty_input():
    df = daft.from_pydict({"x": []})

    result = df.select(to_map("key", 1).alias("m")).to_pydict(maps_as_pydicts="lossy")

    assert result == {"m": []}
