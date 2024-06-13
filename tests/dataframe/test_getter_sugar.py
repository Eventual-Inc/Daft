from __future__ import annotations

import pyarrow as pa

import daft
from daft import col


def test_getter_sugar_struct():
    df = daft.from_pydict({"a": [{"x": 1, "y": "one"}, {"x": 2, "y": "two"}]})

    df = df.select("a.x", "a.y")

    assert df.to_pydict() == {"x": [1, 2], "y": ["one", "two"]}


def test_getter_sugar_map():
    pa_array = pa.array([[("a", 1)], [], [("b", 2)]], type=pa.map_(pa.string(), pa.int64()))

    df = daft.from_arrow(pa.table({"map_col": pa_array}))

    df = df.select(col("map_col.a").alias("a"), col("map_col.b").alias("b"))

    assert df.to_pydict() == {"a": [1, None, None], "b": [None, None, 2]}


def test_getter_sugar_nested_struct():
    df = daft.from_pydict({"a": [{"b": {"c": 1}}, {"b": {"c": 2}}]})

    df = df.select("a.b", "a.b.c")

    assert df.to_pydict() == {"b": [{"c": 1}, {"c": 2}], "c": [1, 2]}
