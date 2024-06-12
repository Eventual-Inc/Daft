from __future__ import annotations

import pyarrow as pa

import daft
from daft import col


def test_get_sugar_struct():
    df = daft.from_pydict({"a": [{"x": 1, "y": "one"}, {"x": 2, "y": "two"}]})

    df = df.select("a.x", "a.y")

    assert df.to_pydict() == {"x": [1, 2], "y": ["one", "two"]}


def test_get_sugar_map():
    arrow_schema = pa.schema({"a": pa.map_(pa.string(), pa.int64())})

    table = pa.table({"a": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]}, schema=arrow_schema)

    df = daft.from_arrow(table)

    df = df.select(col("a.x").alias("x"), col("a.y").alias("y"))

    assert df.to_pydict() == {"x": [1, 3], "y": [2, 4]}


def test_get_sugar_nested_struct():
    df = daft.from_pydict({"a": [{"b": {"c": 1}}, {"b": {"c": 2}}]})

    df = df.select("a.b", "a.b.c")

    assert df.to_pydict() == {"b": [{"c": 1}, {"c": 2}], "c": [1, 2]}
