from __future__ import annotations

import pytest

import daft
import daft.exceptions
from daft import col
from daft.functions import to_struct


def test_to_struct_empty_structs():
    df = daft.from_pydict({"a": [1, 2, 3]})

    with pytest.raises(daft.exceptions.DaftCoreException, match="Cannot call struct with no inputs"):
        df.select(to_struct()).collect()


# there was a bug with pushdowns onto to_struct previously
def test_to_struct_pushdown():
    df = daft.from_pydict(
        {
            "a": [1, 2, 3, 4, None, 6, None],
            "b": ["a", "b", "c", "", "e", None, None],
        }
    )
    df = df.select(to_struct(col("a"), col("b")))
    df = df.select(df["struct"].get("a"))
    assert df.to_pydict() == {"a": [1, 2, 3, 4, None, 6, None]}


def test_to_struct_with_literal_materialized():
    df = daft.from_pydict({"a": [1, 2, 3]})
    df = df.with_column("b", daft.lit("hello"))
    result = df.select(to_struct(col("a"), col("b"))).to_pydict()
    assert result["struct"] == [
        {"a": 1, "b": "hello"},
        {"a": 2, "b": "hello"},
        {"a": 3, "b": "hello"},
    ]


def test_to_struct_with_literal_inline():
    df = daft.from_pydict({"a": [1, 2, 3]})
    result = df.select(to_struct(col("a"), daft.lit("hello").alias("b"))).to_pydict()
    assert result["struct"] == [
        {"a": 1, "b": "hello"},
        {"a": 2, "b": "hello"},
        {"a": 3, "b": "hello"},
    ]


def test_to_struct_empty_partition_with_literal():
    # An empty partition with a literal field: the scalar (len=1) must be sliced
    # to 0 rows rather than leaking through, producing a 0-row struct column.
    df = daft.from_pydict({"a": []})
    result = df.select(to_struct(col("a"), daft.lit("hello").alias("b"))).to_pydict()
    assert result["struct"] == []
