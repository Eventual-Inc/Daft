from __future__ import annotations

import pytest

import daft
from daft import col, lit
from daft.functions import map_get, to_map


def test_to_map_basic():
    df = daft.from_pydict({"k1": ["a", "b"], "v1": [1, 2], "k2": ["c", "d"], "v2": [3, 4]})
    result = df.select(to_map((col("k1"), col("v1")), (col("k2"), col("v2"))).alias("my_map")).collect()
    maps = result.to_pydict()["my_map"]
    assert maps == [[("a", 1), ("c", 3)], [("b", 2), ("d", 4)]]


def test_to_map_single_pair():
    df = daft.from_pydict({"k": ["x", "y", "z"], "v": [10, 20, 30]})
    result = df.select(to_map((col("k"), col("v"))).alias("m")).collect()
    maps = result.to_pydict()["m"]
    assert maps == [[("x", 10)], [("y", 20)], [("z", 30)]]


def test_to_map_with_nulls():
    df = daft.from_pydict({"k": ["a", "b", None], "v": [1, None, 3]})
    result = df.select(to_map((col("k"), col("v"))).alias("m")).collect()
    maps = result.to_pydict()["m"]
    assert maps == [[("a", 1)], [("b", None)], [(None, 3)]]


def test_to_map_empty_raises():
    with pytest.raises(ValueError, match="at least one"):
        to_map()


def test_to_map_bad_tuple_raises():
    with pytest.raises(ValueError, match="tuple of .key, value."):
        to_map(col("a"))


def test_to_map_mismatched_key_types():
    df = daft.from_pydict({"k1": ["a", "b"], "v1": [1, 2], "k2": [1, 2], "v2": [3, 4]})
    with pytest.raises(Exception, match="same type"):
        df.select(to_map((col("k1"), col("v1")), (col("k2"), col("v2")))).collect()


def test_to_map_mismatched_value_types():
    df = daft.from_pydict({"k1": ["a", "b"], "v1": [1, 2], "k2": ["c", "d"], "v2": ["x", "y"]})
    with pytest.raises(Exception, match="same type"):
        df.select(to_map((col("k1"), col("v1")), (col("k2"), col("v2")))).collect()


def test_to_map_with_literals():
    df = daft.from_pydict({"v": [1, 2, 3]})
    result = df.select(to_map((lit("key"), col("v"))).alias("m")).collect()
    maps = result.to_pydict()["m"]
    assert maps == [[("key", 1)], [("key", 2)], [("key", 3)]]


def test_to_map_then_map_get():
    df = daft.from_pydict({"k1": ["a", "b"], "v1": [1, 2], "k2": ["c", "d"], "v2": [3, 4]})
    result = (
        df.select(to_map((col("k1"), col("v1")), (col("k2"), col("v2"))).alias("m"))
        .select(map_get(col("m"), "a"))
        .collect()
    )
    values = result.to_pydict()["value"]
    assert values == [1, None]
