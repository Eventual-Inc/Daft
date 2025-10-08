from __future__ import annotations

import pytest

import daft
from daft import DataType as dt
from daft import col, list_, lit


def test_list_constructor_empty():
    with pytest.raises(Exception, match="List constructor requires at least one item"):
        df = daft.from_pydict({"x": [1, 2, 3]})
        df = df.select(list_())


def test_list_constructor_with_coercions():
    df = daft.from_pydict({"v_i32": [1, 2, 3], "v_bool": [True, True, False]})
    df = df.select(list_(lit(1), col("v_i32"), col("v_bool")))
    assert df.to_pydict() == {"list": [[1, 1, 1], [1, 2, 1], [1, 3, 0]]}


def test_list_constructor_with_lit_first():
    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    df = df.select(list_(lit(1), col("x"), col("y")))
    assert df.to_pydict() == {"list": [[1, 1, 4], [1, 2, 5], [1, 3, 6]]}


def test_list_constructor_with_lit_mid():
    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    df = df.select(list_(col("x"), lit(1), col("y")))
    assert df.to_pydict() == {"list": [[1, 1, 4], [2, 1, 5], [3, 1, 6]]}


def test_list_constructor_with_lit_last():
    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    df = df.select(list_(col("x"), col("y"), lit(1)))
    assert df.to_pydict() == {"list": [[1, 4, 1], [2, 5, 1], [3, 6, 1]]}


def test_list_constructor_multi_column():
    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    df = df.select(list_("x", "y").alias("fwd"), list_("y", "x").alias("rev"))
    assert df.to_pydict() == {"fwd": [[1, 4], [2, 5], [3, 6]], "rev": [[4, 1], [5, 2], [6, 3]]}


def test_list_constructor_different_lengths():
    with pytest.raises(Exception, match="Expected all columns to be of the same length"):
        df = daft.from_pydict({"x": [1, 2], "y": [3]})
        df = df.select(list_("x", "y"))


def test_list_constructor_singleton():
    df = daft.from_pydict({"x": [1, 2, 3]})
    df = df.select(list_(col("x")).alias("singleton"))
    assert df.to_pydict() == {"singleton": [[1], [2], [3]]}


def test_list_constructor_homogeneous():
    df = daft.from_pydict({"x": [1, 2, 3]})
    df = df.select(list_("x", col("x") * 2, col("x") * 3).alias("homogeneous"))
    assert df.to_pydict() == {"homogeneous": [[1, 2, 3], [2, 4, 6], [3, 6, 9]]}


def test_list_constructor_heterogeneous():
    df = daft.from_pydict({"x": [1, 2, 3], "y": [True, True, False]})
    df = df.select(list_("x", "y").alias("heterogeneous"))
    assert df.to_pydict() == {"heterogeneous": [[1, 1], [2, 1], [3, 0]]}


def test_list_constructor_heterogeneous_with_cast():
    df = daft.from_pydict({"x": [1, 2, 3], "y": [True, True, False]})
    df = df.select(list_(col("x").cast(dt.string()), col("y").cast(dt.string())).alias("strs"))
    assert df.to_pydict() == {"strs": [["1", "true"], ["2", "true"], ["3", "false"]]}


def test_list_constructor_mixed_null_first():
    df = daft.from_pydict({"x": [1, 2, 3]})
    df = df.select(list_(lit(None), col("x")).alias("res"))
    assert df.to_pydict() == {"res": [[None, 1], [None, 2], [None, 3]]}


def test_list_constructor_mixed_null_mid():
    df = daft.from_pydict({"x": [1, 2, 3]})
    df = df.select(list_(-1 * col("x"), lit(None), col("x")).alias("res"))
    assert df.to_pydict() == {"res": [[-1, None, 1], [-2, None, 2], [-3, None, 3]]}


def test_list_constructor_mixed_null_last():
    df = daft.from_pydict({"x": [1, 2, 3]})
    df = df.select(list_(col("x"), lit(None)).alias("res"))
    assert df.to_pydict() == {"res": [[1, None], [2, None], [3, None]]}


def test_list_constructor_all_nulls():
    df = daft.from_pydict({"x": [1, 2, 3]})
    df = df.select(list_(lit(None), lit(None)).alias("res"))
    assert df.to_pydict() == {"res": [[None, None], [None, None], [None, None]]}
