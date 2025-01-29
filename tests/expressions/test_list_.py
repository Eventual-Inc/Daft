import pytest

import daft
from daft import DataType, col, list_, lit


def test_list_constructor_empty():
    with pytest.raises(Exception, match="List constructor requires at least one item"):
        df = daft.from_pydict({"x": [1, 2, 3]})
        df = df.select(list_())


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
    with pytest.raises(Exception, match="Expected all arguments to be of the same type"):
        df = daft.from_pydict({"x": [1, 2, 3], "y": [True, True, False]})
        df = df.select(list_("x", "y"))
        df.show()


def test_list_constructor_heterogeneous_with_cast():
    df = daft.from_pydict({"x": [1, 2, 3], "y": [True, True, False]})
    df = df.select(list_(col("x").cast(DataType.string()), col("y").cast(DataType.string())).alias("strs"))
    assert df.to_pydict() == {"strs": [["1", "1"], ["2", "1"], ["3", "0"]]}


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
