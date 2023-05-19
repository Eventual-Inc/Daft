from __future__ import annotations

import itertools

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES


@pytest.mark.parametrize("source_dtype, dest_dtype", itertools.product(ARROW_INT_TYPES + ARROW_FLOAT_TYPES, repeat=2))
def test_series_casting_numeric(source_dtype, dest_dtype) -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data.cast(source_dtype))

    assert s.datatype() == DataType.from_arrow_type(source_dtype)
    target_dtype = DataType.from_arrow_type(dest_dtype)
    t = s.cast(target_dtype)
    assert t.datatype() == target_dtype
    assert t.to_pylist() == [1, 2, 3, None, 5, None]


@pytest.mark.parametrize(
    "source_dtype, dest_dtype", itertools.product(ARROW_INT_TYPES + ARROW_FLOAT_TYPES, ARROW_STRING_TYPES)
)
def test_series_casting_to_string(source_dtype, dest_dtype) -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data.cast(source_dtype))

    assert s.datatype() == DataType.from_arrow_type(source_dtype)
    target_dtype = DataType.from_arrow_type(dest_dtype)
    t = s.cast(target_dtype)
    assert t.datatype() == target_dtype

    if pa.types.is_floating(source_dtype):
        assert t.to_pylist() == ["1.0", "2.0", "3.0", None, "5.0", None]
    else:
        assert t.to_pylist() == ["1", "2", "3", None, "5", None]


@pytest.mark.parametrize(
    "source",
    [
        [None, None, None],
        [1, 2, None],
        ["a", "b", None],
    ],
)
def test_series_cast_to_python(source) -> None:
    s = Series.from_pylist(source)
    t = s.cast(DataType.python())

    assert t.datatype() == DataType.python()
    assert t.to_pylist() == source


def test_series_python_selfcast() -> None:
    data = [object(), None, object()]
    s = Series.from_pylist(data)

    t = s.cast(DataType.python())

    assert t.datatype() == DataType.python()
    assert t.to_pylist() == data


class PycastableObject:
    def __int__(self) -> int:
        return 1

    def __float__(self) -> float:
        return 2.0

    def __str__(self) -> str:
        return "hello"

    def __bytes__(self) -> bytes:
        return b"abc"

    def __bool__(self) -> bool:
        return False


@pytest.mark.parametrize(
    ["dtype", "pytype"],
    [
        (DataType.bool(), bool),
        (DataType.int8(), int),
        (DataType.int16(), int),
        (DataType.int32(), int),
        (DataType.int64(), int),
        (DataType.uint8(), int),
        (DataType.uint16(), int),
        (DataType.uint32(), int),
        (DataType.uint64(), int),
        (DataType.float32(), float),
        (DataType.float64(), float),
        (DataType.string(), str),
        (DataType.binary(), bytes),
    ],
)
def test_series_cast_from_python(dtype, pytype) -> None:
    data = [PycastableObject(), None, PycastableObject()]
    s = Series.from_pylist(data)

    t = s.cast(dtype)

    assert t.datatype() == dtype
    expected_val = pytype(PycastableObject())
    assert t.to_pylist() == [expected_val, None, expected_val]


def test_series_cast_python_to_null() -> None:
    data = [object(), None, object()]
    s = Series.from_pylist(data)

    t = s.cast(DataType.null())

    assert t.datatype() == DataType.null()
    assert t.to_pylist() == [None, None, None]


@pytest.mark.parametrize("dtype", ARROW_FLOAT_TYPES + ARROW_INT_TYPES)
def test_series_cast_python_to_list(dtype) -> None:
    data = [[1, 2, 3], np.arange(3), ["1", "2", "3"], [1, "2", 3.0], pd.Series([1.1, 2]), (1, 2), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.list("arr", DataType.from_arrow_type(dtype))

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    assert t.arr.lengths().to_pylist() == [3, 3, 3, 3, 2, 2, None]

    pydata = t.to_pylist()
    assert pydata[-1] is None
    assert list(map(int, itertools.chain.from_iterable(data[:-1]))) == list(
        map(int, itertools.chain.from_iterable(pydata[:-1]))
    )


@pytest.mark.parametrize("dtype", ARROW_FLOAT_TYPES + ARROW_INT_TYPES)
def test_series_cast_python_to_fixed_size_list(dtype) -> None:
    data = [[1, 2, 3], np.arange(3), ["1", "2", "3"], [1, "2", 3.0], pd.Series([1.1, 2, 3]), (1, 2, 3), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.fixed_size_list("arr", DataType.from_arrow_type(dtype), 3)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    assert t.arr.lengths().to_pylist() == [3, 3, 3, 3, 3, 3, None]

    pydata = t.to_pylist()
    assert pydata[-1] is None
    assert list(map(int, itertools.chain.from_iterable(data[:-1]))) == list(
        map(int, itertools.chain.from_iterable(pydata[:-1]))
    )


@pytest.mark.parametrize("dtype", ARROW_FLOAT_TYPES + ARROW_INT_TYPES)
def test_series_cast_python_to_embedding(dtype) -> None:
    data = [[1, 2, 3], np.arange(3), ["1", "2", "3"], [1, "2", 3.0], pd.Series([1.1, 2, 3]), (1, 2, 3), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.embedding("arr", DataType.from_arrow_type(dtype), 3)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    assert t.arr.lengths().to_pylist() == [3, 3, 3, 3, 3, 3, None]

    pydata = t.to_pylist()
    assert pydata[-1] is None
    assert list(map(int, itertools.chain.from_iterable(data[:-1]))) == list(
        map(int, itertools.chain.from_iterable(pydata[:-1]))
    )
