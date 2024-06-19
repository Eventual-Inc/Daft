from __future__ import annotations

import itertools
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from daft.datatype import DataType, ImageMode, TimeUnit
from daft.exceptions import DaftCoreException
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

    target_dtype = DataType.list(DataType.from_arrow_type(dtype))

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    assert t.list.lengths().to_pylist() == [3, 3, 3, 3, 2, 2, None]

    pydata = t.to_pylist()
    assert pydata[-1] is None
    assert list(map(int, itertools.chain.from_iterable(data[:-1]))) == list(
        map(int, itertools.chain.from_iterable(pydata[:-1]))
    )


@pytest.mark.parametrize("dtype", ARROW_FLOAT_TYPES + ARROW_INT_TYPES)
def test_series_cast_python_to_fixed_size_list(dtype) -> None:
    data = [[1, 2, 3], np.arange(3), ["1", "2", "3"], [1, "2", 3.0], pd.Series([1.1, 2, 3]), (1, 2, 3), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.fixed_size_list(DataType.from_arrow_type(dtype), 3)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    assert t.list.lengths().to_pylist() == [3, 3, 3, 3, 3, 3, None]

    pydata = t.to_pylist()
    assert pydata[-1] is None
    assert list(map(int, itertools.chain.from_iterable(data[:-1]))) == list(
        map(int, itertools.chain.from_iterable(pydata[:-1]))
    )


@pytest.mark.parametrize("dtype", ARROW_FLOAT_TYPES + ARROW_INT_TYPES)
def test_series_cast_python_to_embedding(dtype) -> None:
    data = [[1, 2, 3], np.arange(3), ["1", "2", "3"], [1, "2", 3.0], pd.Series([1.1, 2, 3]), (1, 2, 3), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.embedding(DataType.from_arrow_type(dtype), 3)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    assert t.list.lengths().to_pylist() == [3, 3, 3, 3, 3, 3, None]

    pydata = t.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal([np.asarray(arr, dtype=dtype.to_pandas_dtype()) for arr in data[:-1]], pydata[:-1])


@pytest.mark.parametrize("dtype", ARROW_FLOAT_TYPES + ARROW_INT_TYPES)
def test_series_cast_list_to_embedding(dtype) -> None:
    data = [[1, 2, 3], [3, 2, 1], [4.1, 5.2, 6.3], None]
    s = Series.from_pylist(data, pyobj="disallow")

    target_dtype = DataType.embedding(DataType.from_arrow_type(dtype), 3)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    assert t.list.lengths().to_pylist() == [3, 3, 3, None]

    pydata = t.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(
        [np.asarray(arr, dtype=dtype.to_pandas_dtype()) for arr in data[:-1]],
        pydata[:-1],
    )


def test_series_cast_numpy_to_image() -> None:
    data = [
        np.arange(12, dtype=np.uint8).reshape((2, 2, 3)),
        np.arange(12, 39, dtype=np.uint8).reshape((3, 3, 3)),
        None,
    ]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB")

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    assert t.list.lengths().to_pylist() == [12, 27, None]

    pydata = t.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(data[:-1], pydata[:-1])


def test_series_cast_numpy_to_image_infer_mode() -> None:
    data = [np.arange(4, dtype=np.uint8).reshape((2, 2)), np.arange(4, 31, dtype=np.uint8).reshape((3, 3, 3)), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image()

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    assert t.list.lengths().to_pylist() == [4, 27, None]

    pydata = t.to_arrow().to_pylist()
    assert pydata[0] == {"data": data[0].ravel().tolist(), "mode": ImageMode.L, "channel": 1, "height": 2, "width": 2}
    assert pydata[1] == {"data": data[1].ravel().tolist(), "mode": ImageMode.RGB, "channel": 3, "height": 3, "width": 3}
    assert pydata[2] is None
    pydata = t.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal([np.expand_dims(data[0], -1), data[1]], pydata[:-1])


def test_series_cast_python_to_fixed_shape_image() -> None:
    height = 2
    width = 2
    shape = (height, width, 3)
    data = [np.arange(12, dtype=np.uint8).reshape(shape), np.arange(12, 24, dtype=np.uint8).reshape(shape), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB", height, width)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    assert t.list.lengths().to_pylist() == [12, 12, None]

    pydata = t.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(data[:-1], pydata[:-1])


def test_series_cast_numpy_to_tensor() -> None:
    data = [
        np.arange(12, dtype=np.uint8).reshape((2, 2, 3)),
        np.arange(12, 39, dtype=np.uint8).reshape((3, 3, 3)),
        None,
    ]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.tensor(DataType.uint8())

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    pydata = t.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(data[:-1], pydata[:-1])


def test_series_cast_numpy_to_fixed_shape_tensor() -> None:
    shape = (2, 2)
    data = [
        np.arange(4, dtype=np.uint8).reshape(shape),
        np.arange(4, 8, dtype=np.uint8).reshape(shape),
        None,
    ]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.tensor(DataType.uint8(), shape)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype
    assert len(t) == len(data)

    pydata = t.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(data[:-1], pydata[:-1])


def test_series_cast_image_to_fixed_shape_image() -> None:
    height = 2
    width = 2
    shape = (height, width, 3)
    data = [np.arange(12, dtype=np.uint8).reshape(shape), np.arange(12, 24, dtype=np.uint8).reshape(shape), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB")

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    target_dtype = DataType.image("RGB", height, width)
    u = t.cast(target_dtype)

    assert u.datatype() == target_dtype
    assert len(u) == len(data)

    pydata = u.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(pydata[:-1], data[:-1])


def test_series_cast_image_to_tensor() -> None:
    data = [
        np.arange(12, dtype=np.uint8).reshape((2, 2, 3)),
        np.arange(12, 39, dtype=np.uint8).reshape((3, 3, 3)),
        None,
    ]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB")

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    target_dtype = DataType.tensor(DataType.uint8())

    u = t.cast(target_dtype)

    assert u.datatype() == target_dtype
    assert len(u) == len(data)

    pydata = u.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(data[:-1], pydata[:-1])


def test_series_cast_image_to_fixed_shape_tensor() -> None:
    height = 2
    width = 2
    shape = (height, width, 3)
    data = [np.arange(12, dtype=np.uint8).reshape(shape), np.arange(12, 24, dtype=np.uint8).reshape(shape), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB")

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    target_dtype = DataType.tensor(DataType.uint8(), shape)
    u = t.cast(target_dtype)

    assert u.datatype() == target_dtype
    assert len(u) == len(data)

    pydata = u.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(pydata[:-1], data[:-1])


def test_series_cast_fixed_shape_image_to_image() -> None:
    height = 2
    width = 2
    shape = (height, width, 3)
    data = [np.arange(12, dtype=np.uint8).reshape(shape), np.arange(12, 24, dtype=np.uint8).reshape(shape), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB", height, width)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    target_dtype = DataType.image("RGB")
    u = t.cast(target_dtype)

    assert u.datatype() == target_dtype
    assert len(u) == len(data)

    pydata = u.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(pydata[:-1], data[:-1])


def test_series_cast_fixed_shape_image_to_fixed_shape_tensor() -> None:
    height = 2
    width = 2
    shape = (height, width, 3)
    data = [np.arange(12, dtype=np.uint8).reshape(shape), np.arange(12, 24, dtype=np.uint8).reshape(shape), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB", height, width)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    target_dtype = DataType.tensor(DataType.uint8(), shape)
    u = t.cast(target_dtype)

    assert u.datatype() == target_dtype
    assert len(u) == len(data)

    pydata = u.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(pydata[:-1], data[:-1])


def test_series_cast_fixed_shape_image_to_tensor() -> None:
    height = 2
    width = 2
    shape = (height, width, 3)
    data = [np.arange(12, dtype=np.uint8).reshape(shape), np.arange(12, 24, dtype=np.uint8).reshape(shape), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB", height, width)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    target_dtype = DataType.tensor(DataType.uint8())
    u = t.cast(target_dtype)

    assert u.datatype() == target_dtype
    assert len(u) == len(data)

    pydata = u.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(pydata[:-1], data[:-1])


def test_series_cast_fixed_shape_tensor_to_tensor() -> None:
    shape = (2, 2, 3)
    data = [np.arange(12, dtype=np.uint8).reshape(shape), np.arange(12, 24, dtype=np.uint8).reshape(shape), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.tensor(DataType.uint8(), shape)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    target_dtype = DataType.tensor(DataType.uint8())
    u = t.cast(target_dtype)

    assert u.datatype() == target_dtype
    assert len(u) == len(data)

    pydata = u.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(pydata[:-1], data[:-1])


def test_series_cast_embedding_to_fixed_shape_tensor() -> None:
    shape = (4,)
    data = [
        np.arange(4, dtype=np.uint8).reshape(shape),
        np.arange(4, 8, dtype=np.uint8).reshape(shape),
        None,
    ]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.embedding(DataType.uint8(), 4)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    target_dtype = DataType.tensor(DataType.uint8(), shape)

    u = t.cast(target_dtype)

    assert u.datatype() == target_dtype
    assert len(u) == len(data)

    pydata = u.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(data[:-1], pydata[:-1])


def test_series_cast_embedding_to_tensor() -> None:
    shape = (4,)
    data = [
        np.arange(4, dtype=np.uint8).reshape(shape),
        np.arange(4, 8, dtype=np.uint8).reshape(shape),
        None,
    ]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.embedding(DataType.uint8(), 4)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    target_dtype = DataType.tensor(DataType.uint8())

    u = t.cast(target_dtype)

    assert u.datatype() == target_dtype
    assert len(u) == len(data)

    pydata = u.to_pylist()
    assert pydata[-1] is None
    np.testing.assert_equal(data[:-1], pydata[:-1])


@pytest.mark.parametrize(
    "timeunit",
    [
        TimeUnit.s(),
        TimeUnit.ms(),
        TimeUnit.us(),
        TimeUnit.ns(),
    ],
)
@pytest.mark.parametrize(
    "timezone",
    [
        None,
        "UTC",
        "-04:00",
        "America/Los_Angeles",
    ],
)
def test_series_cast_int_timestamp(timeunit, timezone) -> None:
    # Ensure int->timestamp casting behaves identically to pyarrow.
    series = Series.from_pylist([-1, 0, 1])
    t = series.cast(DataType.timestamp(timeunit, timezone))
    assert t.to_arrow() == pa.array([-1, 0, 1]).cast(pa.timestamp(str(timeunit), timezone))

    # Ensure timestamp->int casting behaves identically to pyarrow.
    arr = pa.array([-1, 0, 1]).cast(pa.timestamp(str(timeunit), timezone))
    series = Series.from_arrow(arr)
    t = series.cast(DataType.int64())
    assert t.to_arrow() == pa.array([-1, 0, 1], type=pa.int64())


@pytest.mark.parametrize(
    ["input_t", "input", "output_t", "output"],
    [
        (
            DataType.timestamp(TimeUnit.s()),
            datetime(1970, 1, 1, 0, 0, 1),
            DataType.timestamp(TimeUnit.us()),
            datetime(1970, 1, 1, 0, 0, 1, 0),
        ),
        (
            DataType.timestamp(TimeUnit.us()),
            datetime(1970, 1, 1, 0, 0, 1, 1),
            DataType.timestamp(TimeUnit.us()),
            datetime(1970, 1, 1, 0, 0, 1, 1),
        ),
        (
            DataType.timestamp(TimeUnit.us()),
            datetime(1970, 1, 1, 0, 0, 1, 1),
            DataType.timestamp(TimeUnit.s()),
            datetime(1970, 1, 1, 0, 0, 1),
        ),
    ],
)
def test_series_cast_timestamp(input_t, input, output_t, output) -> None:
    series = Series.from_pylist([input]).cast(input_t)
    res = series.cast(output_t).to_pylist()[0]
    assert res == output


@pytest.mark.parametrize(
    ["timeunit", "sec_str"],
    [
        (TimeUnit.s(), ":01"),
        (TimeUnit.ms(), ":00.001"),
        (TimeUnit.us(), ":00.000001"),
        (TimeUnit.ns(), ":00.000000001"),
    ],
)
@pytest.mark.parametrize(
    ["timezone", "expected_dt", "tz_suffix"],
    [
        (None, "1970-01-01 00:00", ""),
        ("UTC", "1970-01-01 00:00", " +00:00"),
        ("-04:00", "1969-12-31 20:00", " -04:00"),
        ("America/Los_Angeles", "1969-12-31 16:00", " PST"),
    ],
)
def test_series_cast_timestamp_string(timeunit, sec_str, timezone, expected_dt, tz_suffix) -> None:
    # Ensure int->timestamp casting behaves identically to pyarrow
    # (except that the delimiter is ISO 8601 "T").
    arr = pa.array([1]).cast(pa.timestamp(str(timeunit), timezone))
    series = Series.from_arrow(arr)

    t = series.cast(DataType.string())
    assert t.to_pylist()[0] == f"{expected_dt}{sec_str}{tz_suffix}"


@pytest.mark.parametrize(
    ["timestamp_str", "expected", "tz"],
    [
        ("1970-01-01T01:23:45", datetime(1970, 1, 1, 1, 23, 45), None),
        ("1970-01-01T01:23:45.999999", datetime(1970, 1, 1, 1, 23, 45, 999999), None),
        ("1970-01-01T01:23:45.999999+00:00", datetime(1970, 1, 1, 1, 23, 45, 999999, tzinfo=timezone.utc), "+00:00"),
        (
            "1970-01-01T01:23:45.999999-08:00",
            datetime(1970, 1, 1, 1, 23, 45, 999999, tzinfo=timezone(timedelta(hours=-8))),
            "-08:00",
        ),
    ],
)
def test_series_cast_string_timestamp(timestamp_str, expected, tz) -> None:
    series = Series.from_pylist([timestamp_str])
    # Arrow cast only supports nanosecond timeunit for now.
    casted = series.cast(DataType.timestamp(TimeUnit.ns(), tz))
    assert casted.to_pylist() == [expected]


@pytest.mark.parametrize(
    ["dtype", "result_n1", "result_0", "result_p1"],
    [
        (
            DataType.timestamp(TimeUnit.s()),
            datetime(1969, 12, 31, 23, 59, 59),
            datetime(1970, 1, 1, 0, 0, 0),
            datetime(1970, 1, 1, 0, 0, 1),
        ),
        (
            DataType.timestamp(TimeUnit.ms()),
            datetime(1969, 12, 31, 23, 59, 59, 999000),
            datetime(1970, 1, 1, 0, 0, 0, 0),
            datetime(1970, 1, 1, 0, 0, 0, 1000),
        ),
        (
            DataType.timestamp(TimeUnit.us()),
            datetime(1969, 12, 31, 23, 59, 59, 999999),
            datetime(1970, 1, 1, 0, 0, 0, 0),
            datetime(1970, 1, 1, 0, 0, 0, 1),
        ),
        # Timezoned timestamps are broken in PyArrow 6.0.1.
        # (
        #     DataType.timestamp(TimeUnit.us(), timezone="-08:00"),
        #     datetime(1969, 12, 31, 15, 59, 59, 999999, tzinfo=timezone(timedelta(hours=-8))),
        #     datetime(1969, 12, 31, 16, 0, 0, 0, tzinfo=timezone(timedelta(hours=-8))),
        #     datetime(1969, 12, 31, 16, 0, 0, 1, tzinfo=timezone(timedelta(hours=-8))),
        # ),
        (DataType.duration(TimeUnit.s()), timedelta(seconds=-1), timedelta(seconds=0), timedelta(seconds=1)),
        (
            DataType.duration(TimeUnit.ms()),
            timedelta(milliseconds=-1),
            timedelta(milliseconds=0),
            timedelta(milliseconds=1),
        ),
        (
            DataType.duration(TimeUnit.us()),
            timedelta(microseconds=-1),
            timedelta(microseconds=0),
            timedelta(microseconds=1),
        ),
    ],
)
def test_series_cast_numeric_logical(dtype, result_n1, result_0, result_p1) -> None:
    # Numeric -> logical.
    series = Series.from_pylist([-1, 0, 1])
    casted = series.cast(dtype)
    assert casted.to_pylist() == [result_n1, result_0, result_p1]


@pytest.mark.parametrize(
    ["dtype", "result_n1", "result_0", "result_p1"],
    [
        (
            DataType.timestamp(TimeUnit.s()),
            datetime(1969, 12, 31, 23, 59, 59),
            datetime(1970, 1, 1, 0, 0, 0),
            datetime(1970, 1, 1, 0, 0, 1),
        ),
        (
            DataType.timestamp(TimeUnit.ms()),
            datetime(1969, 12, 31, 23, 59, 59, 999000),
            datetime(1970, 1, 1, 0, 0, 0, 0),
            datetime(1970, 1, 1, 0, 0, 0, 1000),
        ),
        (
            DataType.timestamp(TimeUnit.us()),
            datetime(1969, 12, 31, 23, 59, 59, 999999),
            datetime(1970, 1, 1, 0, 0, 0, 0),
            datetime(1970, 1, 1, 0, 0, 0, 1),
        ),
        (
            DataType.timestamp(TimeUnit.us(), timezone="-08:00"),
            datetime(1969, 12, 31, 23, 59, 59, 999999),
            datetime(1970, 1, 1, 0, 0, 0, 0),
            datetime(1970, 1, 1, 0, 0, 0, 1),
        ),
        # Casting between duration types is currently not supported in Arrow2.
        # (
        #     DataType.duration(TimeUnit.s()),
        #     timedelta(seconds=-1),
        #     timedelta(seconds=0),
        #     timedelta(seconds=1),
        # ),
        # (
        #     DataType.duration(TimeUnit.ms()),
        #     timedelta(milliseconds=-1),
        #     timedelta(milliseconds=0),
        #     timedelta(milliseconds=1),
        # ),
        (
            DataType.duration(TimeUnit.us()),
            timedelta(microseconds=-1),
            timedelta(microseconds=0),
            timedelta(microseconds=1),
        ),
    ],
)
def test_series_cast_logical_numeric(dtype, result_n1, result_0, result_p1) -> None:
    # Logical -> numeric.
    series = Series.from_pylist([result_n1, result_0, result_p1]).cast(dtype)
    casted = series.cast(DataType.int64())
    assert casted.to_pylist() == [-1, 0, 1]


def test_series_cast_struct_col_reordering() -> None:
    data = pa.array([{"foo": i, "bar": i} for i in range(10)], type=pa.struct({"foo": pa.int64(), "bar": pa.int64()}))
    series = Series.from_arrow(data)
    assert series.datatype() == DataType.struct({"foo": DataType.int64(), "bar": DataType.int64()})

    cast_to = DataType.struct({"bar": DataType.int32(), "foo": DataType.int32()})
    casted = series.cast(cast_to)
    assert casted.datatype() == cast_to
    assert casted.to_pylist() == data.to_pylist()


def test_series_cast_struct_prune_col() -> None:
    data = pa.array([{"foo": i, "bar": i} for i in range(10)], type=pa.struct({"foo": pa.int64(), "bar": pa.int64()}))
    series = Series.from_arrow(data)
    assert series.datatype() == DataType.struct({"foo": DataType.int64(), "bar": DataType.int64()})

    cast_to = DataType.struct({"bar": DataType.int32()})
    casted = series.cast(cast_to)
    assert casted.datatype() == cast_to
    assert casted.to_pylist() == [{"bar": x["bar"]} for x in data.to_pylist()]


def test_series_cast_struct_add_col() -> None:
    data = pa.array([{"foo": i, "bar": i} for i in range(10)], type=pa.struct({"foo": pa.int64(), "bar": pa.int64()}))
    series = Series.from_arrow(data)
    assert series.datatype() == DataType.struct({"foo": DataType.int64(), "bar": DataType.int64()})

    cast_to = DataType.struct({"bar": DataType.int32(), "foo": DataType.int32(), "baz": DataType.string()})
    casted = series.cast(cast_to)
    assert casted.datatype() == cast_to
    assert casted.to_pylist() == [{**x, "baz": None} for x in data.to_pylist()]


def test_cast_date_to_timestamp():
    from datetime import date, datetime

    import pytz

    input = Series.from_pylist([date(2022, 1, 6)])
    casted = input.cast(DataType.timestamp("us", "UTC"))
    assert casted.to_pylist() == [datetime(2022, 1, 6, tzinfo=pytz.utc)]

    back = casted.dt.date()
    assert (input == back).to_pylist() == [True]


@pytest.mark.parametrize("timeunit", ["us", "ns"])
def test_cast_timestamp_to_time(timeunit):
    from datetime import datetime, time

    input = Series.from_pylist([datetime(2022, 1, 6, 12, 34, 56, 78)])
    casted = input.cast(DataType.time(timeunit))
    assert casted.to_pylist() == [time(12, 34, 56, 78)]


@pytest.mark.parametrize("timeunit", ["s", "ms"])
def test_cast_timestamp_to_time_unsupported_timeunit(timeunit):
    from datetime import datetime

    input = Series.from_pylist([datetime(2022, 1, 6, 12, 34, 56, 78)])
    with pytest.raises(ValueError):
        input.cast(DataType.time(timeunit))


def test_cast_binary_to_fixed_size_binary():
    data = [b"abc", b"def", None, b"bcd", None]

    input = Series.from_pylist(data)
    assert input.datatype() == DataType.binary()
    casted = input.cast(DataType.fixed_size_binary(3))
    assert casted.to_pylist() == [b"abc", b"def", None, b"bcd", None]


def test_cast_binary_to_fixed_size_binary_fails_with_variable_lengths():
    data = [b"abc", b"def", None, b"bcd", None, b"long"]

    input = Series.from_pylist(data)
    with pytest.raises(DaftCoreException):
        input.cast(DataType.fixed_size_binary(3))
