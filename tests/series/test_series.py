from __future__ import annotations

import copy
import gc
import os
from collections import Counter
from datetime import date, datetime

import numpy as np
import psutil
import pyarrow as pa
import pytest

from daft import DataType, Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES

IS_CI = True if os.getenv("CI") else False


class CustomTestObject:
    def __init__(self, a):
        self.a = a


def test_series_arrow_array_round_trip() -> None:
    arrow = pa.array([1, 2, 3, None, 5, None])
    s = Series.from_arrow(arrow)
    back_to_arrow = s.to_arrow()
    assert arrow == back_to_arrow


def test_series_arrow_chunked_array_round_trip() -> None:
    arrow = pa.chunked_array([[1, 2, 3], [None, 5, None]])
    s = Series.from_arrow(arrow)
    back_to_arrow = s.to_arrow()
    assert arrow.combine_chunks() == back_to_arrow


@pytest.mark.parametrize("pyobj", ["allow", "disallow", "force"])
def test_series_pylist_round_trip_objects(pyobj) -> None:
    data = [1, 2, 3, 4, None]
    s = Series.from_pylist(data, pyobj=pyobj)
    back_to_list = s.to_pylist()
    assert data == back_to_list


def test_series_pyobj_wrong_parameter() -> None:
    data = [1, 2, 3, 4, None]
    with pytest.raises(ValueError):
        Series.from_pylist(data, pyobj="blah")


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES + ARROW_STRING_TYPES)
def test_series_pylist_round_trip(dtype) -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data.cast(dtype))

    words = Counter(repr(s).split())
    assert s.name() in words
    assert words[s.name()] == 1
    assert words["None"] == 2


def test_series_pyobj_roundtrip() -> None:
    objects = [CustomTestObject(0), CustomTestObject(1), None]

    s = Series.from_pylist(objects)
    result = s.to_pylist()

    assert result[0] is objects[0]
    assert result[1] is objects[1]
    assert result[2] is None


def test_series_pyobj_strict_arrow_err() -> None:
    objects = [0, CustomTestObject(1)]

    with pytest.raises(pa.lib.ArrowInvalid):
        Series.from_pylist(objects, pyobj="disallow")


def test_series_pyobj_explicit_roundtrip() -> None:
    objects = [0, 1.1, "foo"]

    s = Series.from_pylist(objects, dtype=DataType.python())

    result = s.to_pylist()

    assert result[0] is objects[0]
    assert result[1] is objects[1]
    assert result[2] is objects[2]


def test_serialize_with_pyobjects() -> None:
    objects = [CustomTestObject(1), None]

    s = Series.from_pylist(objects)
    import pickle

    s = pickle.loads(pickle.dumps(s))

    result = s.to_pylist()

    assert isinstance(result[0], CustomTestObject)
    assert result[0].a == 1
    assert result[1] is None


def test_series_pylist_round_trip_null() -> None:
    data = pa.array([None, None])

    s = Series.from_arrow(data)

    words = Counter(repr(s).split())
    assert s.name() in words
    assert words["None"] == 2


@pytest.mark.parametrize("type", [pa.binary(), pa.binary(1)])
def test_series_pylist_round_trip_binary(type) -> None:
    data = pa.array([b"a", b"b", b"c", None, b"d", None], type=type)

    s = Series.from_arrow(data)

    words = Counter(repr(s).split())
    assert s.name() in words
    assert words[s.name()] == 1
    assert words["None"] == 2


@pytest.mark.parametrize("dtype", ARROW_FLOAT_TYPES + ARROW_INT_TYPES + ARROW_STRING_TYPES)
def test_series_pickling(dtype) -> None:
    s = Series.from_pylist([1, 2, 3, None]).cast(DataType.from_arrow_type(dtype))
    copied_s = copy.deepcopy(s)
    assert s.datatype() == copied_s.datatype()
    assert s.to_pylist() == copied_s.to_pylist()


@pytest.mark.parametrize("dtype", ARROW_FLOAT_TYPES + ARROW_INT_TYPES + ARROW_STRING_TYPES)
def test_series_bincode_serdes(dtype) -> None:
    s = Series.from_pylist([1, 2, 3, None]).cast(DataType.from_arrow_type(dtype))
    serialized = s._debug_bincode_serialize()
    copied_s = Series._debug_bincode_deserialize(serialized)

    assert s.name() == copied_s.name()
    assert s.datatype() == copied_s.datatype()
    assert s.to_pylist() == copied_s.to_pylist()


def test_series_bincode_serdes_fixed_size_binary() -> None:
    s = Series.from_arrow(pa.array([b"a", b"b", b"c", None, b"d", None], type=pa.binary(1)))
    serialized = s._debug_bincode_serialize()
    copied_s = Series._debug_bincode_deserialize(serialized)

    assert s.name() == copied_s.name()
    assert s.datatype() == copied_s.datatype()
    assert s.to_pylist() == copied_s.to_pylist()


@pytest.mark.parametrize(
    "data",
    [
        [{"a": "foo", "b": "bar"}, None, {"b": "baz", "c": "quux"}],
        [[1, 2, 3], None, [4, 5]],
        [datetime.now(), None, datetime.now()],
        [date.today(), date.today(), None],
    ],
)
def test_series_bincode_serdes_on_data(data) -> None:
    s = Series.from_arrow(pa.array(data))
    serialized = s._debug_bincode_serialize()
    copied_s = Series._debug_bincode_deserialize(serialized)

    assert s.name() == copied_s.name()
    assert s.datatype() == copied_s.datatype()
    assert s.to_pylist() == copied_s.to_pylist()


def test_series_bincode_serdes_on_ext_type(uuid_ext_type) -> None:
    arr = pa.array(f"{i}".encode() for i in range(5))
    data = pa.ExtensionArray.from_storage(uuid_ext_type, arr)
    s = Series.from_arrow(data)
    serialized = s._debug_bincode_serialize()
    copied_s = Series._debug_bincode_deserialize(serialized)

    assert s.name() == copied_s.name()
    assert s.datatype() == copied_s.datatype()
    assert s.to_pylist() == copied_s.to_pylist()


def test_series_bincode_serdes_on_complex_types() -> None:
    s = Series.from_pylist([np.ones((4, 5)), np.ones((2, 10))])
    serialized = s._debug_bincode_serialize()
    copied_s = Series._debug_bincode_deserialize(serialized)

    assert s.name() == copied_s.name()
    assert s.datatype() == copied_s.datatype()
    assert all(np.all(left == right) for left, right in zip(s.to_pylist(), copied_s.to_pylist()))


def test_series_bincode_serdes_on_null_types() -> None:
    s = Series.from_pylist([None, None, None])
    serialized = s._debug_bincode_serialize()
    copied_s = Series._debug_bincode_deserialize(serialized)

    assert s.name() == copied_s.name()
    assert s.datatype() == copied_s.datatype()
    assert s.to_pylist() == copied_s.to_pylist()


def test_series_numpy_datetime64_datetime():
    from datetime import datetime

    py_datetimes = [
        datetime(2020, 10, 1, 12, 30, 45),
        datetime(2021, 10, 2, 13, 31, 46),
        datetime(2022, 10, 3, 14, 32, 47),
        datetime(2023, 10, 4, 15, 33, 48),
        datetime(2024, 10, 5, 16, 34, 49),
        datetime(2025, 10, 6, 17, 35, 50),
    ]
    for format_ in ["s", "ms", "us", "ns"]:
        list_from_datetimes = [np.datetime64(dt, format_) for dt in py_datetimes]
        # test from_pylist
        s1 = Series.from_pylist(list_from_datetimes)
        assert s1.to_pylist() == py_datetimes
        # test from_numpy
        s2 = Series.from_numpy(np.array(list_from_datetimes))
        assert s2.to_pylist() == py_datetimes


def test_series_numpy_datetime64_date():
    from datetime import datetime

    py_datetimes = [
        datetime(2020, 10, 1, 12, 30, 45),
        datetime(2021, 10, 2, 13, 31, 46),
        datetime(2022, 10, 3, 14, 32, 47),
        datetime(2023, 10, 4, 15, 33, 48),
        datetime(2024, 10, 5, 16, 34, 49),
        datetime(2025, 10, 6, 17, 35, 50),
    ]
    py_dates = [dt.date() for dt in py_datetimes]
    for format_ in ["D"]:
        # make list of numpy datetime64
        list_from_datetimes = [np.datetime64(dt, format_) for dt in py_datetimes]
        list_from_dates = [np.datetime64(d, format_) for d in py_dates]
        # test from_pylist
        s1 = Series.from_pylist(list_from_datetimes)
        s2 = Series.from_pylist(list_from_dates)
        assert s1.to_pylist() == py_dates
        assert s2.to_pylist() == py_dates
        # test from_numpy
        s3 = Series.from_numpy(np.array(list_from_datetimes))
        s4 = Series.from_numpy(np.array(list_from_dates))
        assert s3.to_pylist() == py_dates
        assert s4.to_pylist() == py_dates


def test_series_iter() -> None:
    arrow = pa.array([1, 2, 3, None, 5, None])
    s = Series.from_arrow(arrow)
    for original, iterated in zip(arrow, s):
        assert original.as_py() == iterated


def get_memory_usage() -> float:
    """Get the current memory usage in MiB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


@pytest.mark.skipif(IS_CI, reason="Memory usage test is flaky in CI environments")
def test_series_iter_memory_efficiency() -> None:
    # Create a Series with 1 million elements.
    n = 1000_000
    arrow = pa.array(list(range(n)))
    s = Series.from_arrow(arrow)
    sum_using_iter = 0
    sum_using_pylist = 0

    # Force garbage collection before measurements.
    gc.collect()

    # Test 1: Using iterator.
    iter_memory_before = get_memory_usage()
    iter_peak_memory = 0
    for i, item in enumerate(s):
        sum_using_iter += item or 0
        # Periodically check peak memory usage.
        if i % 100_000 == 0:
            current_memory = get_memory_usage() - iter_memory_before
            iter_peak_memory = max(iter_peak_memory, current_memory)

    current_memory = get_memory_usage() - iter_memory_before
    iter_peak_memory = max(iter_peak_memory, current_memory)

    # Force garbage collection before the next test.
    gc.collect()

    # Test 2: Using to_pylist().
    pylist_memory_before = get_memory_usage()
    pylist_peak_memory = 0
    py_list = s.to_pylist()
    for i, item in enumerate(py_list):
        sum_using_pylist += item or 0
        # Periodically check peak memory usage.
        if i % 100_000 == 0:
            current_memory = get_memory_usage() - pylist_memory_before
            pylist_peak_memory = max(pylist_peak_memory, current_memory)

    current_memory = get_memory_usage() - pylist_memory_before
    pylist_peak_memory = max(pylist_peak_memory, current_memory)

    # Check correctness.
    assert sum_using_iter == sum_using_pylist
    # Assert that the iterator uses less memory.
    assert iter_peak_memory < pylist_peak_memory, "Iterator should use less memory than to_pylist()"
