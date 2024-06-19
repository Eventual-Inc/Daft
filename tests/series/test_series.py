from __future__ import annotations

import copy
from collections import Counter
from datetime import date, datetime

import numpy as np
import pyarrow as pa
import pytest

from daft import DataType, Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES


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

    s = Series.from_pylist(objects, pyobj="force")

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
