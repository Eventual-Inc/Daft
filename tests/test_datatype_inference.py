from __future__ import annotations

import datetime
import decimal

import numpy as np
import numpy.typing as npt
import pandas
import PIL.Image
import pytest
from typing_extensions import TypedDict

from daft import DataType as dt
from daft import Series
from daft.daft import ImageMode
from daft.datatype import TimeUnit
from daft.file import File


@pytest.mark.parametrize(
    "user_provided_type, expected_datatype",
    [
        (type(None), dt.null()),
        (bool, dt.bool()),
        (str, dt.string()),
        (bytes, dt.binary()),
        (int, dt.int64()),
        (float, dt.float64()),
        (datetime.datetime, dt.timestamp(TimeUnit.us())),
        (datetime.date, dt.date()),
        (datetime.time, dt.time(TimeUnit.us())),
        (datetime.timedelta, dt.duration(TimeUnit.us())),
        (list, dt.list(dt.python())),
        (list[str], dt.list(dt.string())),
        (list[list], dt.list(dt.list(dt.python()))),
        (list[list[str]], dt.list(dt.list(dt.string()))),
        (TypedDict("Foobar", {"foo": str, "bar": int}), dt.struct({"foo": dt.string(), "bar": dt.int64()})),
        (dict, dt.map(dt.python(), dt.python())),
        (dict[str, str], dt.map(dt.string(), dt.string())),
        (tuple, dt.list(dt.python())),
        (tuple[str, ...], dt.list(dt.string())),
        (tuple[str, int], dt.struct({"_0": dt.string(), "_1": dt.int64()})),
        (np.ndarray, dt.tensor(dt.python())),
        (npt.NDArray[int], dt.tensor(dt.int64())),
        (np.bool_, dt.bool()),
        (np.int8, dt.int8()),
        (np.uint8, dt.uint8()),
        (np.int16, dt.int16()),
        (np.uint16, dt.uint16()),
        (np.int32, dt.int32()),
        (np.uint32, dt.uint32()),
        (np.int64, dt.int64()),
        (np.uint64, dt.uint64()),
        (np.float32, dt.float32()),
        (np.float64, dt.float64()),
        (np.datetime64, dt.timestamp(TimeUnit.us())),
        (pandas.Series, dt.list(dt.python())),
        (PIL.Image.Image, dt.image()),
        (Series, dt.list(dt.python())),
        (File, dt.file()),
        (object, dt.python()),
    ],
)
def test_infer_from_type(user_provided_type, expected_datatype):
    actual = dt.infer_from_type(user_provided_type)
    assert actual == expected_datatype


@pytest.mark.parametrize(
    "user_provided_object, expected_datatype",
    [
        (None, dt.null()),
        (False, dt.bool()),
        ("a", dt.string()),
        (b"a", dt.binary()),
        (1, dt.int64()),
        (2**63, dt.uint64()),
        (1.0, dt.float64()),
        (datetime.datetime.now(), dt.timestamp(TimeUnit.us())),
        (datetime.date.today(), dt.date()),
        (datetime.time(microsecond=1), dt.time(TimeUnit.us())),
        (datetime.timedelta(microseconds=1), dt.duration(TimeUnit.us())),
        ([], dt.list(dt.null())),
        (["a", "b", "c"], dt.list(dt.string())),
        ({}, dt.struct({"": dt.null()})),
        ({"foo": "1", "bar": 2}, dt.struct({"foo": dt.string(), "bar": dt.int64()})),
        ({1: 2, 3: 4}, dt.map(dt.int64(), dt.int64())),
        ((), dt.struct({"": dt.null()})),
        (("0", 1), dt.struct({"_0": dt.string(), "_1": dt.int64()})),
        (decimal.Decimal("1.5"), dt.decimal128(38, 1)),
        (np.array([1, 2, 3]), dt.tensor(dt.int64())),
        (np.bool_(False), dt.bool()),
        (np.int8(1), dt.int8()),
        (np.uint8(1), dt.uint8()),
        (np.int16(1), dt.int16()),
        (np.uint16(1), dt.uint16()),
        (np.int32(1), dt.int32()),
        (np.uint32(1), dt.uint32()),
        (np.int64(1), dt.int64()),
        (np.uint64(1), dt.uint64()),
        (np.float32(1.0), dt.float32()),
        (np.float64(1.0), dt.float64()),
        (np.datetime64(1, "Y"), dt.date()),
        (np.datetime64(1, "M"), dt.date()),
        (np.datetime64(1, "W"), dt.date()),
        (np.datetime64(1, "D"), dt.date()),
        (np.datetime64(1, "h"), dt.timestamp(TimeUnit.s())),
        (np.datetime64(1, "m"), dt.timestamp(TimeUnit.s())),
        (np.datetime64(1, "s"), dt.timestamp(TimeUnit.s())),
        (np.datetime64(1, "ms"), dt.timestamp(TimeUnit.ms())),
        (np.datetime64(1, "us"), dt.timestamp(TimeUnit.us())),
        (np.datetime64(1, "ns"), dt.timestamp(TimeUnit.ns())),
        (np.datetime64(1, "ps"), dt.timestamp(TimeUnit.ns())),
        (pandas.Series([1, 2, 3]), dt.list(dt.int64())),
        (PIL.Image.new("L", (10, 20)), dt.image(ImageMode.L)),
        (PIL.Image.new("LA", (10, 20)), dt.image(ImageMode.LA)),
        (PIL.Image.new("RGB", (10, 20)), dt.image(ImageMode.RGB)),
        (PIL.Image.new("RGBA", (10, 20)), dt.image(ImageMode.RGBA)),
        (Series.from_pylist([1, 2, 3]), dt.list(dt.int64())),
        (File(b"1234"), dt.file()),
        (object(), dt.python()),
    ],
)
def test_infer_from_object(user_provided_object, expected_datatype):
    actual = dt.infer_from_object(user_provided_object)
    assert actual == expected_datatype


@pytest.mark.parametrize(
    "user_provided_object, expected_datatype",
    [
        # Nested lists
        ([[1, 2], [3, 4]], dt.list(dt.list(dt.int64()))),
        ([["a", "b"], ["c", "d"]], dt.list(dt.list(dt.string()))),
        ([[[1]], [[2, 3]]], dt.list(dt.list(dt.list(dt.int64())))),
        # Mixed nested lists
        ([[1, 2], ["a", "b"]], dt.list(dt.list(dt.string()))),
        # Nested structs
        ({"outer": {"inner": 1}}, dt.struct({"outer": dt.struct({"inner": dt.int64()})})),
        ({"a": {"b": {"c": "nested"}}}, dt.struct({"a": dt.struct({"b": dt.struct({"c": dt.string()})})})),
        # Mixed nested types - list of structs
        (
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
            dt.list(dt.struct({"name": dt.string(), "age": dt.int64()})),
        ),
        # Mixed nested types - struct with lists
        (
            {"numbers": [1, 2, 3], "strings": ["a", "b"]},
            dt.struct({"numbers": dt.list(dt.int64()), "strings": dt.list(dt.string())}),
        ),
        # Complex nested structures
        (
            {"users": [{"profile": {"name": "Alice"}, "scores": [1, 2, 3]}]},
            dt.struct(
                {
                    "users": dt.list(
                        dt.struct({"profile": dt.struct({"name": dt.string()}), "scores": dt.list(dt.int64())})
                    )
                }
            ),
        ),
        # Nested tuples as structs
        ((1, (2, 3)), dt.struct({"_0": dt.int64(), "_1": dt.struct({"_0": dt.int64(), "_1": dt.int64()})})),
        # Empty nested structures
        ([[], []], dt.list(dt.list(dt.null()))),
        ([{}, {}], dt.list(dt.struct({"": dt.null()}))),
    ],
)
def test_infer_from_nested_object(user_provided_object, expected_datatype):
    actual = dt.infer_from_object(user_provided_object)
    assert actual == expected_datatype


@pytest.mark.parametrize(
    "obj",
    [
        None,
        False,
        "a",
        b"a",
        1,
        2**63,
        1.0,
        datetime.datetime.now(),
        datetime.date.today(),
        datetime.time(microsecond=1),
        datetime.timedelta(microseconds=1),
        [],
        ["a", "b", "c"],
        {},
        {"foo": "1", "bar": 2},
        decimal.Decimal("1.5"),
        object(),
        # Nested structures for roundtrip testing
        [[1, 2], [3, 4]],
        [["a", "b"], ["c", "d"]],
        {"outer": {"inner": 1}},
        [{"name": "Alice", "age": 30}],
        {"numbers": [1, 2, 3]},
    ],
)
def test_roundtrippable(obj):
    input = [None, obj, None]
    s = Series.from_pylist(input)
    assert input == s.to_pylist()


@pytest.mark.parametrize(
    "arr",
    [
        np.array([1, 2, 3], dtype=np.int64),
        np.array([1.0, 2.0, 3.0], dtype=np.float64),
        np.array([[1, 2], [3, 4]], dtype=np.int64),
        np.array([["a", "b"], ["c", "d"]], dtype=object),
        np.array([], dtype=np.float64),
        np.array([True, False, True], dtype=bool),
        np.array([1, 2, 3], dtype=np.uint8),
        np.array([1, 2, 3], dtype=np.int8),
        np.array([1, 2, 3], dtype=np.uint16),
        np.array([1, 2, 3], dtype=np.int16),
        np.array([1, 2, 3], dtype=np.uint32),
        np.array([1, 2, 3], dtype=np.int32),
        np.array([1, 2, 3], dtype=np.uint64),
        np.array([1, 2, 3], dtype=np.float32),
    ],
)
def test_roundtrippable_numpy(arr):
    input = [None, arr, None]
    s = Series.from_pylist(input)
    pre, out_arr, post = s.to_pylist()
    assert pre is None
    assert post is None
    assert (out_arr == arr).all()
