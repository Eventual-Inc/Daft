from __future__ import annotations

import pytest

from daft.datatype import DataType as dt
from daft.datatype import DataTypeLike


@pytest.mark.parametrize(
    "text,dtype,expected",
    [
        # json null -> null
        ("null", dt.null(), None),
        # json null -> T
        ("null", dt.bool(), None),
        ("null", dt.int64(), None),
        ("null", dt.string(), None),
        # json number -> bool
        ("false", dt.bool(), False),
        ("true", dt.bool(), True),
        # json number -> integer
        ("42", dt.int8(), 42),
        ("42", dt.int16(), 42),
        ("42", dt.int32(), 42),
        ("42", dt.uint8(), 42),
        ("42", dt.uint16(), 42),
        ("42", dt.uint32(), 42),
        ("42", dt.uint64(), 42),
        # json number -> float (can't compare floats accurately)
        # ("3.14", dt.float64(), 3.14),
        # ("3.14", dt.float32(), 3.14),
        # json number -> decimal ()
        # ("3.14", dt.decimal128(precision=10, scale=2), 3.14),
        # text -> json string
        ('"hello"', dt.string(), "hello"),
    ],
)
def test_deserialize_json_with_scalars(deserialize, text, dtype, expected):
    assert deserialize([text], "json", dtype)[0] == expected


@pytest.mark.parametrize(
    "data_type_like",
    [
        "STRUCT<name STRING, age INT64>",
        dt.struct({"name": dt.string(), "age": dt.int64()}),
    ],
)
def test_deserialize_json_with_struct(deserialize, data_type_like: DataTypeLike):
    items = [
        '{"name": "Alice", "age": 30}',
        '{"name": "Bob", "age": 25}',
        '{"name": "Charlie", "age": 35}',
    ]
    assert deserialize(items, "json", data_type_like) == [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]


@pytest.mark.parametrize(
    "data_type_like",
    [
        "INT64[]",
        dt.list(dt.int64()),
    ],
)
def test_deserialize_json_with_list(deserialize, data_type_like: DataTypeLike):
    items = [
        "[1, 2, 3]",
        "[4, 5, 6]",
    ]
    assert deserialize(items, "json", data_type_like) == [
        [1, 2, 3],
        [4, 5, 6],
    ]


@pytest.mark.skip("our arrow2 does not yet support json deserialize on the map type.")
def test_deserialize_json_with_map(deserialize):
    items = [
        '{"a": 1, "b": 2}',
        '{"c": 3, "d": 4}',
    ]
    # MAP<STRING, INT64>
    dtype = dt.map(dt.string(), dt.int64())
    assert deserialize(items, "json", dtype) == [
        {"a": 1, "b": 2},
        {"c": 3, "d": 4},
    ]


def test_try_deserialize_json(deserialize, try_deserialize):
    items = [
        "1",
        "true",
        "null",
        '"abc"',
        "3.14",
    ]
    assert try_deserialize(items, "json", dt.int64()) == [1, 1, None, None, 3]
    assert try_deserialize(items, "json", dt.bool()) == [None, True, None, None, None]
    assert try_deserialize(items, "json", dt.string()) == ["1", "true", None, "abc", "3.14"]


def test_deserialize_json_with_missing_fields(deserialize):
    items = [
        '{"name": "Alice", "age": 30}',
        '{ "age": 25}',
        '{"name": "Charlie"}',
    ]
    dtype = dt.struct({"name": dt.string(), "age": dt.int64()})
    assert deserialize(items, "json", dtype) == [
        {"name": "Alice", "age": 30},
        {"name": None, "age": 25},
        {"name": "Charlie", "age": None},
    ]
