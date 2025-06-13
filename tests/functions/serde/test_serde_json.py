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


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_serialize_json(series):
    # sanity check..
    items = [1, 2, 3]
    assert series(items).serialize("json") == ["1", "2", "3"]


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_serialize_json_struct(series):
    items = [
        {"a": None, "b": 1, "c": 1.1, "d": "ABC"},
        {"a": None, "b": 2, "c": 2.2, "d": "DEF"},
        {"a": None, "b": 3, "c": 3.3, "d": "GHI"},
        {"a": None, "b": 4, "c": 4.4, "d": "JKL"},
    ]
    assert series(items).serialize("json") == [
        '{"a":null,"b":1,"c":1.1,"d":"ABC"}',
        '{"a":null,"b":2,"c":2.2,"d":"DEF"}',
        '{"a":null,"b":3,"c":3.3,"d":"GHI"}',
        '{"a":null,"b":4,"c":4.4,"d":"JKL"}',
    ]


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_serialize_json_with_nulls(series):
    items = [
        None,
        {"a": 1, "b": None, "c": "valid"},
        {"a": None, "b": 2, "c": None},
        {"a": 3, "b": 3, "c": "valid"},
        None,
    ]
    assert series(items).serialize("json") == [
        None,
        '{"a":1,"b":null,"c":"valid"}',
        '{"a":null,"b":2,"c":null}',
        '{"a":3,"b":3,"c":"valid"}',
        None,
    ]


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_serialize_json_with_collections(series):
    items = [
        {"arr": [1, 2, 3], "map": {"a": None, "b": 2}, "struct": {"x": 1, "y": "z"}},
        {"arr": [4, 5, 6], "map": {"a": 3, "b": None}, "struct": {"x": 2, "y": "w"}},
        {"arr": None, "map": None, "struct": None},
    ]
    assert series(items).serialize("json") == [
        '{"arr":[1,2,3],"map":{"a":null,"b":2},"struct":{"x":1,"y":"z"}}',
        '{"arr":[4,5,6],"map":{"a":3,"b":null},"struct":{"x":2,"y":"w"}}',
        '{"arr":null,"map":null,"struct":null}',
    ]


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_serialize_json_with_nested_collections(series):
    items = [
        {"arr": [{"x": 1}, {"x": 2}], "map": {"a": [1, 2], "b": {"c": 3}}},
        {"arr": [{"x": 3}, {"x": 4}], "map": {"a": [3, 4], "b": {"c": 5}}},
        {"arr": None, "map": None},
    ]
    assert series(items).serialize("json") == [
        '{"arr":[{"x":1},{"x":2}],"map":{"a":[1,2],"b":{"c":3}}}',
        '{"arr":[{"x":3},{"x":4}],"map":{"a":[3,4],"b":{"c":5}}}',
        '{"arr":null,"map":null}',
    ]
