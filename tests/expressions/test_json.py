from typing import Any

import pytest

import daft
from daft import DataFrame, col
from daft import DataType as dt


def assert_eq(df1: DataFrame, df2: DataFrame):
    assert df1.to_pydict() == df2.to_pydict(0)


def from_json(items: list[str], dtype: dt) -> list[Any]:
    c = "some_json_text"
    df = daft.from_pydict({c: items})
    df = df.select(col(c).json_loads(dtype))
    return df.to_pydict()[c]


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
def test_from_json_with_scalars(text, dtype, expected):
    df = daft.from_pydict({"col": [text]})
    df = df.select(col("col").json_loads(dtype))
    assert df.to_pydict()["col"][0] == expected


def test_from_json_with_structs():
    items = [
        '{"name": "Alice", "age": 30}',
        '{"name": "Bob", "age": 25}',
        '{"name": "Charlie", "age": 35}',
    ]
    # STRUCT<name: STRING, age: BIGINT>
    dtype = dt.struct(
        {
            "name": dt.string(),
            "age": dt.int64(),
        }
    )
    assert from_json(items, dtype) == [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]


def test_from_json_with_list():
    items = [
        "[1, 2, 3]",
        "[4, 5, 6]",
    ]
    # BIGINT ARRAY[]
    dtype = dt.list(dt.int64())
    assert from_json(items, dtype) == [
        [1, 2, 3],
        [4, 5, 6],
    ]


@pytest.mark.skip("arrow2 does not yet support json deserialize on the map type.")
def test_from_json_with_map():
    items = [
        '{"a": 1, "b": 2}',
        '{"c": 3, "d": 4}',
    ]
    # MAP<STRING, BIGINT>
    dtype = dt.map(dt.string(), dt.int64())
    assert from_json(items, dtype) == [
        {"a": 1, "b": 2},
        {"c": 3, "d": 4},
    ]
