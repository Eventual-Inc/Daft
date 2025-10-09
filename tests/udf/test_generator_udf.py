from __future__ import annotations

import collections.abc
import re
import typing

import pytest

import daft


def test_generator_udf():
    @daft.func(return_dtype=daft.DataType.string())
    def my_repeat(to_repeat: str, n: int):
        for _ in range(n):
            yield to_repeat

    df = daft.from_pydict({"id": [0, 1, 2], "value": ["pip", "install", "daft"], "occurrences": [0, 2, 4]})
    actual = df.select("id", my_repeat(df["value"], df["occurrences"])).to_pydict()

    expected = {"id": [0, 1, 1, 2, 2, 2, 2], "value": [None, "install", "install", "daft", "daft", "daft", "daft"]}

    assert actual == expected


def test_generator_udf_literal_arg():
    @daft.func(return_dtype=daft.DataType.string())
    def my_repeat(to_repeat: str, n: int):
        for _ in range(n):
            yield to_repeat

    df = daft.from_pydict({"id": [0, 1, 2], "value": ["pip", "install", "daft"]})
    actual = df.select("id", my_repeat(df["value"], 2)).to_pydict()

    expected = {"id": [0, 0, 1, 1, 2, 2], "value": ["pip", "pip", "install", "install", "daft", "daft"]}

    assert actual == expected


def test_generator_udf_literal_eval():
    @daft.func(return_dtype=daft.DataType.string())
    def my_repeat(to_repeat: str, n: int):
        for _ in range(n):
            yield to_repeat

    assert list(my_repeat("foo", 3)) == ["foo", "foo", "foo"]


def test_generator_udf_typing_iterator():
    @daft.func
    def my_gen_func(input: int) -> typing.Iterator[str]:
        yield str(input)

    df = daft.from_pydict({"input": [0]})
    df = df.select(my_gen_func(df["input"]).alias("output"))

    assert df.schema() == daft.Schema.from_pydict({"output": daft.DataType.string()})


def test_generator_udf_typing_generator():
    @daft.func
    def my_gen_func(input: int) -> typing.Generator[str, None, None]:
        yield str(input)

    df = daft.from_pydict({"input": [0]})
    df = df.select(my_gen_func(df["input"]).alias("output"))

    assert df.schema() == daft.Schema.from_pydict({"output": daft.DataType.string()})


def test_generator_udf_collections_iterator():
    @daft.func
    def my_gen_func(input: int) -> collections.abc.Iterator[str]:
        yield str(input)

    df = daft.from_pydict({"input": [0]})
    df = df.select(my_gen_func(df["input"]).alias("output"))

    assert df.schema() == daft.Schema.from_pydict({"output": daft.DataType.string()})


def test_generator_udf_collections_generator():
    @daft.func
    def my_gen_func(input: int) -> collections.abc.Generator[str, None, None]:
        yield str(input)

    df = daft.from_pydict({"input": [0]})
    df = df.select(my_gen_func(df["input"]).alias("output"))

    assert df.schema() == daft.Schema.from_pydict({"output": daft.DataType.string()})


def test_generator_udf_unnest():
    @daft.func(
        return_dtype=daft.DataType.struct({"id": daft.DataType.int64(), "value": daft.DataType.string()}), unnest=True
    )
    def create_records(count: int, base_value: str):
        for i in range(count):
            yield {"id": i, "value": f"{base_value}_{i}"}

    df = daft.from_pydict({"count": [2, 3, 1], "base": ["a", "b", "c"]})
    result = df.select(create_records(df["count"], df["base"])).to_pydict()

    expected = {"id": [0, 1, 0, 1, 2, 0], "value": ["a_0", "a_1", "b_0", "b_1", "b_2", "c_0"]}
    assert result == expected


def test_generator_udf_unnest_empty_generator():
    @daft.func(
        return_dtype=daft.DataType.struct({"x": daft.DataType.int64(), "y": daft.DataType.string()}), unnest=True
    )
    def empty_gen(n: int):
        if n > 0:
            yield {"x": n, "y": str(n)}

    df = daft.from_pydict({"n": [0, 1, 2]})
    result = df.select(empty_gen(df["n"])).to_pydict()

    expected = {"x": [None, 1, 2], "y": [None, "1", "2"]}
    assert result == expected


def test_generator_udf_unnest_error_non_struct():
    with pytest.raises(
        ValueError,
        match=re.escape("Expected Daft function `return_dtype` to be `DataType.struct(..)` when `unnest=True`"),
    ):

        @daft.func(return_dtype=daft.DataType.string(), unnest=True)
        def invalid_unnest_generator(n: int):
            for i in range(n):
                yield str(i)
