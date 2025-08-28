from __future__ import annotations

import collections.abc
import typing

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

    assert list(my_repeat.eval("foo", 3)) == ["foo", "foo", "foo"]


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
