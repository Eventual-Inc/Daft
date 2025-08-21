from __future__ import annotations

import pytest

import daft
from daft import DataType, col


def test_row_wise_udf():
    @daft.func()
    def my_stringify_and_sum(a: int, b: int) -> str:
        return f"{a + b}"

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    actual = df.select(my_stringify_and_sum(col("x"), col("y"))).to_pydict()

    expected = {"x": ["5", "7", "9"]}

    assert actual == expected


def test_row_wise_udf_alternative_signature():
    @daft.func
    def my_stringify_and_sum(a: int, b: int) -> str:
        return f"{a + b}"

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    actual = df.select(my_stringify_and_sum(col("x"), col("y"))).to_pydict()

    expected = {"x": ["5", "7", "9"]}

    assert actual == expected


def test_row_wise_udf_with_literal():
    @daft.func()
    def my_stringify_and_sum(a: int, b: int) -> str:
        return f"{a + b}"

    df = daft.from_pydict({"x": [1, 2, 3]})
    actual = df.select(my_stringify_and_sum(col("x"), 4)).to_pydict()

    expected = {"x": ["5", "6", "7"]}

    assert actual == expected


def test_row_wise_udf_should_infer_dtype_from_function():
    @daft.func()
    def list_string_return_type(a: int, b: int) -> list[str]:
        return [f"{a + b}"]

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    df = df.select(list_string_return_type(col("x"), col("y")))

    schema = df.schema()
    expected_schema = daft.Schema.from_pydict({"x": daft.DataType.list(daft.DataType.string())})

    assert schema == expected_schema


def test_func_requires_return_dtype_when_no_annotation():
    with pytest.raises(ValueError, match="return_dtype is required when function has no return annotation"):

        @daft.func()
        def my_func(a: int, b: int):
            return f"{a + b}"


def test_row_wise_udf_override_return_dtype():
    @daft.func(return_dtype=DataType.int16())
    def my_sum(a: int, b: int) -> int:
        return a + b

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    actual = df.select(my_sum(col("x"), col("y")))

    assert actual.schema()["x"].dtype == DataType.int16()

    expected = {"x": [5, 7, 9]}

    assert actual.to_pydict() == expected


def test_row_wise_udf_literal_eval():
    @daft.func
    def my_stringify_and_sum(a: int, b: int) -> str:
        return f"{a + b}"

    assert my_stringify_and_sum.eval(1, 2) == "3"


def test_row_wise_udf_kwargs():
    @daft.func
    def my_stringify_and_sum_repeat(a: int, b: int, repeat: int = 1) -> str:
        return f"{a + b}" * repeat

    assert my_stringify_and_sum_repeat.eval(1, 2) == "3"
    assert my_stringify_and_sum_repeat.eval(1, 2, 3) == "333"

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    default_df = df.select(my_stringify_and_sum_repeat(col("x"), col("y")))
    assert default_df.to_pydict() == {"x": ["5", "7", "9"]}

    constant_repeat_df = df.select(my_stringify_and_sum_repeat(col("x"), col("y"), repeat=2))
    assert constant_repeat_df.to_pydict() == {"x": ["55", "77", "99"]}

    dynamic_repeat_df = df.select(my_stringify_and_sum_repeat(col("x"), col("y"), repeat=col("x")))
    assert dynamic_repeat_df.to_pydict() == {"x": ["5", "77", "999"]}


def test_row_wise_async_udf():
    import asyncio

    @daft.func
    async def my_async_stringify_and_sum(a: int, b: int) -> str:
        await asyncio.sleep(0.1)
        return f"{a + b}"

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    async_df = df.select(my_async_stringify_and_sum(col("x"), col("y")))
    assert async_df.to_pydict() == {"x": ["5", "7", "9"]}
