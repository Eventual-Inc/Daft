from __future__ import annotations

import asyncio
import re

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
    with pytest.raises(
        ValueError,
        match="Daft functions require either a return type hint or the `return_dtype` argument to be specified.",
    ):

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

    assert my_stringify_and_sum(1, 2) == "3"


def test_row_wise_udf_kwargs():
    @daft.func
    def my_stringify_and_sum_repeat(a: int, b: int, repeat: int = 1) -> str:
        return f"{a + b}" * repeat

    assert my_stringify_and_sum_repeat(1, 2) == "3"
    assert my_stringify_and_sum_repeat(1, 2, 3) == "333"

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    default_df = df.select(my_stringify_and_sum_repeat(col("x"), col("y")))
    assert default_df.to_pydict() == {"x": ["5", "7", "9"]}

    constant_repeat_df = df.select(my_stringify_and_sum_repeat(col("x"), col("y"), repeat=2))
    assert constant_repeat_df.to_pydict() == {"x": ["55", "77", "99"]}

    dynamic_repeat_df = df.select(my_stringify_and_sum_repeat(col("x"), col("y"), repeat=col("x")))
    assert dynamic_repeat_df.to_pydict() == {"x": ["5", "77", "999"]}


def test_row_wise_async_udf():
    @daft.func
    async def my_async_stringify_and_sum(a: int, b: int) -> str:
        await asyncio.sleep(0.1)
        return f"{a + b}"

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    async_df = df.select(my_async_stringify_and_sum(col("x"), col("y")))
    assert sorted(async_df.to_pydict()["x"]) == ["5", "7", "9"]


def test_row_wise_udf_unnest():
    @daft.func(
        return_dtype=daft.DataType.struct(
            {
                "id": daft.DataType.int64(),
                "name": daft.DataType.string(),
                "score": daft.DataType.float64(),
            }
        ),
        unnest=True,
    )
    def create_record(value: int):
        return {"id": value, "name": f"item_{value}", "score": value * 1.5}

    df = daft.from_pydict({"value": [1, 2, 3]})
    result = df.select(create_record(col("value"))).to_pydict()

    expected = {
        "id": [1, 2, 3],
        "name": ["item_1", "item_2", "item_3"],
        "score": [1.5, 3.0, 4.5],
    }
    assert result == expected


def test_row_wise_udf_unnest_error_non_struct():
    with pytest.raises(
        ValueError,
        match=re.escape("Expected Daft function `return_dtype` to be `DataType.struct(..)` when `unnest=True`"),
    ):

        @daft.func(return_dtype=daft.DataType.int64(), unnest=True)
        def invalid_unnest(a: int):
            return a


def test_rowwise_on_err_ignore():
    @daft.func(on_error="ignore")
    def raise_err(x) -> int:
        if x % 2:
            raise ValueError("This is an error")
        else:
            return x

    df = daft.from_pydict({"value": [1, 2, 3]})

    expected = {"value": [None, 2, None]}

    actual = df.select(raise_err(col("value"))).to_pydict()
    assert actual == expected


def test_async_rowwise_on_err_ignore():
    @daft.func(on_error="ignore")
    async def raise_err(x) -> int:
        raise ValueError("This is an error")

    df = daft.from_pydict({"value": [1]})

    expected = {"value": [None]}

    actual = df.select(raise_err(col("value"))).to_pydict()
    assert actual == expected


def test_rowwise_retry():
    first_time = True

    @daft.func(on_error="ignore", max_retries=1)
    def raise_err_first_time_only(x) -> int:
        nonlocal first_time
        if first_time:
            first_time = False
            raise ValueError("This is an error")
        else:
            return x * 2

    df = daft.from_pydict({"value": [1]})

    expected = {"value": [2]}

    actual = df.select(raise_err_first_time_only(col("value"))).to_pydict()
    assert actual == expected


def test_async_rowwise_retry():
    first_time = True

    @daft.func(on_error="ignore", max_retries=1)
    async def raise_err_first_time_only(x) -> int:
        nonlocal first_time
        if first_time:
            first_time = False
            raise ValueError("This is an error")
        else:
            return x * 2

    df = daft.from_pydict({"value": [1]})

    expected = {"value": [2]}

    actual = df.select(raise_err_first_time_only(col("value"))).to_pydict()
    assert actual == expected


def test_rowwise_retry_expected_to_fail():
    first_time = True

    @daft.func(on_error="ignore", max_retries=0)
    def raise_err_first_time_only(x) -> int:
        nonlocal first_time
        if first_time:
            first_time = False
            raise ValueError("This is an error")
        else:
            return x * 2

    df = daft.from_pydict({"value": [1]})

    expected = {"value": [None]}

    actual = df.select(raise_err_first_time_only(col("value"))).to_pydict()
    assert actual == expected


def test_rowwise_retry_expected_to_fail_with_raise():
    @daft.func(on_error="raise", max_retries=0)
    def raise_err(x) -> int:
        raise ValueError("This is an error")

    df = daft.from_pydict({"value": [1]})

    try:
        df.select(raise_err(col("value"))).to_pydict()
        pytest.fail("Expected ValueError")
    except ValueError:
        pass


def test_async_rowwise_retry_expected_to_fail_with_raise():
    @daft.func(on_error="raise", max_retries=0)
    async def raise_err(x) -> int:
        raise ValueError("This is an error")

    df = daft.from_pydict({"value": [1]})

    try:
        df.select(raise_err(col("value"))).to_pydict()
        pytest.fail("Expected ValueError")
    except ValueError:
        pass


def test_rowwise_retry_defaults_to_raise_and_zero_retries():
    @daft.func
    def raise_err(x) -> int:
        raise ValueError("This is an error")

    df = daft.from_pydict({"value": [1]})

    try:
        df.select(raise_err(col("value"))).to_pydict()
        pytest.fail("Expected ValueError")
    except ValueError:
        pass


def test_async_rowwise_retry_defaults_to_raise_and_zero_retries():
    @daft.func
    async def raise_err(x) -> int:
        raise ValueError("This is an error")

    df = daft.from_pydict({"value": [1]})

    try:
        df.select(raise_err(col("value"))).to_pydict()
        pytest.fail("Expected ValueError")
    except ValueError:
        pass


def test_row_wise_async_udf_use_process():
    @daft.func(use_process=True)
    async def my_async_stringify_and_sum(a: int, b: int) -> str:
        await asyncio.sleep(0.01)
        return f"{a + b}"

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    async_df = df.select(my_async_stringify_and_sum(col("x"), col("y")))
    assert sorted(async_df.to_pydict()["x"]) == ["5", "7", "9"]
