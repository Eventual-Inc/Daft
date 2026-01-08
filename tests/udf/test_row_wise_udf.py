from __future__ import annotations

import asyncio
import re
import time

import numpy as np
import pytest

import daft
from daft import DataType, col
from daft.ai.utils import RetryAfterError
from daft.recordbatch import MicroPartition, RecordBatch
from tests.conftest import get_tests_daft_runner_name


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


def test_row_wise_udf_with_ray_options():
    @daft.func(ray_options={"num_cpus": 1, "num_gpus": 0.5})
    def my_udf(x: int) -> int:
        return x

    df = daft.from_pydict({"x": [1, 2, 3]})

    # We can only verify that the options are passed correctly via explain
    import io

    f = io.StringIO()
    df.select(my_udf(col("x"))).explain(file=f, show_all=True)
    explanation = f.getvalue()

    assert "'num_cpus': 1" in explanation
    assert "'num_gpus': 0.5" in explanation

    # Also verify execution
    actual = df.select(my_udf(col("x"))).to_pydict()
    expected = {"x": [1, 2, 3]}
    assert actual == expected


def test_row_wise_udf_override_concurrency():
    @daft.func(return_dtype=DataType.int64())
    def my_udf(x):
        return x

    # Override concurrency
    func = my_udf.with_concurrency(10)

    df = daft.from_pydict({"x": [1, 2, 3]})

    import io

    f = io.StringIO()
    df.select(func(col("x"))).explain(file=f, show_all=True)
    explanation = f.getvalue()

    assert "concurrency = 10" in explanation

    actual = df.select(func(col("x"))).to_pydict()
    expected = {"x": [1, 2, 3]}
    assert actual == expected


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
    class RetryState:
        def __init__(self):
            self.first_time = True

    state = RetryState()

    @daft.func(on_error="ignore", max_retries=1)
    def raise_err_first_time_only(x) -> int:
        if state.first_time:
            state.first_time = False
            raise ValueError("This is an error")
        else:
            return x * 2

    df = daft.from_pydict({"value": [1]})

    expected = {"value": [2]}

    actual = df.select(raise_err_first_time_only(col("value"))).to_pydict()
    assert actual == expected


def test_async_rowwise_retry():
    class RetryState:
        def __init__(self):
            self.first_time = True

    state = RetryState()

    @daft.func(on_error="ignore", max_retries=1)
    async def raise_err_first_time_only(x) -> int:
        if state.first_time:
            state.first_time = False
            raise ValueError("This is an error")
        else:
            return x * 2

    df = daft.from_pydict({"value": [1]})

    expected = {"value": [2]}

    actual = df.select(raise_err_first_time_only(col("value"))).to_pydict()
    assert actual == expected


@pytest.mark.parametrize("max_retries", [1, 2, 3])
@pytest.mark.parametrize("is_async", [False, True])
def test_rowwise_retry_after_delay_respected(max_retries, is_async):
    class RetryState:
        def __init__(self):
            self.call_count = 0

    state = RetryState()
    retry_delay = 0.1

    def _retry_func_impl(x) -> int:
        state.call_count += 1
        if state.call_count <= max_retries:
            raise RetryAfterError(retry_delay)
        return x * 3

    if is_async:

        @daft.func(max_retries=max_retries)
        async def retry_func(x) -> int:
            return _retry_func_impl(x)

    else:

        @daft.func(max_retries=max_retries)
        def retry_func(x) -> int:
            return _retry_func_impl(x)

    df = daft.from_pydict({"value": [2]})

    start = time.perf_counter()
    result = df.select(retry_func(col("value"))).to_pydict()
    elapsed = time.perf_counter() - start

    assert result == {"value": [6]}
    # call_count tracking doesn't work with Ray due to process serialization
    # but retry behavior is verified through timing and result correctness
    if get_tests_daft_runner_name() != "ray":
        assert state.call_count == max_retries + 1
    # Should honor the retry-after delay (accounting for ±25% jitter, so minimum is 75% of base delay)
    assert elapsed >= retry_delay * 0.7 * max_retries


@pytest.mark.parametrize("is_async", [False, True])
def test_rowwise_retry_after_max_retries_exceeded(is_async):
    """Test that when max retries is exceeded, the original exception from RetryAfterError is raised."""

    class RetryState:
        def __init__(self):
            self.call_count = 0

    state = RetryState()
    retry_delay = 0.1
    original_error_message = "Rate limit exceeded"

    def _always_retry_impl(x) -> int:
        state.call_count += 1
        # Always raise RetryAfterError with an original exception until max retries is exceeded
        original_exc = ValueError(original_error_message)
        raise RetryAfterError(retry_delay, original=original_exc)

    if is_async:

        @daft.func(max_retries=1)
        async def always_retry(x) -> int:
            return _always_retry_impl(x)

    else:

        @daft.func(max_retries=1)
        def always_retry(x) -> int:
            return _always_retry_impl(x)

    df = daft.from_pydict({"value": [2]})

    start = time.perf_counter()
    with pytest.raises(ValueError, match=original_error_message) as exc_info:
        df.select(always_retry(col("value"))).to_pydict()
    elapsed = time.perf_counter() - start

    assert original_error_message in str(exc_info.value)
    # call_count tracking doesn't work with Ray due to process serialization
    # but retry behavior is verified through exception and timing
    if get_tests_daft_runner_name() != "ray":
        # Should have attempted initial call + max_retries retries
        assert state.call_count == 2
    # Should have respected at least one retry delay (accounting for ±25% jitter, so minimum is 75% of base delay)
    assert elapsed >= retry_delay * 0.7


def test_rowwise_retry_expected_to_fail():
    class RetryState:
        def __init__(self):
            self.first_time = True

    state = RetryState()

    @daft.func(on_error="ignore", max_retries=0)
    def raise_err_first_time_only(x) -> int:
        if state.first_time:
            state.first_time = False
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


def test_dynamic_batching_same_result():
    def create_df_from_batches(batches_data):
        micropartitions = []
        for mp_data in batches_data:
            record_batches = [RecordBatch.from_pydict(rb_data) for rb_data in mp_data]
            mp = MicroPartition._from_record_batches(record_batches)
            micropartitions.append(mp)
        return daft.DataFrame._from_micropartitions(*micropartitions)

    df = (
        create_df_from_batches(
            [
                [
                    {"x": np.arange(1001).tolist(), "y": np.arange(1001) * 2},
                    {"x": np.arange(523).tolist(), "y": np.arange(523) * 2},
                    {"x": np.arange(15).tolist(), "y": np.arange(15) * 2},
                ],
                [
                    {"x": np.arange(1).tolist(), "y": np.arange(1) * 2},
                    {"x": np.arange(2).tolist(), "y": np.arange(2) * 2},
                ],
                [
                    {"x": np.arange(9).tolist(), "y": np.arange(9) * 2},
                ],
                [
                    {"x": np.arange(111).tolist(), "y": np.arange(111) * 2},
                    {"x": np.arange(1).tolist(), "y": np.arange(1) * 2},
                    {"x": np.arange(7597).tolist(), "y": np.arange(7597) * 2},
                    {"x": np.arange(253).tolist(), "y": np.arange(253) * 2},
                ],
            ]
        )
        ._add_monotonically_increasing_id()
        .collect()
    )

    @daft.func
    def stringify_and_sum(a: int, b: int) -> str:
        return f"{a + b}"

    non_dynamic_batching_df = df.select("*", stringify_and_sum(col("x"), col("y")).alias("sum")).collect()

    # dynamic batching is feature flagged
    with daft.execution_config_ctx(maintain_order=False, enable_dynamic_batching=True):
        dynamic_batching_df = df.select("*", stringify_and_sum(col("x"), col("y")).alias("sum"))
        dynamic_batching_df = dynamic_batching_df.collect().sort("id")
        assert non_dynamic_batching_df.to_pydict() == dynamic_batching_df.to_pydict()
