from __future__ import annotations

import asyncio
import time

import pytest

import daft
from daft import DataType, Series, col
from daft.ai.utils import RetryAfterError
from tests.conftest import get_tests_daft_runner_name


def test_batch_udf():
    @daft.func.batch(return_dtype=DataType.int64())
    def my_sum(a: Series, b: Series) -> Series:
        import pyarrow.compute as pc

        a_arrow = a.to_arrow()
        b_arrow = b.to_arrow()
        result = pc.add(a_arrow, b_arrow)
        return Series.from_arrow(result)

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    actual = df.select(my_sum(col("x"), col("y"))).to_pydict()

    expected = {"x": [5, 7, 9]}

    assert actual == expected


def test_batch_udf_with_literal():
    @daft.func.batch(return_dtype=DataType.int64())
    def my_sum_with_scalar(a: Series, b: int) -> Series:
        import pyarrow.compute as pc

        a_arrow = a.to_arrow()
        result = pc.add(a_arrow, b)
        return Series.from_arrow(result)

    df = daft.from_pydict({"x": [1, 2, 3]})
    actual = df.select(my_sum_with_scalar(col("x"), 10)).to_pydict()

    expected = {"x": [11, 12, 13]}

    assert actual == expected


def test_batch_udf_literal_eval():
    @daft.func.batch(return_dtype=DataType.int64())
    def my_sum_with_scalar(a: Series, b: int) -> Series:
        import pyarrow.compute as pc

        a_arrow = a.to_arrow()
        result = pc.add(a_arrow, b)
        return Series.from_arrow(result)

    a = Series.from_pylist([1, 2, 3])
    result = my_sum_with_scalar(a, 3)
    assert result.to_pylist() == [4, 5, 6]


def test_batch_udf_unnest():
    @daft.func.batch(
        return_dtype=DataType.struct(
            {"doubled": DataType.int64(), "tripled": DataType.int64(), "name": DataType.string()}
        ),
        unnest=True,
    )
    def create_records(a: Series) -> Series:
        doubled = [x * 2 for x in a.to_pylist()]
        tripled = [x * 3 for x in a.to_pylist()]
        names = [f"val_{x}" for x in a.to_pylist()]
        data = [{"doubled": d, "tripled": t, "name": n} for d, t, n in zip(doubled, tripled, names)]
        return Series.from_pylist(data)

    df = daft.from_pydict({"value": [1, 2, 3]})
    result = df.select(create_records(col("value"))).to_pydict()

    expected = {"doubled": [2, 4, 6], "tripled": [3, 6, 9], "name": ["val_1", "val_2", "val_3"]}
    assert result == expected


def test_batch_udf_with_batch_size():
    # Test that batch_size parameter is accepted
    @daft.func.batch(return_dtype=DataType.int64(), batch_size=3)
    def my_sum(a: Series, b: Series) -> Series:
        import pyarrow.compute as pc

        assert len(a) <= 3

        a_arrow = a.to_arrow()
        b_arrow = b.to_arrow()
        result = pc.add(a_arrow, b_arrow)
        return Series.from_arrow(result)

    df = daft.from_pydict({"x": [1, 2, 3, 4], "y": [5, 6, 7, 8]})
    actual = df.select(my_sum(col("x"), col("y"))).to_pydict()

    expected = {"x": [6, 8, 10, 12]}

    assert actual == expected


def test_batch_udf_returns_list():
    @daft.func.batch(return_dtype=DataType.int64())
    def multiply_and_return_list(a: Series, b: Series) -> list:
        a_list = a.to_pylist()
        b_list = b.to_pylist()
        # Return a list instead of a Series
        return [x * y for x, y in zip(a_list, b_list)]

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    actual = df.select(multiply_and_return_list(col("x"), col("y"))).to_pydict()

    expected = {"x": [4, 10, 18]}

    assert actual == expected


def test_batch_udf_returns_numpy_array():
    @daft.func.batch(return_dtype=DataType.float64())
    def numpy_sqrt(a: Series):
        import numpy as np

        a_array = a.to_pylist()
        # Return a numpy array instead of a Series
        return np.sqrt(np.array(a_array, dtype=np.float64))

    df = daft.from_pydict({"x": [1.0, 4.0, 9.0, 16.0]})
    actual = df.select(numpy_sqrt(col("x"))).to_pydict()

    expected = {"x": [1.0, 2.0, 3.0, 4.0]}

    assert actual == expected


def test_batch_udf_returns_pyarrow_array():
    @daft.func.batch(return_dtype=DataType.int64())
    def pyarrow_add(a: Series, b: Series):
        import pyarrow.compute as pc

        a_arrow = a.to_arrow()
        b_arrow = b.to_arrow()
        # Return a pyarrow array instead of a Series
        result = pc.add(a_arrow, b_arrow)
        return result

    df = daft.from_pydict({"x": [10, 20, 30], "y": [1, 2, 3]})
    actual = df.select(pyarrow_add(col("x"), col("y"))).to_pydict()

    expected = {"x": [11, 22, 33]}

    assert actual == expected


def test_batch_udf_returns_pyarrow_chunked_array():
    @daft.func.batch(return_dtype=DataType.int64())
    def double_values(a: Series):
        import pyarrow as pa
        import pyarrow.compute as pc

        a_arrow = a.to_arrow()
        # Double the values
        result = pc.multiply(a_arrow, 2)
        # Wrap in a ChunkedArray to test that case
        chunked = pa.chunked_array([result])
        return chunked

    df = daft.from_pydict({"x": [10, 20, 30]})
    actual = df.select(double_values(col("x"))).to_pydict()

    expected = {"x": [20, 40, 60]}

    assert actual == expected


def test_batch_udf_literal_eval_returns_list():
    # Note: When calling batch UDFs directly (literal evaluation),
    # the return value is not automatically converted through call_batch,
    # so we get the raw return type
    @daft.func.batch(return_dtype=DataType.int64())
    def add_scalar_return_list(a: Series, b: int) -> list:
        a_list = a.to_pylist()
        return [x + b for x in a_list]

    a = Series.from_pylist([1, 2, 3])
    result = add_scalar_return_list(a, 10)
    # Result is a list, not a Series, when called directly
    assert result == [11, 12, 13]


def test_batch_udf_literal_eval_returns_numpy():
    # Note: When calling batch UDFs directly (literal evaluation),
    # the return value is not automatically converted through call_batch,
    # so we get the raw return type
    @daft.func.batch(return_dtype=DataType.int64())
    def multiply_scalar_return_numpy(a: Series, b: int):
        import numpy as np

        a_array = np.array(a.to_pylist())
        return a_array * b

    a = Series.from_pylist([1, 2, 3])
    result = multiply_scalar_return_numpy(a, 5)
    # Result is a numpy array, not a Series, when called directly
    import numpy as np

    assert np.array_equal(result, np.array([5, 10, 15]))


def test_batch_on_error_ignore():
    @daft.func.batch(on_error="ignore", return_dtype=int)
    def raise_err(x):
        raise ValueError("batch failed")

    df = daft.from_pydict({"value": [1, 2, 3]})

    expected = {"value": [None, None, None]}

    actual = df.select(raise_err(col("value"))).to_pydict()
    assert actual == expected


def test_batch_retry_defaults_to_raise_and_zero_retries():
    @daft.func.batch(return_dtype=int)
    def raise_err(x):
        raise ValueError("This is an error")

    df = daft.from_pydict({"value": [1]})

    try:
        df.select(raise_err(col("value"))).to_pydict()
        pytest.fail("Expected ValueError")
    except ValueError:
        pass


@pytest.mark.parametrize("max_retries", [1, 2, 3])
@pytest.mark.parametrize("is_async", [False, True])
def test_batch_retry_after_delay_respected(max_retries, is_async):
    class RetryState:
        def __init__(self):
            self.call_count = 0

    state = RetryState()
    retry_delay = 0.1

    def _retry_func_impl(x: Series) -> list:
        state.call_count += 1
        if state.call_count <= max_retries:
            raise RetryAfterError(retry_delay)
        return [val * 2 for val in x]

    if is_async:

        @daft.func.batch(return_dtype=DataType.int64(), max_retries=max_retries)
        async def retry_func(x: Series) -> Series:
            await asyncio.sleep(0.01)  # Small delay to simulate async work
            result = _retry_func_impl(x)
            return result

    else:

        @daft.func.batch(return_dtype=DataType.int64(), max_retries=max_retries)
        def retry_func(x: Series) -> Series:
            result = _retry_func_impl(x)
            return result

    df = daft.from_pydict({"value": [1, 2, 3]})

    start = time.perf_counter()
    result = df.select(retry_func(col("value"))).to_pydict()
    elapsed = time.perf_counter() - start

    assert result == {"value": [2, 4, 6]}
    # call_count tracking doesn't work with Ray due to process serialization
    # but retry behavior is verified through timing and result correctness
    if get_tests_daft_runner_name() != "ray":
        assert state.call_count == max_retries + 1
    # Should honor the retry-after delay (accounting for ±25% jitter, so minimum is 75% of base delay)
    assert elapsed >= retry_delay * 0.7 * max_retries


@pytest.mark.parametrize("is_async", [False, True])
def test_batch_retry_after_max_retries_exceeded(is_async):
    """Test that when max retries is exceeded, the original exception from RetryAfterError is raised."""

    class RetryState:
        def __init__(self):
            self.call_count = 0

    state = RetryState()
    retry_delay = 0.1
    original_error_message = "Rate limit exceeded"

    def _always_retry_impl(x: Series) -> list:
        state.call_count += 1
        # Always raise RetryAfterError with an original exception until max retries is exceeded
        original_exc = ValueError(original_error_message)
        raise RetryAfterError(retry_delay, original=original_exc)

    if is_async:

        @daft.func.batch(return_dtype=DataType.int64(), max_retries=1)
        async def always_retry(x: Series) -> Series:
            await asyncio.sleep(0.01)  # Small delay to simulate async work
            return _always_retry_impl(x)

    else:

        @daft.func.batch(return_dtype=DataType.int64(), max_retries=1)
        def always_retry(x: Series) -> Series:
            return _always_retry_impl(x)

    df = daft.from_pydict({"value": [1, 2, 3]})

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


def test_batch_max_retries():
    class RetryState:
        def __init__(self):
            self.first_time = True

    state = RetryState()

    @daft.func.batch(return_dtype=int, max_retries=1)
    def raise_err_first_time_only(x: Series):
        if state.first_time:
            state.first_time = False
            raise ValueError("This is an error")

        return [val * 2 for val in x]

    df = daft.from_pydict({"value": [1, 2, 3]})
    actual = df.select(raise_err_first_time_only(col("value"))).to_pydict()
    expected = {"value": [2, 4, 6]}
    assert actual == expected


def test_async_batch_udf():
    import asyncio

    @daft.func.batch(return_dtype=DataType.int64())
    async def async_batch_func(a: Series) -> Series:
        await asyncio.sleep(0.1)
        return a

    df = daft.from_pydict({"x": [1, 2, 3]})
    actual = df.select(async_batch_func(col("x"))).to_pydict()

    expected = {"x": [1, 2, 3]}

    assert actual == expected


def test_async_batch_on_error_ignore():
    @daft.func.batch(on_error="ignore", return_dtype=int)
    async def raise_err(x):
        raise ValueError("batch failed")

    df = daft.from_pydict({"value": [1, 2, 3]})

    expected = {"value": [None, None, None]}

    actual = df.select(raise_err(col("value"))).to_pydict()
    assert actual == expected


def test_async_batch_retry():
    class RetryState:
        def __init__(self):
            self.first_time = True

    state = RetryState()

    @daft.func.batch(on_error="ignore", max_retries=1, return_dtype=int)
    async def raise_err_first_time_only(x: Series) -> list:
        if state.first_time:
            state.first_time = False
            raise ValueError("This is an error")
        else:
            return [val * 2 for val in x]

    df = daft.from_pydict({"value": [1, 2, 3]})

    expected = {"value": [2, 4, 6]}

    actual = df.select(raise_err_first_time_only(col("value"))).to_pydict()
    assert actual == expected


def test_async_batch_retry_expected_to_fail_with_raise():
    @daft.func.batch(on_error="raise", max_retries=0, return_dtype=int)
    async def raise_err(x: Series):
        raise ValueError("This is an error")

    df = daft.from_pydict({"value": [1]})

    try:
        df.select(raise_err(col("value"))).to_pydict()
        pytest.fail("Expected ValueError")
    except ValueError:
        pass


def test_async_batch_retry_defaults_to_raise_and_zero_retries():
    @daft.func.batch(return_dtype=int)
    async def raise_err(x):
        raise ValueError("This is an error")

    df = daft.from_pydict({"value": [1]})

    try:
        df.select(raise_err(col("value"))).to_pydict()
        pytest.fail("Expected ValueError")
    except ValueError:
        pass
