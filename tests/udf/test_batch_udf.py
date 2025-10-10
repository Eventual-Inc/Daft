from __future__ import annotations

import pytest

import daft
from daft import DataType, Series, col
from daft.context import get_context


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


@pytest.mark.skipif(
    get_context().daft_execution_config.use_legacy_ray_runner,
    reason="batch size is not supported in legacy ray runner",
)
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
