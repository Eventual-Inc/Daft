from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft import col
from daft.context import get_context
from daft.datatype import DataType
from daft.expressions import Expression
from daft.expressions.testing import expr_structurally_equal
from daft.recordbatch import MicroPartition
from daft.series import Series
from daft.udf import udf
from tests.conftest import get_tests_daft_runner_name


def test_udf():
    table = MicroPartition.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string())
    def repeat_n(data, n):
        return Series.from_pylist([d.as_py() * n for d in data.to_arrow()])

    expr = repeat_n(col("a"), 2)
    field = expr._to_field(table.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()

    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
@pytest.mark.parametrize("use_actor_pool", [False, True])
def test_class_udf(batch_size, use_actor_pool):
    df = daft.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string(), batch_size=batch_size)
    class RepeatN:
        def __init__(self):
            self.n = 2

        def __call__(self, data):
            return Series.from_pylist([d.as_py() * self.n for d in data.to_arrow()])

    if use_actor_pool:
        RepeatN = RepeatN.with_concurrency(2)

    expr = RepeatN(col("a"))
    field = expr._to_field(df.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()

    result = df.select(expr)
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
@pytest.mark.parametrize("use_actor_pool", [False, True])
def test_class_udf_init_args(batch_size, use_actor_pool):
    df = daft.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string(), batch_size=batch_size)
    class RepeatN:
        def __init__(self, initial_n: int = 2):
            self.n = initial_n

        def __call__(self, data):
            return Series.from_pylist([d.as_py() * self.n for d in data.to_arrow()])

    if use_actor_pool:
        RepeatN = RepeatN.with_concurrency(2)

    expr = RepeatN(col("a"))
    field = expr._to_field(df.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()
    result = df.select(expr)
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}

    expr = RepeatN.with_init_args(initial_n=3)(col("a"))
    field = expr._to_field(df.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()
    result = df.select(expr)
    assert result.to_pydict() == {"a": ["foofoofoo", "barbarbar", "bazbazbaz"]}


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
@pytest.mark.parametrize("use_actor_pool", [False, True])
def test_class_udf_init_args_no_default(batch_size, use_actor_pool):
    df = daft.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string(), batch_size=batch_size)
    class RepeatN:
        def __init__(self, initial_n):
            self.n = initial_n

        def __call__(self, data):
            return Series.from_pylist([d.as_py() * self.n for d in data.to_arrow()])

    if use_actor_pool:
        RepeatN = RepeatN.with_concurrency(2)

    with pytest.raises(ValueError, match="Cannot call class UDF without initialization arguments."):
        RepeatN(col("a"))

    expr = RepeatN.with_init_args(initial_n=2)(col("a"))
    field = expr._to_field(df.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()
    result = df.select(expr)
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}


@pytest.mark.parametrize("use_actor_pool", [False, True])
def test_class_udf_init_args_bad_args(use_actor_pool):
    @udf(return_dtype=DataType.string())
    class RepeatN:
        def __init__(self, initial_n):
            self.n = initial_n

        def __call__(self, data):
            return Series.from_pylist([d.as_py() * self.n for d in data.to_arrow()])

    if use_actor_pool:
        RepeatN = RepeatN.with_concurrency(2)

    with pytest.raises(TypeError, match="missing a required argument: 'initial_n'"):
        RepeatN.with_init_args(wrong=5)


@pytest.mark.parametrize("concurrency", [1, 2, 4])
def test_actor_pool_udf_concurrency(concurrency):
    df = daft.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string(), batch_size=1)
    class RepeatN:
        def __init__(self):
            self.n = 2

        def __call__(self, data):
            return Series.from_pylist([d.as_py() * self.n for d in data.to_arrow()])

    RepeatN = RepeatN.with_concurrency(concurrency)

    expr = RepeatN(col("a"))
    field = expr._to_field(df.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()

    result = df.select(expr)
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}


def test_udf_kwargs():
    table = MicroPartition.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string())
    def repeat_n(*, data=None, n=2):
        return Series.from_pylist([d.as_py() * n for d in data.to_arrow()])

    expr = repeat_n(data=col("a"))
    field = expr._to_field(table.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()

    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
def test_udf_tuples(batch_size):
    table = MicroPartition.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string(), batch_size=batch_size)
    def repeat_n(data, tuple_data):
        n = tuple_data[0]
        return Series.from_pylist([d.as_py() * n for d in data.to_arrow()])

    expr = repeat_n(col("a"), (2,))
    field = expr._to_field(table.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()

    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}


@pytest.mark.parametrize("container", [Series, list, np.ndarray])
@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
def test_udf_return_containers(container, batch_size):
    table = MicroPartition.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string(), batch_size=batch_size)
    def identity(data):
        if container is Series:
            return data
        elif container is list:
            return data.to_pylist()
        elif container is np.ndarray:
            return np.array(data.to_arrow())
        else:
            raise NotImplementedError(f"Test not implemented for container type: {container}")

    result = table.eval_expression_list([identity(col("a"))])
    assert result.to_pydict() == {"a": ["foo", "bar", "baz"]}


def test_udf_error():
    table = MicroPartition.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string())
    def throw_value_err(x):
        raise ValueError("AN ERROR OCCURRED!")

    expr = throw_value_err(col("a"))

    with pytest.raises(ValueError) as exc_info:
        table.eval_expression_list([expr])

    assert str(exc_info.value).startswith("AN ERROR OCCURRED!\nUser-defined function ")
    assert str(exc_info.value).endswith("failed when executing on inputs:\n  - a (Utf8, length=3)")


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
@pytest.mark.parametrize("use_actor_pool", [False, True])
def test_no_args_udf_call(batch_size, use_actor_pool):
    @udf(return_dtype=DataType.int64(), batch_size=batch_size)
    def udf_no_args():
        pass

    if use_actor_pool:
        udf_no_args = udf_no_args.with_concurrency(2)

    assert isinstance(udf_no_args(), Expression)

    with pytest.raises(TypeError):
        udf_no_args("invalid")

    with pytest.raises(TypeError):
        udf_no_args(invalid="invalid")


@pytest.mark.parametrize("use_actor_pool", [False, True])
def test_full_udf_call(use_actor_pool):
    @udf(return_dtype=DataType.int64())
    def full_udf(e_arg, val, kwarg_val=None, kwarg_ex=None):
        pass

    if use_actor_pool:
        full_udf = full_udf.with_concurrency(2)

    assert isinstance(full_udf(col("x"), 1, kwarg_val=0, kwarg_ex=col("y")), Expression)

    with pytest.raises(TypeError):
        full_udf()


@pytest.mark.parametrize("use_actor_pool", [False, True])
def test_class_udf_initialization_error(use_actor_pool):
    df = daft.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string())
    class IdentityWithInitError:
        def __init__(self):
            raise RuntimeError("UDF INIT ERROR")

        def __call__(self, data):
            return data

    if use_actor_pool:
        IdentityWithInitError = IdentityWithInitError.with_concurrency(1)

    expr = IdentityWithInitError(col("a"))
    if use_actor_pool:
        with pytest.raises(Exception):
            df.select(expr).collect()
    else:
        with pytest.raises(RuntimeError, match="UDF INIT ERROR"):
            df.select(expr).collect()


@pytest.mark.parametrize("use_actor_pool", [False, True])
def test_udf_equality(use_actor_pool):
    @udf(return_dtype=DataType.int64())
    def udf1(x):
        pass

    if use_actor_pool:
        udf1 = udf1.with_concurrency(2)

    assert expr_structurally_equal(udf1("x"), udf1("x"))
    assert not expr_structurally_equal(udf1("x"), udf1("y"))


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
def test_udf_return_tensor(batch_size):
    @udf(return_dtype=DataType.tensor(DataType.float64()), batch_size=batch_size)
    def np_udf(x):
        return [np.ones((3, 3)) * i for i in x.to_pylist()]

    expr = np_udf(col("x"))
    table = MicroPartition.from_pydict({"x": [0, 1, 2]})
    result = table.eval_expression_list([expr])
    assert len(result.to_pydict()["x"]) == 3
    for i in range(3):
        np.testing.assert_array_equal(result.to_pydict()["x"][i], np.ones((3, 3)) * i)


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
@pytest.mark.parametrize("use_actor_pool", [False, True])
def test_udf_return_embedding(batch_size, use_actor_pool):
    # Create test data
    table = MicroPartition.from_pydict({"x": [0, 1, 2]})

    # Define UDF that returns an embedding
    @udf(return_dtype=DataType.embedding(DataType.float32(), 2), batch_size=batch_size)
    def embedding_udf(x):
        # Create a 2D embedding for each input value
        return [np.array([i, i + 1], dtype=np.float32) for i in x.to_pylist()]

    # Apply the UDF
    if use_actor_pool:
        embedding_udf = embedding_udf.with_concurrency(2)

    expr = embedding_udf(col("x"))
    result = table.eval_expression_list([expr])

    # Verify results
    embeddings = result.to_pydict()["x"]
    assert len(embeddings) == 3

    # Check each embedding vector
    np.testing.assert_array_equal(embeddings[0], np.array([0, 1], dtype=np.float32))
    np.testing.assert_array_equal(embeddings[1], np.array([1, 2], dtype=np.float32))
    np.testing.assert_array_equal(embeddings[2], np.array([2, 3], dtype=np.float32))


@pytest.mark.skip(
    reason="[RUST-INT][UDF] repr is very naive at the moment py_udf(...exprs), we should fix to show all parameters and use the function name"
)
def test_udf_repr():
    @udf(return_dtype=DataType.int64(), expr_inputs=["x"])
    def single_arg_udf(x, y, z=10):
        pass

    assert single_arg_udf(col("x"), "y", z=20).__repr__() == "@udf[single_arg_udf](col('x'), 'y', z=20)"


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
def test_udf_arbitrary_number_of_args(batch_size):
    table = MicroPartition.from_pydict({"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]})

    @udf(return_dtype=DataType.int64(), batch_size=batch_size)
    def add_cols_elementwise(*args):
        return Series.from_pylist([sum(x) for x in zip(*[arg.to_pylist() for arg in args])])

    expr = add_cols_elementwise(col("a"), col("b"), col("c"))
    field = expr._to_field(table.schema())
    assert field.name == "a"
    assert field.dtype == DataType.int64()

    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"a": [3, 6, 9]}


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
def test_udf_arbitrary_number_of_kwargs(batch_size):
    table = MicroPartition.from_pydict({"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]})

    @udf(return_dtype=DataType.string(), batch_size=batch_size)
    def repeat_kwargs(**kwargs):
        data = {k: v.to_pylist() for k, v in kwargs.items()}
        length = len(data[next(iter(data.keys()))])
        return Series.from_pylist(["".join([key * data[key][i] for key in data]) for i in range(length)])

    expr = repeat_kwargs(a=col("a"), b=col("b"), c=col("c"))
    field = expr._to_field(table.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()

    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"a": ["abc", "aabbcc", "aaabbbccc"]}


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
def test_udf_arbitrary_number_of_args_with_kwargs(batch_size):
    table = MicroPartition.from_pydict({"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]})

    @udf(return_dtype=DataType.int64(), batch_size=batch_size)
    def add_cols_elementwise(*args, multiplier: float):
        return Series.from_pylist([multiplier * sum(x) for x in zip(*[arg.to_pylist() for arg in args])])

    expr = add_cols_elementwise(col("a"), col("b"), col("c"), multiplier=2)
    field = expr._to_field(table.schema())
    assert field.name == "a"
    assert field.dtype == DataType.int64()

    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"a": [6, 12, 18]}


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
def test_udf_return_pyarrow(batch_size):
    table = MicroPartition.from_pydict({"a": [1, 2, 3]})

    @udf(return_dtype=DataType.int64(), batch_size=batch_size)
    def add_1(data):
        return pa.compute.add(data.to_arrow(), 1)

    result = table.eval_expression_list([add_1(col("a"))])
    assert result.to_pydict() == {"a": [2, 3, 4]}


@pytest.mark.parametrize("batch_size", [1, 2, 3, 4, 7, 8, 1000])
def test_udf_batch_size(batch_size):
    table = MicroPartition.from_pydict({"a": [1, 2, 3, 4, 5, 6, 7]})

    @udf(return_dtype=DataType.int64(), batch_size=batch_size)
    def add_1(data):
        lt = data.to_pylist()
        assert len(lt) <= batch_size
        return [x + 1 for x in lt]

    result = table.eval_expression_list([add_1(col("a"))])
    assert result.to_pydict() == {"a": [2, 3, 4, 5, 6, 7, 8]}


@pytest.mark.parametrize("batch_size", [1, 2, 3, 4, 7, 8, 1000])
def test_udf_batch_size_override(batch_size):
    table = MicroPartition.from_pydict({"a": [1, 2, 3, 4, 5, 6, 7]})

    @udf(return_dtype=DataType.int64())
    def add_1(data):
        lt = data.to_pylist()
        assert len(lt) <= batch_size
        return [x + 1 for x in lt]

    result = table.eval_expression_list([add_1.override_options(batch_size=batch_size)(col("a"))])
    assert result.to_pydict() == {"a": [2, 3, 4, 5, 6, 7, 8]}


def test_udf_invalid_batch_sizes():
    table = MicroPartition.from_pydict({"a": [1, 2, 3, 4, 5, 6, 7]})

    @udf(return_dtype=DataType.int64())
    def noop(data):
        return data

    with pytest.raises(ValueError, match="batch size must be positive"):
        table.eval_expression_list([noop.override_options(batch_size=0)(col("a"))])

    with pytest.raises(OverflowError):
        table.eval_expression_list([noop.override_options(batch_size=-1)(col("a"))])


@pytest.mark.parametrize("batch_size", [None, 1, 2])
@pytest.mark.parametrize("use_actor_pool", [False, True])
def test_udf_empty(batch_size, use_actor_pool):
    df = daft.from_pydict({"a": []})

    @udf(return_dtype=DataType.int64(), batch_size=batch_size)
    def identity(data):
        return data

    if use_actor_pool:
        identity = identity.with_concurrency(2)

    result = df.select(identity(col("a")))
    assert result.to_pydict() == {"a": []}


@pytest.mark.parametrize("use_actor_pool", [False, True])
def test_udf_with_error(use_actor_pool):
    import re

    df = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.int64())
    def fail_hard(a, b):
        raise ValueError("AN ERROR OCCURRED!")

    if use_actor_pool:
        fail_hard = fail_hard.with_concurrency(2)

    with pytest.raises(Exception) as exc_info:
        df.select(fail_hard(col("a"), col("b"))).collect()

    pattern = (
        r"AN ERROR OCCURRED!\n"
        r"User-defined function `<function test_udf_with_error\.<locals>\.fail_hard at 0x[0-9a-f]+>` "
        r"failed when executing on inputs:\s*"
        r"- a \(Int64, length=3\)\s*"
        r"- b \(Utf8, length=3\)$"
    )
    assert re.search(pattern, str(exc_info.value)), f"String doesn't end with expected pattern: {exc_info.value!s}"


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray"
    or get_context().daft_execution_config.use_experimental_distributed_engine is False,
    reason="requires Flotilla to be in use",
)
@pytest.mark.parametrize("use_actor_pool", [True, False])
def test_udf_retry_with_process_killed_ray(use_actor_pool):
    import os

    import ray

    df = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})

    @ray.remote
    class HasFailedAlready:
        def __init__(self):
            self.has_failed = False

        def should_fail(self) -> bool:
            if self.has_failed:
                return False
            self.has_failed = True
            return True

    @udf(return_dtype=DataType.int64())
    def random_exit_udf(a, b, has_failed_already: HasFailedAlready):
        if ray.get(has_failed_already.should_fail.remote()):
            os._exit(0)
        return a

    if use_actor_pool:
        random_exit_udf = random_exit_udf.with_concurrency(1)

    expr = random_exit_udf(col("a"), col("b"), HasFailedAlready.remote())
    df = df.select(expr)
    df.collect()


def test_non_batched_udf():
    @daft.func()
    def my_stringify_and_sum(a: int, b: int) -> str:
        return f"{a + b}"

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    actual = df.select(my_stringify_and_sum(col("x"), col("y"))).to_pydict()

    expected = {"x": ["5", "7", "9"]}

    assert actual == expected


def test_non_batched_udf_alternative_signature():
    @daft.func
    def my_stringify_and_sum(a: int, b: int) -> str:
        return f"{a + b}"

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    actual = df.select(my_stringify_and_sum(col("x"), col("y"))).to_pydict()

    expected = {"x": ["5", "7", "9"]}

    assert actual == expected


def test_non_batched_udf_should_infer_dtype_from_function():
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


def test_func_batch_same_as_udf():
    @daft.func.batch(return_dtype=int)
    def my_batch_sum(a, b):
        return a + b

    @daft.udf(return_dtype=int)
    def my_udf(a, b):
        return a + b

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    using_batch = df.select(my_batch_sum(col("x"), col("y"))).to_pydict()
    using_udf = df.select(my_udf(col("x"), col("y"))).to_pydict()

    assert using_batch == using_udf


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
@pytest.mark.parametrize("use_actor_pool", [False, True])
@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray"
    and get_context().daft_execution_config.use_experimental_distributed_engine is False,
    reason="Multiple UDFs on different columns fails on legacy ray runner",
)
def test_multiple_udfs_different_columns(batch_size, use_actor_pool):
    """Test running multiple UDFs on different columns simultaneously."""
    df = daft.from_pydict(
        {
            "strings": ["foo", "bar", "baz", "qux", "hello", "world", "test", "data", "more", "items"],
            "numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "floats": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5],
        }
    )

    @udf(return_dtype=DataType.string(), batch_size=batch_size)
    def uppercase_strings(data):
        return [s.upper() for s in data]

    @udf(return_dtype=DataType.int64(), batch_size=batch_size)
    def multiply_numbers(data):
        return [n * 10 for n in data]

    @udf(return_dtype=DataType.float64(), batch_size=batch_size)
    def square_floats(data):
        return [f**2 for f in data]

    if use_actor_pool:
        uppercase_strings = uppercase_strings.with_concurrency(1)
        multiply_numbers = multiply_numbers.with_concurrency(1)
        square_floats = square_floats.with_concurrency(1)

    # Apply all UDFs simultaneously on different columns
    result = df.select(
        uppercase_strings(col("strings")).alias("upper_strings"),
        multiply_numbers(col("numbers")).alias("mult_numbers"),
        square_floats(col("floats")).alias("squared_floats"),
    )

    expected = {
        "upper_strings": ["FOO", "BAR", "BAZ", "QUX", "HELLO", "WORLD", "TEST", "DATA", "MORE", "ITEMS"],
        "mult_numbers": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        "squared_floats": [2.25, 6.25, 12.25, 20.25, 30.25, 42.25, 56.25, 72.25, 90.25, 110.25],
    }

    assert result.to_pydict() == expected


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
@pytest.mark.parametrize("use_actor_pool", [False, True])
@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray"
    and get_context().daft_execution_config.use_experimental_distributed_engine is False,
    reason="Multiple UDFs on same column fails on legacy ray runner",
)
def test_multiple_udfs_same_column(batch_size, use_actor_pool):
    """Test running multiple UDFs on the same column simultaneously."""
    df = daft.from_pydict(
        {
            "numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )

    @udf(return_dtype=DataType.int64(), batch_size=batch_size)
    def multiply_by_2(data):
        return [n * 2 for n in data]

    @udf(return_dtype=DataType.int64(), batch_size=batch_size)
    def add_10(data):
        return [n + 10 for n in data]

    @udf(return_dtype=DataType.float64(), batch_size=batch_size)
    def square_and_divide(data):
        return [n**2 / 2.0 for n in data]

    @udf(return_dtype=DataType.string(), batch_size=batch_size)
    def number_to_string(data):
        return [f"num_{n}" for n in data]

    if use_actor_pool:
        multiply_by_2 = multiply_by_2.with_concurrency(1)
        add_10 = add_10.with_concurrency(1)
        square_and_divide = square_and_divide.with_concurrency(1)
        number_to_string = number_to_string.with_concurrency(1)

    # Apply all UDFs to the same column simultaneously
    result = df.select(
        multiply_by_2(col("numbers")).alias("doubled"),
        add_10(col("numbers")).alias("plus_ten"),
        square_and_divide(col("numbers")).alias("squared_halved"),
        number_to_string(col("numbers")).alias("stringified"),
    )

    expected = {
        "doubled": [2, 4, 6, 8, 10, 12, 14, 16, 18, 20],
        "plus_ten": [11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
        "squared_halved": [0.5, 2.0, 4.5, 8.0, 12.5, 18.0, 24.5, 32.0, 40.5, 50.0],
        "stringified": ["num_1", "num_2", "num_3", "num_4", "num_5", "num_6", "num_7", "num_8", "num_9", "num_10"],
    }

    assert result.to_pydict() == expected
