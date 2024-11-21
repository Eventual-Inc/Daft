from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft import col
from daft.context import get_context, set_planning_config
from daft.datatype import DataType
from daft.expressions import Expression
from daft.expressions.testing import expr_structurally_equal
from daft.series import Series
from daft.table import MicroPartition
from daft.udf import udf


@pytest.fixture(scope="function", params=[False, True])
def actor_pool_enabled(request):
    original_config = get_context().daft_planning_config
    try:
        set_planning_config(
            config=get_context().daft_planning_config.with_config_values(enable_actor_pool_projections=request.param)
        )
        yield request.param
    finally:
        set_planning_config(config=original_config)


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
def test_class_udf(batch_size, actor_pool_enabled):
    df = daft.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string(), batch_size=batch_size)
    class RepeatN:
        def __init__(self):
            self.n = 2

        def __call__(self, data):
            return Series.from_pylist([d.as_py() * self.n for d in data.to_arrow()])

    if actor_pool_enabled:
        RepeatN = RepeatN.with_concurrency(1)

    expr = RepeatN(col("a"))
    field = expr._to_field(df.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()

    result = df.select(expr)
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
def test_class_udf_init_args(batch_size, actor_pool_enabled):
    df = daft.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string(), batch_size=batch_size)
    class RepeatN:
        def __init__(self, initial_n: int = 2):
            self.n = initial_n

        def __call__(self, data):
            return Series.from_pylist([d.as_py() * self.n for d in data.to_arrow()])

    if actor_pool_enabled:
        RepeatN = RepeatN.with_concurrency(1)

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
def test_class_udf_init_args_no_default(batch_size, actor_pool_enabled):
    df = daft.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string(), batch_size=batch_size)
    class RepeatN:
        def __init__(self, initial_n):
            self.n = initial_n

        def __call__(self, data):
            return Series.from_pylist([d.as_py() * self.n for d in data.to_arrow()])

    if actor_pool_enabled:
        RepeatN = RepeatN.with_concurrency(1)

    with pytest.raises(ValueError, match="Cannot call StatefulUDF without initialization arguments."):
        RepeatN(col("a"))

    expr = RepeatN.with_init_args(initial_n=2)(col("a"))
    field = expr._to_field(df.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()
    result = df.select(expr)
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}


def test_class_udf_init_args_bad_args(actor_pool_enabled):
    @udf(return_dtype=DataType.string())
    class RepeatN:
        def __init__(self, initial_n):
            self.n = initial_n

        def __call__(self, data):
            return Series.from_pylist([d.as_py() * self.n for d in data.to_arrow()])

    if actor_pool_enabled:
        RepeatN = RepeatN.with_concurrency(1)

    with pytest.raises(TypeError, match="missing a required argument: 'initial_n'"):
        RepeatN.with_init_args(wrong=5)


@pytest.mark.parametrize("concurrency", [1, 2, 4])
@pytest.mark.parametrize("actor_pool_enabled", [True], indirect=True)
def test_stateful_udf_concurrency(concurrency, actor_pool_enabled):
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

    with pytest.raises(RuntimeError) as exc_info:
        table.eval_expression_list([expr])
    assert isinstance(exc_info.value.__cause__, ValueError)
    assert str(exc_info.value.__cause__) == "AN ERROR OCCURRED!"


@pytest.mark.parametrize("batch_size", [None, 1, 2, 3, 10])
def test_no_args_udf_call(batch_size):
    @udf(return_dtype=DataType.int64(), batch_size=batch_size)
    def udf_no_args():
        pass

    assert isinstance(udf_no_args(), Expression)

    with pytest.raises(TypeError):
        udf_no_args("invalid")

    with pytest.raises(TypeError):
        udf_no_args(invalid="invalid")


def test_full_udf_call():
    @udf(return_dtype=DataType.int64())
    def full_udf(e_arg, val, kwarg_val=None, kwarg_ex=None):
        pass

    assert isinstance(full_udf(col("x"), 1, kwarg_val=0, kwarg_ex=col("y")), Expression)

    with pytest.raises(TypeError):
        full_udf()


def test_class_udf_initialization_error(actor_pool_enabled):
    df = daft.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string())
    class IdentityWithInitError:
        def __init__(self):
            raise RuntimeError("UDF INIT ERROR")

        def __call__(self, data):
            return data

    if actor_pool_enabled:
        IdentityWithInitError = IdentityWithInitError.with_concurrency(1)

    expr = IdentityWithInitError(col("a"))
    if actor_pool_enabled:
        with pytest.raises(Exception):
            df.select(expr).collect()
    else:
        with pytest.raises(RuntimeError, match="UDF INIT ERROR"):
            df.select(expr).collect()


def test_udf_equality():
    @udf(return_dtype=DataType.int64())
    def udf1(x):
        pass

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
        length = len(data[list(data.keys())[0]])
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
