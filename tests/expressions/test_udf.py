from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft import col
from daft.datatype import DataType
from daft.expressions import Expression
from daft.expressions.testing import expr_structurally_equal
from daft.series import Series
from daft.table import MicroPartition
from daft.udf import udf


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


def test_class_udf():
    table = MicroPartition.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string())
    class RepeatN:
        def __init__(self):
            self.n = 2

        def __call__(self, data):
            return Series.from_pylist([d.as_py() * self.n for d in data.to_arrow()])

    expr = RepeatN(col("a"))
    field = expr._to_field(table.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()

    result = table.eval_expression_list([expr])
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


def test_udf_tuples():
    table = MicroPartition.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string())
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
def test_udf_return_containers(container):
    table = MicroPartition.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string())
    def identity(data):
        if container == Series:
            return data
        elif container == list:
            return data.to_pylist()
        elif container == np.ndarray:
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


def test_no_args_udf_call():
    @udf(return_dtype=DataType.int64())
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


def test_class_udf_initialization_error():
    table = MicroPartition.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string())
    class IdentityWithInitError:
        def __init__(self):
            raise RuntimeError("UDF INIT ERROR")

        def __call__(self, data):
            return data

    expr = IdentityWithInitError(col("a"))
    with pytest.raises(RuntimeError, match="UDF INIT ERROR"):
        table.eval_expression_list([expr])


def test_udf_equality():
    @udf(return_dtype=DataType.int64())
    def udf1(x):
        pass

    assert expr_structurally_equal(udf1("x"), udf1("x"))
    assert not expr_structurally_equal(udf1("x"), udf1("y"))


def test_udf_return_tensor():
    @udf(return_dtype=DataType.tensor(DataType.float64()))
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


def test_udf_arbitrary_number_of_args():
    table = MicroPartition.from_pydict({"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]})

    @udf(return_dtype=DataType.int64())
    def add_cols_elementwise(*args):
        return Series.from_pylist([sum(x) for x in zip(*[arg.to_pylist() for arg in args])])

    expr = add_cols_elementwise(col("a"), col("b"), col("c"))
    field = expr._to_field(table.schema())
    assert field.name == "a"
    assert field.dtype == DataType.int64()

    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"a": [3, 6, 9]}


def test_udf_arbitrary_number_of_kwargs():
    table = MicroPartition.from_pydict({"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]})

    @udf(return_dtype=DataType.string())
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


def test_udf_arbitrary_number_of_args_with_kwargs():
    table = MicroPartition.from_pydict({"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]})

    @udf(return_dtype=DataType.int64())
    def add_cols_elementwise(*args, multiplier: float):
        return Series.from_pylist([multiplier * sum(x) for x in zip(*[arg.to_pylist() for arg in args])])

    expr = add_cols_elementwise(col("a"), col("b"), col("c"), multiplier=2)
    field = expr._to_field(table.schema())
    assert field.name == "a"
    assert field.dtype == DataType.int64()

    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"a": [6, 12, 18]}


def test_udf_return_pyarrow():
    table = MicroPartition.from_pydict({"a": [1, 2, 3]})

    @udf(return_dtype=DataType.int64())
    def add_1(data):
        return pa.compute.add(data.to_arrow(), 1)

    result = table.eval_expression_list([add_1(col("a"))])
    assert result.to_pydict() == {"a": [2, 3, 4]}
