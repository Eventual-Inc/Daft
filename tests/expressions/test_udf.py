from __future__ import annotations

import pytest

from daft import col
from daft.datatype import DataType
from daft.expressions import Expression
from daft.series import Series
from daft.table import Table
from daft.udf import udf


def test_udf():
    table = Table.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string())
    def repeat_n(data, n):
        return Series.from_pylist([d.as_py() * n for d in data.to_arrow()])

    expr = repeat_n(col("a"), 2)
    field = expr._to_field(table.schema())
    assert field.name == "a"
    assert field.dtype == DataType.string()

    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"a": ["foofoo", "barbar", "bazbaz"]}


def test_udf_error():
    table = Table.from_pydict({"a": ["foo", "bar", "baz"]})

    @udf(return_dtype=DataType.string())
    def throw_value_err(x):
        raise ValueError("AN ERROR OCCURRED!")

    expr = throw_value_err(col("a"))

    with pytest.raises(ValueError, match="DaftError::ComputeError Error occurred when running Python"):
        table.eval_expression_list([expr])

    # Assert that the error message includes a traceback
    with pytest.raises(ValueError, match=r"Traceback \(most recent call last\):"):
        table.eval_expression_list([expr])


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


@pytest.mark.skip(
    reason="[RUST-INT][UDF] repr is very naive at the moment py_udf(...exprs), we should fix to show all parameters and use the function name"
)
def test_udf_repr():
    @udf(return_dtype=DataType.int64(), expr_inputs=["x"])
    def single_arg_udf(x, y, z=10):
        pass

    assert single_arg_udf(col("x"), "y", z=20).__repr__() == "@udf[single_arg_udf](col('x'), 'y', z=20)"
