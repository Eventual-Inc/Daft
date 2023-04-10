from __future__ import annotations

import pytest

from daft import col
from daft.datatype import DataType
from daft.expressions import Expression
from daft.udf import udf


def test_no_args_udf():
    @udf(return_dtype=DataType.int64(), expr_inputs=[])
    def udf_no_args():
        pass

    assert isinstance(udf_no_args(), Expression)

    with pytest.raises(TypeError):
        udf_no_args("invalid")

    with pytest.raises(TypeError):
        udf_no_args(invalid="invalid")


def test_full_udf():
    @udf(return_dtype=DataType.int64(), expr_inputs=["e_arg"])
    def full_udf(e_arg, val, kwarg_val=None):
        pass

    assert isinstance(full_udf(col("x"), 1, kwarg_val=0), Expression)

    with pytest.raises(TypeError):
        full_udf()
    with pytest.raises(TypeError):
        # Args specified in input_columns must be expressions
        full_udf("invalid", 1, kwarg_val=0)
    with pytest.raises(TypeError):
        # Args not specified in input_columns must not be expressions
        full_udf(col("x"), col("invalid"), kwarg_val=0)


@pytest.mark.skip(
    reason="[RUST-INT][UDF] repr is very naive at the moment py_udf(...exprs), we should fix to show all parameters and use the function name"
)
def test_udf_repr():
    @udf(return_dtype=DataType.int64(), expr_inputs=["x"])
    def single_arg_udf(x, y, z=10):
        pass

    assert single_arg_udf(col("x"), "y", z=20).__repr__() == "@udf[single_arg_udf](col('x'), 'y', z=20)"
