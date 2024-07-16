import itertools
from operator import add, mul, sub, truediv

import numpy as np
import pytest
from numpy.testing import assert_almost_equal

import daft
from daft import DataType

daft_signed_int_types = [
    DataType.int8(),
    DataType.int16(),
    DataType.int32(),
    DataType.int64(),
]

daft_numeric_types = daft_signed_int_types + [DataType.float32(), DataType.float64()]


@pytest.mark.parametrize(
    "op, ldtype, rdtype", itertools.product([add, mul, sub, truediv], daft_numeric_types, daft_numeric_types)
)
def test_math_tensors(op, ldtype, rdtype) -> None:
    np.random.seed(1)
    x = np.random.randint(0, 10, (12, 10, 1)).astype(ldtype.to_arrow_dtype().to_pandas_dtype())
    y = np.random.randint(0, 10, (12, 10, 1)).astype(rdtype.to_arrow_dtype().to_pandas_dtype())
    expected = op(x, y)

    df = daft.from_pydict({"x": x, "y": y})
    df = df.with_column("x", df["x"].cast(daft.DataType.tensor(ldtype, (10, 1))))
    df = df.with_column("y", df["y"].cast(daft.DataType.tensor(rdtype, (10, 1))))

    result = df.with_column("z", op(df["x"], df["y"])).collect()
    if op == truediv:
        assert result.schema()["z"].dtype == daft.DataType.tensor(daft.DataType.float64(), (10, 1))
    assert_almost_equal(result.to_pydict()["z"], expected)


@pytest.mark.parametrize(
    "op, ldtype, rdtype", itertools.product([add, mul, sub, truediv], daft_numeric_types, daft_numeric_types)
)
def test_math_tensors_with_literal(op, ldtype, rdtype) -> None:
    np.random.seed(1)
    x = np.random.randint(0, 10, (12, 10, 1)).astype(ldtype.to_arrow_dtype().to_pandas_dtype())
    y = np.random.randint(0, 10, (10, 1)).astype(rdtype.to_arrow_dtype().to_pandas_dtype())
    expected = op(x, y)

    df = daft.from_pydict({"x": x})
    df = df.with_column("x", df["x"].cast(daft.DataType.tensor(ldtype, (10, 1))))

    result = df.with_column("z", op(df["x"], daft.lit(y).cast(daft.DataType.tensor(rdtype, (10, 1))))).collect()
    if op == truediv:
        assert result.schema()["z"].dtype == daft.DataType.tensor(daft.DataType.float64(), (10, 1))

    assert_almost_equal(result.to_pydict()["z"], expected)


@pytest.mark.parametrize("op, ldtype", itertools.product([add, mul, sub, truediv], daft_numeric_types))
def test_math_tensors_with_null_literal(op, ldtype) -> None:
    np.random.seed(1)
    x = np.random.randint(0, 10, (12, 10, 1)).astype(ldtype.to_arrow_dtype().to_pandas_dtype())

    df = daft.from_pydict({"x": x})
    df = df.with_column("x", df["x"].cast(daft.DataType.tensor(ldtype, (10, 1))))

    result = df.with_column("z", op(df["x"], daft.lit(None).cast(daft.DataType.tensor(ldtype, (10, 1))))).collect()
    if op == truediv:
        assert result.schema()["z"].dtype == daft.DataType.tensor(daft.DataType.float64(), (10, 1))

    assert result.to_pydict()["z"] == [None] * 12
