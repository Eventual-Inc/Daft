from __future__ import annotations

from operator import and_, or_, xor

import pytest

from daft.expressions import col
from daft.table import MicroPartition

BITWISE_OPERATORS = [and_, or_, xor]


@pytest.mark.parametrize("op", BITWISE_OPERATORS)
@pytest.mark.parametrize(
    "left, right",
    [
        pytest.param([0b1100, 0b1010, 0b1001], [0b1010, 0b1100, 0b1001], id="no_nulls"),
        pytest.param([0b1100, None, 0b1001], [0b1010, 0b1100, None], id="with_nulls"),
        pytest.param([None, None, None], [0b1010, 0b1100, 0b1001], id="left_nulls"),
        pytest.param([0b1100, 0b1010, 0b1001], [None, None, None], id="right_nulls"),
    ],
)
def test_bitwise_op(op, left, right):
    table = MicroPartition.from_pydict({"left": left, "right": right})

    result = table.eval_expression_list([op(col("left"), col("right"))])
    expected = [op(i, j) if i is not None and j is not None else None for i, j in zip(left, right)]
    assert result.to_pydict()["left"] == expected


@pytest.mark.parametrize(
    "expression, op",
    [
        (col("left").bitwise_and(col("right")), and_),
        (col("left").bitwise_or(col("right")), or_),
        (col("left").bitwise_xor(col("right")), xor),
    ],
)
@pytest.mark.parametrize(
    "left, right",
    [
        pytest.param([0b1100, 0b1010, 0b1001], [0b1010, 0b1100, 0b1001], id="no_nulls"),
        pytest.param([0b1100, None, 0b1001], [0b1010, 0b1100, None], id="with_nulls"),
        pytest.param([None, None, None], [0b1010, 0b1100, 0b1001], id="left_nulls"),
        pytest.param([0b1100, 0b1010, 0b1001], [None, None, None], id="right_nulls"),
    ],
)
def test_bitwise_expression(expression, op, left, right):
    table = MicroPartition.from_pydict({"left": left, "right": right})

    result = table.eval_expression_list([expression])
    expected = [op(i, j) if i is not None and j is not None else None for i, j in zip(left, right)]
    assert result.to_pydict()["left"] == expected


@pytest.mark.parametrize("op", BITWISE_OPERATORS)
@pytest.mark.parametrize(
    "data, scalar",
    [
        pytest.param([0b1100, 0b1010, 0b1001], 0b1010, id="no_nulls"),
        pytest.param([0b1100, None, 0b1001], None, id="with_nulls"),
    ],
)
def test_bitwise_scalar(op, data, scalar):
    table = MicroPartition.from_pydict({"data": data})

    result = table.eval_expression_list([op(col("data"), scalar)])
    expected = [op(d, scalar) if d is not None and scalar is not None else None for d in data]
    assert result.to_pydict()["data"] == expected
