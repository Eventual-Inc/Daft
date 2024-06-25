from __future__ import annotations

import pytest

from daft.expressions import col
from daft.table import MicroPartition
from operator import and_, or_, xor

BITWISE_OPERATORS = [and_, or_, xor]


@pytest.mark.parametrize("op", BITWISE_OPERATORS)
def test_bitwise_op(op):
    left = [0b1100, 0b1010, 0b1001]
    right = [0b1010, 0b1100, 0b1001]
    table = MicroPartition.from_pydict({"left": left, "right": right})

    result = table.eval_expression_list([op(col("left"), col("right"))])
    expected = [op(l, r) for l, r in zip(left, right)]
    assert result.to_pydict()["left"] == expected


@pytest.mark.parametrize(
    "expression, op",
    [
        (col("left").bitwise_and(col("right")), and_),
        (col("left").bitwise_or(col("right")), or_),
        (col("left").bitwise_xor(col("right")), xor),
    ],
)
def test_bitwise_expression(expression, op):
    left = [0b1100, 0b1010, 0b1001]
    right = [0b1010, 0b1100, 0b1001]
    table = MicroPartition.from_pydict({"left": left, "right": right})

    result = table.eval_expression_list([expression])
    expected = [op(l, r) for l, r in zip(left, right)]
    assert result.to_pydict()["left"] == expected


@pytest.mark.parametrize("op", BITWISE_OPERATORS)
def test_bitwise_scalar(op):
    data = [0b1010, 0b1100, 0b1001]
    scalar = 0b1010
    table = MicroPartition.from_pydict({"data": data})

    result = table.eval_expression_list([op(col("data"), scalar)])
    expected = [op(d, scalar) for d in data]
    assert result.to_pydict()["data"] == expected
