from __future__ import annotations

from datetime import date

import pytest

from daft.datatype import DataType
from daft.expressions2 import col, lit
from daft.table import Table


@pytest.mark.parametrize(
    "data, expected_dtype",
    [
        (1, DataType.int32()),
        (2**32, DataType.int64()),
        (1 << 63, DataType.uint64()),
        (1.2, DataType.float64()),
        ("a", DataType.string()),
        (b"a", DataType.binary()),
        (True, DataType.bool()),
        (None, DataType.null()),
        (date(2023, 1, 1), DataType.date()),
    ],
)
def test_make_lit(data, expected_dtype) -> None:
    l = lit(data)
    assert l.name() == "literal"
    empty_table = Table.empty()
    lit_table = empty_table.eval_expression_list([l])
    series = lit_table.get_column("literal")
    assert series.datatype() == expected_dtype
    repr_out = repr(l)
    if expected_dtype != DataType.date():
        assert repr_out.startswith("lit(")
        assert repr_out.endswith(")")


import operator as ops

OPS = [
    (ops.add, "+"),
    (ops.sub, "-"),
    (ops.mul, "*"),
    (ops.truediv, "/"),
    (ops.mod, "%"),
    (ops.lt, "<"),
    (ops.le, "<="),
    (ops.eq, "=="),
    (ops.ne, "!="),
    (ops.ge, ">="),
    (ops.gt, ">"),
]


@pytest.mark.parametrize("op, symbol", OPS)
def test_repr_binary_operators(op, symbol) -> None:
    a = col("a")
    b = col("b")
    y = op(a, b)
    output = repr(y)
    tokens = output.split(" ")
    assert len(tokens) == 3
    assert tokens[0] == "col(a)"
    assert tokens[1] == symbol
    assert tokens[2] == "col(b)"


def test_repr_functions_abs() -> None:
    a = col("a")
    y = abs(a)
    repr_out = repr(y)
    assert repr_out == "abs(col(a))"


def test_repr_functions_year() -> None:
    a = col("a")
    y = a.dt.year()
    repr_out = repr(y)
    assert repr_out == "year(col(a))"
