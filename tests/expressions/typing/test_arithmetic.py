from __future__ import annotations

import operator as ops

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.table import Table
from tests.expressions.typing.conftest import (
    assert_typing_resolve_vs_runtime_behavior,
    is_numeric,
)


def plus_resolvable(lhs: DataType, rhs: DataType) -> bool:
    """Checks whether these input types are resolvable for the + operation"""
    return (
        # (numeric + numeric = numeric)
        (is_numeric(lhs) and is_numeric(rhs))
        or
        # (string + string = string)
        (lhs == DataType.string() and rhs == DataType.string())
    )


def test_plus(binary_data_fixture):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict({s.name(): s for s in binary_data_fixture}),
        col(lhs.name()) + col(rhs.name()),
        lambda tbl: tbl.get_column(lhs.name()) + tbl.get_column(rhs.name()),
        plus_resolvable(lhs.datatype(), rhs.datatype()),
    )


def arithmetic_resolvable(lhs: DataType, rhs: DataType) -> bool:
    """Checks whether these input types are resolvable for arithmetic operations"""
    # (numeric <op> numeric = numeric)
    return is_numeric(lhs) and is_numeric(rhs)


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(ops.sub, id="sub"),
        pytest.param(ops.sub, id="mul"),
        pytest.param(ops.sub, id="truediv"),
        pytest.param(ops.sub, id="mod"),
    ],
)
def test_arithmetic(binary_data_fixture, op):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict({s.name(): s for s in binary_data_fixture}),
        op(col(lhs.name()), col(rhs.name())),
        lambda tbl: op(tbl.get_column(lhs.name()), tbl.get_column(rhs.name())),
        arithmetic_resolvable(lhs.datatype(), rhs.datatype()),
    )
