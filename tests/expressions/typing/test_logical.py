from __future__ import annotations

import operator as ops

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.table import Table
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


def logical_resolvable(lhs: DataType, rhs: DataType) -> bool:
    return (lhs == DataType.bool()) and (rhs == DataType.bool())


@pytest.mark.parametrize("op", [ops.and_, ops.or_])
def test_logical_binary(binary_data_fixture, op):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict({s.name(): s for s in binary_data_fixture}),
        op(col(lhs.name()), col(rhs.name())),
        lambda tbl: op(tbl.get_column(lhs.name()), tbl.get_column(rhs.name())),
        logical_resolvable(lhs.datatype(), rhs.datatype()),
    )


@pytest.mark.parametrize("op", [ops.invert])
def test_logical_unary(unary_data_fixture, op):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict({arg.name(): arg}),
        op(col(arg.name())),
        lambda tbl: op(tbl.get_column(arg.name())),
        arg.datatype() == DataType.bool(),
    )
