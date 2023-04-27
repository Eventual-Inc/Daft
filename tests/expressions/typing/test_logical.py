from __future__ import annotations

import operator as ops

import pytest

from daft.datatype import DataType
from daft.expressions import col
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


def logical_resolvable(lhs: DataType, rhs: DataType) -> bool:
    # Must have a Bool on one side; may have a Bool or Null on the other.
    return {lhs, rhs} in ({DataType.bool()}, {DataType.bool(), DataType.null()})


@pytest.mark.parametrize("op", [ops.and_, ops.or_])
def test_logical_binary(binary_data_fixture, op):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=op(col(lhs.name()), col(rhs.name())),
        run_kernel=lambda: op(lhs, rhs),
        resolvable=logical_resolvable(lhs.datatype(), rhs.datatype()),
    )


@pytest.mark.parametrize("op", [ops.invert])
def test_logical_unary(unary_data_fixture, op):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=op(col(arg.name())),
        run_kernel=lambda: op(arg),
        resolvable=arg.datatype() == DataType.bool(),
    )
