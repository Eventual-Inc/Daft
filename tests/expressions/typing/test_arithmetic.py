from __future__ import annotations

import operator as ops

import pytest

from daft.datatype import DataType
from daft.expressions import col
from tests.expressions.typing.conftest import (
    assert_typing_resolve_vs_runtime_behavior,
    has_supertype,
    is_numeric,
)


def plus_type_validation(lhs: DataType, rhs: DataType) -> bool:
    """Checks whether these input types are resolvable for the + operation"""

    # Plus only works for certain types
    for arg in (lhs, rhs):
        if not (is_numeric(arg) or (arg == DataType.string()) or (arg == DataType.bool()) or (arg == DataType.null())):
            return False

    return has_supertype(lhs, rhs)


def test_plus(binary_data_fixture):
    lhs, rhs = binary_data_fixture

    # TODO: [RUST-INT][TYPING] Add has not implemented all these types yet, enable tests when ready
    if (lhs.datatype() == DataType.null()) or (lhs.datatype() == DataType.bool()):
        return

    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=col(lhs.name()) + col(rhs.name()),
        run_kernel=lambda: lhs + rhs,
        resolvable=plus_type_validation(lhs.datatype(), rhs.datatype()),
    )


def binary_numeric_arithmetic_type_validation(lhs: DataType, rhs: DataType) -> bool:
    """Checks whether these input types are resolvable for arithmetic operations"""
    # (numeric <op> numeric = numeric)
    return is_numeric(lhs) and is_numeric(rhs) and has_supertype(lhs, rhs)


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(ops.sub, id="sub"),
        pytest.param(ops.sub, id="mul"),
        pytest.param(ops.sub, id="truediv"),
        pytest.param(ops.sub, id="mod"),
    ],
)
def test_binary_numeric_arithmetic(binary_data_fixture, op):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=op(col(lhs.name()), col(rhs.name())),
        run_kernel=lambda: op(lhs, rhs),
        resolvable=binary_numeric_arithmetic_type_validation(lhs.datatype(), rhs.datatype()),
    )


def test_abs(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=abs(col(arg.name())),
        run_kernel=lambda: abs(arg),
        resolvable=is_numeric(arg.datatype()),
    )
