from __future__ import annotations

import math
import operator as ops

import pytest

from daft.datatype import DataType
from daft.expressions import col
from tests.expressions.typing.conftest import (
    assert_typing_resolve_vs_runtime_behavior,
    has_supertype,
    is_integer,
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


def binary_numeric_arithmetic_type_validation(lhs: DataType, rhs: DataType, op: ops) -> bool:
    """Checks whether these input types are resolvable for arithmetic operations"""
    # (temporal - temporal = duration)
    if lhs._is_temporal_type() and rhs._is_temporal_type() and lhs == rhs and op == ops.sub:
        return True

    # (numeric <op> numeric = numeric)
    return is_numeric(lhs) and is_numeric(rhs) and has_supertype(lhs, rhs)


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(ops.sub, id="sub"),
        pytest.param(ops.mul, id="mul"),
        pytest.param(ops.truediv, id="truediv"),
        pytest.param(ops.mod, id="mod"),
    ],
)
def test_binary_numeric_arithmetic(binary_data_fixture, op):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=op(col(lhs.name()), col(rhs.name())),
        run_kernel=lambda: op(lhs, rhs),
        resolvable=binary_numeric_arithmetic_type_validation(lhs.datatype(), rhs.datatype(), op),
    )


def test_abs(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=abs(col(arg.name())),
        run_kernel=lambda: abs(arg),
        resolvable=is_numeric(arg.datatype()),
    )


def test_ceil(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).ceil(),
        run_kernel=lambda: arg.ceil(),
        resolvable=is_numeric(arg.datatype()),
    )


def test_floor(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).floor(),
        run_kernel=lambda: arg.floor(),
        resolvable=is_numeric(arg.datatype()),
    )


def test_sign(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).sign(),
        run_kernel=lambda: arg.sign(),
        resolvable=is_numeric(arg.datatype()),
    )


def test_round(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).round(0),
        run_kernel=lambda: arg.round(0),
        resolvable=is_numeric(arg.datatype()),
    )


def test_log2(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).log2(),
        run_kernel=lambda: arg.log2(),
        resolvable=is_numeric(arg.datatype()),
    )


def test_log10(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).log10(),
        run_kernel=lambda: arg.log10(),
        resolvable=is_numeric(arg.datatype()),
    )


@pytest.mark.parametrize(
    "base",
    [2, 10, 100, math.e],
)
def test_log(unary_data_fixture, base: float):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).log(base=base),
        run_kernel=lambda: arg.log(base=base),
        resolvable=is_numeric(arg.datatype()),
    )


def test_ln(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).ln(),
        run_kernel=lambda: arg.ln(),
        resolvable=is_numeric(arg.datatype()),
    )


@pytest.mark.parametrize(
    "fun",
    [
        "sin",
        "cos",
        "tan",
        "cot",
        "arcsin",
        "arccos",
        "arctan",
        "radians",
        "degrees",
    ],
)
def test_trigonometry(fun: str, unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=getattr(col(arg.name()), fun)(),
        run_kernel=lambda: getattr(arg, fun)(),
        resolvable=is_numeric(arg.datatype()),
    )


def test_atan2(binary_data_fixture):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=col(lhs.name()).arctan2(col(rhs.name())),
        run_kernel=lambda: lhs.arctan2(rhs),
        resolvable=is_numeric(lhs.datatype()) and is_numeric(rhs.datatype()),
    )


def test_atanh(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).arctanh(),
        run_kernel=lambda: arg.arctanh(),
        resolvable=is_numeric(arg.datatype()),
    )


def test_asinh(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).arcsinh(),
        run_kernel=lambda: arg.arcsinh(),
        resolvable=is_numeric(arg.datatype()),
    )


def test_acosh(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).arccosh(),
        run_kernel=lambda: arg.arccosh(),
        resolvable=is_numeric(arg.datatype()),
    )


def test_exp(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).exp(),
        run_kernel=lambda: arg.exp(),
        resolvable=is_numeric(arg.datatype()),
    )


def test_sqrt(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).sqrt(),
        run_kernel=lambda: arg.sqrt(),
        resolvable=is_numeric(arg.datatype()),
    )


def test_shift_left(binary_data_fixture):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=col(lhs.name()) << (col(rhs.name())),
        run_kernel=lambda: lhs.shift_left(rhs),
        resolvable=is_integer(lhs.datatype()) and is_integer(rhs.datatype()),
    )


def test_shift_right(binary_data_fixture):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=col(lhs.name()) >> (col(rhs.name())),
        run_kernel=lambda: lhs.shift_right(rhs),
        resolvable=is_integer(lhs.datatype()) and is_integer(rhs.datatype()),
    )
