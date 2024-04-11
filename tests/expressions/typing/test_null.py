from __future__ import annotations

from daft.expressions import col
from tests.expressions.typing.conftest import (
    assert_typing_resolve_vs_runtime_behavior,
    has_supertype,
)


def test_is_null(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).is_null(),
        run_kernel=lambda: arg.is_null(),
        resolvable=True,
    )


def test_not_null(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).not_null(),
        run_kernel=lambda: arg.not_null(),
        resolvable=True,
    )


def test_fill_null(binary_data_fixture):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=col(lhs.name()).fill_null(col(rhs.name())),
        run_kernel=lambda: lhs.fill_null(rhs),
        resolvable=has_supertype(lhs.datatype(), rhs.datatype()),
    )
