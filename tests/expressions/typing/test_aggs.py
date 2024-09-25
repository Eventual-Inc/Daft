from __future__ import annotations

import pytest

from daft.expressions import col
from tests.expressions.typing.conftest import (
    assert_typing_resolve_vs_runtime_behavior,
    is_comparable,
    is_numeric,
)


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(lambda x: x.min(), id="min"),
        pytest.param(lambda x: x.max(), id="max"),
    ],
)
def test_comparable_aggs(unary_data_fixture, op):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=op(col(arg.name())),
        run_kernel=lambda: op(arg),
        resolvable=is_comparable(arg.datatype()),
    )


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(lambda x: x.sum(), id="sum"),
        pytest.param(lambda x: x.mean(), id="mean"),
    ],
)
def test_numeric_aggs(unary_data_fixture, op):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=op(col(arg.name())),
        run_kernel=lambda: op(arg),
        resolvable=is_numeric(arg.datatype()),
    )


def test_decimal_sum(decimal_unary_data_fixture):
    """a copy of the above but for decimal types that do not more widely support
    numeric operations. When they do and can be added to ALL_DTYPES and resolve
    is_numeric to True, this test can be removed."""
    arg = decimal_unary_data_fixture

    def op(x):
        return x.sum()

    assert_typing_resolve_vs_runtime_behavior(
        data=(decimal_unary_data_fixture,),
        expr=op(col(arg.name())),
        run_kernel=lambda: op(arg),
        resolvable=True,
    )


def test_count(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).count(),
        run_kernel=lambda: arg.count(),
        resolvable=True,
    )
