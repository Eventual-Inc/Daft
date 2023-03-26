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
        pytest.param(lambda x: x._min(), id="min"),
        pytest.param(lambda x: x._max(), id="max"),
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
        pytest.param(lambda x: x._sum(), id="sum"),
        pytest.param(lambda x: x._mean(), id="mean"),
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


def test_count(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name())._count(),
        run_kernel=lambda: arg._count(),
        resolvable=True,
    )
