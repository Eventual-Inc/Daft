from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.table import Table
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

    # TODO: [RUST-INT][LOGICAL] date min/max currently not implemented and will panic, so we skip tests here
    if arg.datatype() == DataType.date():
        return

    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict({arg.name(): arg}),
        op(col(arg.name())),
        lambda tbl: op(tbl.get_column(arg.name())),
        is_comparable(arg.datatype()),
    )


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(lambda x: x._sum(), id="sum"),
        pytest.param(lambda x: x._mean(), id="mean"),
    ],
)
def test_arithmetic_aggs(unary_data_fixture, op):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict({arg.name(): arg}),
        op(col(arg.name())),
        lambda tbl: op(tbl.get_column(arg.name())),
        is_numeric(arg.datatype()),
    )


def test_count(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict({arg.name(): arg}),
        col(arg.name())._count(),
        lambda tbl: tbl.get_column(arg.name())._count(),
        True,
    )
