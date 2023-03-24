from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.table import Table
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(lambda x: x.dt.year(), id="year"),
    ],
)
def test_dt_extraction_ops(unary_data_fixture, op):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict({arg.name(): arg}),
        op(col(arg.name())),
        lambda tbl: op(tbl.get_column(arg.name())),
        arg.datatype() == DataType.date(),
    )
