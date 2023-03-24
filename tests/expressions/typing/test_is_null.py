from __future__ import annotations

from daft.expressions import col
from daft.table import Table
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


def test_is_null(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict({arg.name(): arg}),
        col(arg.name()).is_null(),
        lambda tbl: tbl.get_column(arg.name()).is_null(),
        True,
    )
