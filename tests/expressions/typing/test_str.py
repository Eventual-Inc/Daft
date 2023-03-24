from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.table import Table
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(lambda data, pat: data.str.contains(pat), id="contains"),
        pytest.param(lambda data, pat: data.str.startswith(pat), id="startswith"),
        pytest.param(lambda data, pat: data.str.endswith(pat), id="endswith"),
    ],
)
def test_str_compares(binary_data_fixture, op):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict({s.name(): s for s in binary_data_fixture}),
        op(col(lhs.name()), col(rhs.name())),
        lambda tbl: op(tbl.get_column(lhs.name()), tbl.get_column(rhs.name())),
        (lhs.datatype() == DataType.string()) and (rhs.datatype() == DataType.string()),
    )
