from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.expressions import col
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(lambda data, pat: data.str.contains(pat), id="contains"),
        pytest.param(lambda data, pat: data.str.startswith(pat), id="startswith"),
        pytest.param(lambda data, pat: data.str.endswith(pat), id="endswith"),
        pytest.param(lambda data, pat: data.str.concat(pat), id="concat"),
    ],
)
def test_str_compares(binary_data_fixture, op, request):
    lhs, rhs = binary_data_fixture
    if "concat" in request.node.callspec.id and (
        lhs.datatype() != DataType.string() or rhs.datatype() != DataType.string()
    ):
        # Only test concat with strings, since other types will have their own semantics
        # for the underlying + operator.
        return
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=op(col(lhs.name()), col(rhs.name())),
        run_kernel=lambda: op(lhs, rhs),
        resolvable=(lhs.datatype() == DataType.string()) and (rhs.datatype() == DataType.string()),
    )
