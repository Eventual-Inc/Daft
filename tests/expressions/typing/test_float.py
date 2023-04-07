from __future__ import annotations

from daft.datatype import DataType
from daft.expressions import col
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


def test_float_is_nan(unary_data_fixture):
    assert_typing_resolve_vs_runtime_behavior(
        data=[unary_data_fixture],
        expr=col(unary_data_fixture.name()).float.is_nan(),
        run_kernel=unary_data_fixture.float.is_nan,
        resolvable=unary_data_fixture.datatype() in (DataType.float32(), DataType.float64()),
    )
