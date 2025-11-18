from __future__ import annotations

import daft
from daft.datatype import DataType
from daft.expressions import col
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


def test_float_is_nan(unary_data_fixture):
    assert_typing_resolve_vs_runtime_behavior(
        data=[unary_data_fixture],
        expr=daft.functions.is_nan(col(unary_data_fixture.name())),
        run_kernel=unary_data_fixture.float.is_nan,
        resolvable=unary_data_fixture.datatype() in (DataType.float32(), DataType.float64(), DataType.null()),
    )


def test_float_is_inf(unary_data_fixture):
    assert_typing_resolve_vs_runtime_behavior(
        data=[unary_data_fixture],
        expr=daft.functions.is_inf(col(unary_data_fixture.name())),
        run_kernel=unary_data_fixture.float.is_inf,
        resolvable=unary_data_fixture.datatype() in (DataType.float32(), DataType.float64(), DataType.null()),
    )


def test_float_not_nan(unary_data_fixture):
    assert_typing_resolve_vs_runtime_behavior(
        data=[unary_data_fixture],
        expr=daft.functions.not_nan(col(unary_data_fixture.name())),
        run_kernel=unary_data_fixture.float.not_nan,
        resolvable=unary_data_fixture.datatype() in (DataType.float32(), DataType.float64(), DataType.null()),
    )


def test_fill_nan(binary_data_fixture):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=daft.functions.fill_nan(col(lhs.name()), col(rhs.name())),
        run_kernel=lambda: lhs.float.fill_nan(rhs),
        resolvable=(
            lhs.datatype() in (DataType.float32(), DataType.float64())
            and rhs.datatype() in (DataType.float32(), DataType.float64())
        )
        or lhs.datatype() == DataType.null(),
    )
