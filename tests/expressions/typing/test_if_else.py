from __future__ import annotations

from daft.datatype import DataType
from daft.expressions import col
from daft.series import Series
from tests.expressions.typing.conftest import (
    assert_typing_resolve_vs_runtime_behavior,
    has_supertype,
    is_numeric,
)


def test_if_else(binary_data_fixture):
    lhs, rhs = binary_data_fixture
    predicate_series = Series.from_pylist([True, False, None], name="predicate")

    # TODO: [RUST-INT] If/Else has not implemented all these types yet, enable when ready
    kernel_not_implemented = not is_numeric(lhs.datatype()) and not lhs.datatype() == DataType.string()

    assert_typing_resolve_vs_runtime_behavior(
        data=(*binary_data_fixture, predicate_series),
        expr=col("predicate").if_else(col(lhs.name()), col(rhs.name())),
        run_kernel=lambda: predicate_series.if_else(lhs, rhs),
        resolvable=has_supertype(lhs.datatype(), rhs.datatype()) and (not kernel_not_implemented),
    )
