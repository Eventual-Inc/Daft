from __future__ import annotations

import daft
from daft.expressions import col
from daft.series import Series
from tests.expressions.typing.conftest import (
    assert_typing_resolve_vs_runtime_behavior,
    has_supertype,
)


def test_if_else(binary_data_fixture):
    lhs, rhs = binary_data_fixture
    predicate_series = Series.from_pylist([True, False, None], name="predicate")

    assert_typing_resolve_vs_runtime_behavior(
        data=(*binary_data_fixture, predicate_series),
        expr=daft.functions.when(col("predicate"), col(lhs.name())).otherwise(col(rhs.name())),
        run_kernel=lambda: predicate_series.if_else(lhs, rhs),
        resolvable=has_supertype(lhs.datatype(), rhs.datatype()),
    )
