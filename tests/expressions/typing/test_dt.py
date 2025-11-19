from __future__ import annotations

import pytest

from daft.expressions import col
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


@pytest.mark.parametrize(
    ("expr", "series_op"),
    [
        (lambda x: x.dt.day(), lambda x: x.dt.day()),
        (lambda x: x.dt.month(), lambda x: x.dt.month()),
        (lambda x: x.dt.year(), lambda x: x.dt.year()),
        (lambda x: x.dt.day_of_week(), lambda x: x.dt.day_of_week()),
        (lambda x: x.dt.date(), lambda x: x.dt.date()),
    ],
)
def test_dt_extraction_ops(unary_data_fixture, expr, series_op):
    arg = unary_data_fixture
    # For run_kernel, use Series.dt methods (Series namespace still exists)
    # Map function names back to Series.dt methods
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=expr(col(arg.name())),
        run_kernel=lambda: series_op(arg),
        resolvable=arg.datatype().is_temporal(),
    )
