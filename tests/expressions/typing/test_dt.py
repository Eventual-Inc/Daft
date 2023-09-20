from __future__ import annotations

import pytest

from daft.expressions import col
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


@pytest.mark.parametrize(
    "op",
    [
        pytest.param(lambda x: x.dt.day(), id="day"),
        pytest.param(lambda x: x.dt.month(), id="month"),
        pytest.param(lambda x: x.dt.year(), id="year"),
        pytest.param(lambda x: x.dt.day_of_week(), id="day_of_week"),
        pytest.param(lambda x: x.dt.date(), id="date"),
    ],
)
def test_dt_extraction_ops(unary_data_fixture, op):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=op(col(arg.name())),
        run_kernel=lambda: op(arg),
        resolvable=arg.datatype()._is_temporal_type(),
    )
