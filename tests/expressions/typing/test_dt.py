from __future__ import annotations

import pytest

import daft
from daft.expressions import col
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


@pytest.mark.parametrize(
    ("op", "fn_name"),
    [
        (lambda x: daft.functions.day(x), "day"),
        (lambda x: daft.functions.month(x), "month"),
        (lambda x: daft.functions.year(x), "year"),
        (lambda x: daft.functions.day_of_week(x), "day_of_week"),
        (lambda x: daft.functions.date(x), "date"),
    ],
)
def test_dt_extraction_ops(unary_data_fixture, op, fn_name):
    arg = unary_data_fixture
    # For run_kernel, use Series.dt methods (Series namespace still exists)
    # Map function names back to Series.dt methods
    fn_name_map = {
        "day": lambda x: x.dt.day(),
        "month": lambda x: x.dt.month(),
        "year": lambda x: x.dt.year(),
        "day_of_week": lambda x: x.dt.day_of_week(),
        "date": lambda x: x.dt.date(),
    }
    run_kernel_op = fn_name_map[fn_name]
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=op(col(arg.name())),
        run_kernel=lambda: run_kernel_op(arg),
        resolvable=arg.datatype().is_temporal(),
    )
