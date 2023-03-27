from __future__ import annotations

from daft.expressions import col
from tests.expressions.typing.conftest import assert_typing_resolve_vs_runtime_behavior


def test_is_null(unary_data_fixture):
    arg = unary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=(unary_data_fixture,),
        expr=col(arg.name()).is_null(),
        run_kernel=lambda: arg.is_null(),
        resolvable=True,
    )
