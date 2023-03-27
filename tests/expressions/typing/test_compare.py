from __future__ import annotations

import operator as ops

import pytest

from daft.datatype import DataType
from daft.expressions import col
from tests.expressions.typing.conftest import (
    assert_typing_resolve_vs_runtime_behavior,
    has_supertype,
    is_comparable,
)


def comparable_type_validation(lhs: DataType, rhs: DataType) -> bool:
    return is_comparable(lhs) and is_comparable(rhs) and has_supertype(lhs, rhs)


@pytest.mark.parametrize("op", [ops.eq, ops.ne, ops.lt, ops.le, ops.gt, ops.ge])
def test_comparable(binary_data_fixture, op):
    lhs, rhs = binary_data_fixture
    assert_typing_resolve_vs_runtime_behavior(
        data=binary_data_fixture,
        expr=op(col(lhs.name()), col(rhs.name())),
        run_kernel=lambda: op(lhs, rhs),
        resolvable=comparable_type_validation(lhs.datatype(), rhs.datatype()),
    )
