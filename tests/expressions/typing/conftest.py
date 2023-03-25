from __future__ import annotations

import datetime
import itertools
from typing import Protocol

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.expressions import Expression, ExpressionsProjection
from daft.series import Series
from daft.table import Table

ALL_DTYPES = [
    (DataType.int8(), pa.array([1, 2, None], type=pa.int8())),
    (DataType.int16(), pa.array([1, 2, None], type=pa.int16())),
    (DataType.int32(), pa.array([1, 2, None], type=pa.int32())),
    (DataType.int64(), pa.array([1, 2, None], type=pa.int64())),
    (DataType.uint8(), pa.array([1, 2, None], type=pa.uint8())),
    (DataType.uint16(), pa.array([1, 2, None], type=pa.uint16())),
    (DataType.uint32(), pa.array([1, 2, None], type=pa.uint32())),
    (DataType.uint64(), pa.array([1, 2, None], type=pa.uint64())),
    (DataType.float32(), pa.array([1, 2, None], type=pa.float32())),
    (DataType.float64(), pa.array([1, 2, None], type=pa.float64())),
    (DataType.string(), pa.array(["1", "2", "3"], type=pa.string())),
    (DataType.bool(), pa.array([True, False, None], type=pa.bool_())),
    (DataType.null(), pa.array([None, None, None], type=pa.null())),
    (DataType.date(), pa.array([datetime.date(2021, 1, 1), datetime.date(2021, 1, 2), None], type=pa.date32())),
    # TODO: [RUST-INT][TYPING] Enable and perform fixes for these types
    # (DataType.binary(), pa.array([b"1", b"2", None], type=pa.binary())),
]

ALL_DATATYPES_BINARY_PAIRS = list(itertools.product(ALL_DTYPES, repeat=2))


@pytest.fixture(
    scope="module",
    params=ALL_DATATYPES_BINARY_PAIRS,
    ids=[f"{dt1}-{dt2}" for (dt1, _), (dt2, _) in ALL_DATATYPES_BINARY_PAIRS],
)
def binary_data_fixture(request) -> tuple[Series, Series]:
    """Returns binary permutation of Series' of all DataType pairs"""
    (dt1, data1), (dt2, data2) = request.param
    s1 = Series.from_arrow(data1, name="lhs")
    assert s1.datatype() == dt1
    s2 = Series.from_arrow(data2, name="rhs")
    assert s2.datatype() == dt2
    return (s1, s2)


@pytest.fixture(
    scope="module",
    params=ALL_DTYPES,
    ids=[f"{dt}" for (dt, _) in ALL_DTYPES],
)
def unary_data_fixture(request) -> tuple[Series, Series]:
    """Returns binary permutation of Series' of all DataType pairs"""
    (dt, data) = request.param
    s = Series.from_arrow(data, name="lhs")
    assert s.datatype() == dt
    return s


class TableLambda(Protocol):
    def __call__(self, table: Table) -> Series:
        ...


def assert_typing_resolve_vs_runtime_behavior(
    table: Table,
    expr: Expression,
    runtime_expr: TableLambda,
    resolvable: bool,
):
    """Asserts that typing behavior during schema resolution matches behavior during runtime on Series'

    Args:
        table: data to test against
        expr (Expression): Expression to run the kernel on the table
        runtime_expr (TableLambda): Function that takes the table as input and runs the kernel manually on the table's Series'
        resolvable (bool): Whether the expression should be resolvable against the table's schema
    """
    projection = ExpressionsProjection([expr.alias("result")])
    if resolvable:
        # Check that schema resolution and Series runtime return the same datatype
        resolved_schema = projection.resolve_schema(table.schema())
        result = runtime_expr(table)
        assert (
            resolved_schema["result"].dtype == result.datatype()
        ), "Should have matching result types at runtime and schema-resolve-time"
    else:
        # Check that we fail to resolve types during schema resolution
        with pytest.raises(ValueError):
            projection.resolve_schema(table.schema())
        # TODO: check that types also fail to resolve at runtime
        # with pytest.raises(ValueError):
        #     runtime_func(table)


def is_numeric(dt: DataType) -> bool:
    """Returns if this datatype is a numeric types that supports arithmetic"""
    return (
        dt == DataType.int8()
        or dt == DataType.int16()
        or dt == DataType.int32()
        or dt == DataType.int64()
        or dt == DataType.uint8()
        or dt == DataType.uint16()
        or dt == DataType.uint32()
        or dt == DataType.uint64()
        or dt == DataType.float32()
        or dt == DataType.float64()
    )


def is_comparable(dt: DataType):
    """Returns if this datatype supports comparisons between elements"""
    return (
        is_numeric(dt)
        or dt == DataType.bool()
        or dt == DataType.string()
        or dt == DataType.null()
        or dt == DataType.date()
    )


def is_same_type_hierarchy(dt1: DataType, dt2: DataType) -> bool:
    """Returns if these two datatypes belong to the same type hierarchy"""
    return (dt1 == dt2) or (is_numeric(dt1) and is_numeric(dt2))
