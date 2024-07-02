from __future__ import annotations

import datetime
import itertools
from typing import Callable

import pyarrow as pa
import pytest
import pytz

from daft.datatype import DataType
from daft.expressions import Expression, ExpressionsProjection
from daft.series import Series
from daft.table import MicroPartition

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
    (DataType.binary(), pa.array([b"1", b"2", None], type=pa.binary())),
    (DataType.fixed_size_binary(1), pa.array([b"1", b"2", None], type=pa.binary(1))),
]

ALL_DATATYPES_BINARY_PAIRS = list(itertools.product(ALL_DTYPES, repeat=2))


ALL_TEMPORAL_DTYPES = [
    (
        DataType.date(),
        pa.array(
            [datetime.date(2021, 1, 1), datetime.date(2021, 1, 2), None],
            type=pa.date32(),
        ),
    ),
    *[
        (
            DataType.timestamp(unit),
            pa.array(
                [datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 2), None],
                type=pa.timestamp(unit),
            ),
        )
        for unit in ["ns", "us", "ms"]
    ],
    *[
        (
            DataType.timestamp(unit, "US/Eastern"),
            pa.array(
                [
                    datetime.datetime(2021, 1, 1).astimezone(pytz.timezone("US/Eastern")),
                    datetime.datetime(2021, 1, 2).astimezone(pytz.timezone("US/Eastern")),
                    None,
                ],
                type=pa.timestamp(unit, "US/Eastern"),
            ),
        )
        for unit in ["ns", "us", "ms"]
    ],
    *[
        (
            DataType.timestamp(unit, "Africa/Accra"),
            pa.array(
                [
                    datetime.datetime(2021, 1, 1).astimezone(pytz.timezone("Africa/Accra")),
                    datetime.datetime(2021, 1, 2).astimezone(pytz.timezone("Africa/Accra")),
                    None,
                ],
                type=pa.timestamp(unit, "Africa/Accra"),
            ),
        )
        for unit in ["ns", "us", "ms"]
    ],
]

ALL_DTYPES += ALL_TEMPORAL_DTYPES

ALL_TEMPORAL_DATATYPES_BINARY_PAIRS = [
    ((dt1, data1), (dt2, data2))
    for (dt1, data1), (dt2, data2) in itertools.product(ALL_TEMPORAL_DTYPES, repeat=2)
    if not (
        pa.types.is_timestamp(data1.type)
        and pa.types.is_timestamp(data2.type)
        and (data1.type.tz is None) ^ (data2.type.tz is None)
    )
]

ALL_DATATYPES_BINARY_PAIRS += ALL_TEMPORAL_DATATYPES_BINARY_PAIRS


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
def unary_data_fixture(request) -> Series:
    """Returns unary permutation of Series' of all DataType pairs"""
    (dt, data) = request.param
    s = Series.from_arrow(data, name="arg")
    assert s.datatype() == dt
    return s


def assert_typing_resolve_vs_runtime_behavior(
    data: tuple[Series],
    expr: Expression,
    run_kernel: Callable[[], Series],
    resolvable: bool,
):
    """Asserts that typing behavior during schema resolution matches behavior during runtime on Series'

    Example Usage:

        >>> def my_test(binary_data_fixture):
        >>>     lhs, rhs = binary_data_fixture  # unwrap the generated Series data
        >>>     assert_typing_resolve_vs_runtime_behavior(
        >>>         data=binary_data_fixture,
        >>>         expr=col(lhs.name()) + col(rhs.name()),
        >>>         run_kernel=lambda: lhs + rhs,
        >>>         resolvable=can_add_dtypes(lhs.datatype(), rhs.datatype()),
        >>>     )

    Args:
        data: data to test against (generated using one of the provided fixtures, `{unary, binary}_data_fixture`)
        expr (Expression): Expression used to run the kernel in a MicroPartition (use `.name()` of the generated data to refer to columns)
        run_kernel (Callable): A lambda that will run the kernel directly on the generated Series' without going through the Expressions API
        resolvable (bool): Whether this kernel should be valid, given the datatypes of the generated Series'
    """
    table = MicroPartition.from_pydict({s.name(): s for s in data})
    projection = ExpressionsProjection([expr.alias("result")])
    if resolvable:
        # Check that schema resolution and Series runtime return the same datatype
        resolved_schema = projection.resolve_schema(table.schema())
        result = run_kernel()
        assert (
            resolved_schema["result"].dtype == result.datatype()
        ), "Should have matching result types at runtime and schema-resolve-time"
    else:
        # Check that we fail to resolve types during schema resolution
        with pytest.raises(ValueError):
            projection.resolve_schema(table.schema())
        # TODO: check that types also fail to resolve at runtime
        # with pytest.raises(ValueError):
        #     run_kernel()


def is_numeric(dt: DataType) -> bool:
    """Checks if this type is a numeric type"""
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


def is_integer(dt: DataType) -> bool:
    """Checks if this type is an integer type"""
    return (
        dt == DataType.int8()
        or dt == DataType.int16()
        or dt == DataType.int32()
        or dt == DataType.int64()
        or dt == DataType.uint8()
        or dt == DataType.uint16()
        or dt == DataType.uint32()
        or dt == DataType.uint64()
    )


def is_signed_integer(dt: DataType) -> bool:
    """Checks if this type is a signed integer type"""
    return dt == DataType.int8() or dt == DataType.int16() or dt == DataType.int32() or dt == DataType.int64()


def is_comparable(dt: DataType):
    """Checks if this type is a comparable type"""
    return (
        is_numeric(dt)
        or dt == DataType.bool()
        or dt == DataType.string()
        or dt == DataType.null()
        or dt == DataType.binary()
        or dt == DataType.fixed_size_binary(1)
        or dt._is_temporal_type()
    )


def is_numeric_bitwidth_gte_32(dt: DataType):
    """Checks if type is numeric and above a bitwidth of 32"""
    return (
        dt == DataType.int32()
        or dt == DataType.int64()
        or dt == DataType.uint32()
        or dt == DataType.uint64()
        or dt == DataType.float32()
        or dt == DataType.float64()
    )


def has_supertype(dt1: DataType, dt2: DataType) -> bool:
    """Checks if two DataTypes have supertypes - note that this is a simplified
    version of `supertype.rs`, since it only defines "reachability" within the supertype
    tree in a more human-readable way for testing purposes.
    """
    # super(T, T) = T
    if dt1 == dt2:
        return True

    for x, y in ((dt1, dt2), (dt2, dt1)):
        # --- Common types across hierarchies ---
        either_null = x == DataType.null()
        either_string_and_other_not_binary = x == DataType.string() and not (
            y == DataType.binary() or y == DataType.fixed_size_binary(1)
        )

        # --- Within type hierarchies ---
        both_numeric = (is_numeric(x) and is_numeric(y)) or ((x == DataType.bool()) and is_numeric(y))
        both_temporal = x._is_temporal_type() and y._is_temporal_type()

        # --- Across type hierarchies ---
        date_and_numeric = x == DataType.date() and is_numeric(y)
        time_and_numeric = x == (DataType.time("us") or DataType.time("ns")) and is_numeric(y)
        timestamp_and_big_numeric = x._is_temporal_type() and is_numeric_bitwidth_gte_32(y)

        if (
            either_null
            or either_string_and_other_not_binary
            or both_numeric
            or both_temporal
            or date_and_numeric
            or time_and_numeric
            or timestamp_and_big_numeric
        ):
            return True

    return False
