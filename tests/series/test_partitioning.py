from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from itertools import product

import pytest

from daft import DataType, TimeUnit
from daft.series import Series


@pytest.mark.parametrize(
    "input,dtype,expected",
    [
        ([-1], DataType.date(), [-1]),
        ([-1, None, 17501], DataType.date(), [-1, None, 17501]),
        ([], DataType.date(), []),
        ([None], DataType.date(), [None]),
        ([1512151975038194111], DataType.timestamp(timeunit=TimeUnit.from_str("ns")), [17501]),
        ([1512151975038194], DataType.timestamp(timeunit=TimeUnit.from_str("us")), [17501]),
        ([1512151975038], DataType.timestamp(timeunit=TimeUnit.from_str("ms")), [17501]),
        ([1512151975], DataType.timestamp(timeunit=TimeUnit.from_str("s")), [17501]),
        ([-1], DataType.timestamp(timeunit=TimeUnit.from_str("us")), [-1]),
        ([-1], DataType.timestamp(timeunit=TimeUnit.from_str("us"), timezone="-08:00"), [-1]),
        ([-13 * 3_600_000_000], DataType.timestamp(timeunit=TimeUnit.from_str("us"), timezone="-12:00"), [-1]),
    ],
)
def test_partitioning_days(input, dtype, expected):
    s = Series.from_pylist(input).cast(dtype)
    d = s.partitioning.days()
    assert d.datatype() == DataType.date()
    assert d.cast(DataType.int32()).to_pylist() == expected


@pytest.mark.parametrize(
    "input,dtype,expected",
    [
        ([-1], DataType.date(), [-1]),
        ([-1, 0, -13, None, 17501], DataType.date(), [-1, 0, -1, None, 575]),
        ([], DataType.date(), []),
        ([None], DataType.date(), [None]),
        ([1512151975038194111], DataType.timestamp(timeunit=TimeUnit.from_str("ns")), [575]),
        ([1512151975038194], DataType.timestamp(timeunit=TimeUnit.from_str("us")), [575]),
        ([1512151975038], DataType.timestamp(timeunit=TimeUnit.from_str("ms")), [575]),
        ([1512151975], DataType.timestamp(timeunit=TimeUnit.from_str("s")), [575]),
        ([-1], DataType.timestamp(timeunit=TimeUnit.from_str("us")), [-1]),
        ([-1], DataType.timestamp(timeunit=TimeUnit.from_str("us"), timezone="-08:00"), [-1]),
        (
            [(-24 * 31 + 11) * 3_600_000_000],
            DataType.timestamp(timeunit=TimeUnit.from_str("us"), timezone="-12:00"),
            [-1],
        ),
    ],
)
def test_partitioning_months(input, dtype, expected):
    s = Series.from_pylist(input).cast(dtype)
    m = s.partitioning.months()
    assert m.datatype() == DataType.int32()
    assert m.to_pylist() == expected


@pytest.mark.parametrize(
    "input,dtype,expected",
    [
        ([-1], DataType.date(), [-1]),
        ([-1, 0, -13, None, 17501], DataType.date(), [-1, 0, -1, None, 47]),
        ([], DataType.date(), []),
        ([None], DataType.date(), [None]),
        ([-364, -366, 364, 366], DataType.date(), [-1, -2, 0, 1]),
        ([1512151975038194111], DataType.timestamp(timeunit=TimeUnit.from_str("ns")), [47]),
        ([1512151975038194], DataType.timestamp(timeunit=TimeUnit.from_str("us")), [47]),
        ([1512151975038], DataType.timestamp(timeunit=TimeUnit.from_str("ms")), [47]),
        ([1512151975], DataType.timestamp(timeunit=TimeUnit.from_str("s")), [47]),
        ([-1], DataType.timestamp(timeunit=TimeUnit.from_str("us")), [-1]),
        ([-1], DataType.timestamp(timeunit=TimeUnit.from_str("us"), timezone="-08:00"), [-1]),
    ],
)
def test_partitioning_years(input, dtype, expected):
    s = Series.from_pylist(input).cast(dtype)
    y = s.partitioning.years()
    assert y.datatype() == DataType.int32()
    assert y.to_pylist() == expected


@pytest.mark.parametrize(
    "input,dtype,expected",
    [
        ([1512151975038194111], DataType.timestamp(timeunit=TimeUnit.from_str("ns")), [420042]),
        ([1512151975038194], DataType.timestamp(timeunit=TimeUnit.from_str("us")), [420042]),
        ([1512151975038], DataType.timestamp(timeunit=TimeUnit.from_str("ms")), [420042]),
        ([1512151975], DataType.timestamp(timeunit=TimeUnit.from_str("s")), [420042]),
        ([-1], DataType.timestamp(timeunit=TimeUnit.from_str("us")), [0]),
        ([-1], DataType.timestamp(timeunit=TimeUnit.from_str("us"), timezone="-08:00"), [0]),
        (
            [-3_600_000_000 + 1, -3_600_000_000, 3_600_000_000 + -1, 3_600_000_000 + 1],
            DataType.timestamp(timeunit=TimeUnit.from_str("us"), timezone="-08:00"),
            [0, -1, 0, 1],
        ),
    ],
)
def test_partitioning_hours(input, dtype, expected):
    s = Series.from_pylist(input).cast(dtype)
    h = s.partitioning.hours()
    assert h.datatype() == DataType.int32()
    assert h.to_pylist() == expected


@pytest.mark.parametrize(
    "input,n",
    product(
        [
            ["x", "y", None, "y", "x", None, "x"],
            [1, 2, 3, 2, 1, None],
            [date(1920, 3, 1), date(1920, 3, 1), date(2020, 3, 1)],
            [datetime(1920, 3, 1), datetime(1920, 3, 1), datetime(2020, 3, 1)],
            [Decimal("1420"), Decimal("1420"), Decimal("14.20"), Decimal(".1420"), Decimal(".1420")],
        ],
        [1, 4, 9],
    ),
)
def test_iceberg_bucketing(input, n):
    s = Series.from_pylist(input)
    buckets = s.partitioning.iceberg_bucket(n)
    assert buckets.datatype() == DataType.int32()
    seen = dict()
    for v, b in zip(input, buckets.to_pylist()):
        if v is None:
            assert b is None
        else:
            assert b >= 0
        if v in seen:
            assert seen[v] == b
        else:
            seen[v] = b


def test_iceberg_truncate_decimal():
    data = ["12.34", "12.30", "12.29", "0.05", "-0.05"]
    data = [Decimal(v) for v in data] + [None]
    expected = ["12.30", "12.30", "12.20", "0.00", "-0.10"]
    expected = [Decimal(v) for v in expected] + [None]

    s = Series.from_pylist(data)
    trunc = s.partitioning.iceberg_truncate(10)
    assert trunc.datatype() == s.datatype()
    assert trunc.to_pylist() == expected


@pytest.mark.parametrize(
    "dtype",
    [
        DataType.int8(),
        DataType.int16(),
        DataType.int32(),
        DataType.int64(),
    ],
)
def test_iceberg_truncate_signed_int(dtype):
    data = [0, 1, 5, 9, 10, 11, -1, -5, -10, -11, None]
    expected = [0, 0, 0, 0, 10, 10, -10, -10, -10, -20, None]

    s = Series.from_pylist(data).cast(dtype)
    trunc = s.partitioning.iceberg_truncate(10)
    assert trunc.datatype() == s.datatype()
    assert trunc.to_pylist() == expected


@pytest.mark.parametrize(
    "dtype",
    [
        DataType.uint8(),
        DataType.uint16(),
        DataType.uint32(),
        DataType.uint64(),
        DataType.int8(),
        DataType.int16(),
        DataType.int32(),
        DataType.int64(),
    ],
)
def test_iceberg_truncate_all_int(dtype):
    data = [0, 1, 5, 9, 10, 11, None]
    expected = [0, 0, 0, 0, 10, 10, None]

    s = Series.from_pylist(data).cast(dtype)
    trunc = s.partitioning.iceberg_truncate(10)
    assert trunc.datatype() == s.datatype()
    assert trunc.to_pylist() == expected


def test_iceberg_truncate_str():
    data = ["abcdefg", "abc", "abcde", None]
    expected = ["abcde", "abc", "abcde", None]
    s = Series.from_pylist(data)
    trunc = s.partitioning.iceberg_truncate(5)
    assert trunc.datatype() == s.datatype()
    assert trunc.to_pylist() == expected
