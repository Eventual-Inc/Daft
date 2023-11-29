from __future__ import annotations

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
    ],
)
def test_partitioning_days(input, dtype, expected):
    s = Series.from_pylist(input).cast(dtype)
    assert s.part.days().to_pylist() == expected


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
    ],
)
def test_partitioning_months(input, dtype, expected):
    s = Series.from_pylist(input).cast(dtype)
    assert s.part.months().to_pylist() == expected


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
    assert s.part.years().to_pylist() == expected


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
    assert s.part.hours().to_pylist() == expected
