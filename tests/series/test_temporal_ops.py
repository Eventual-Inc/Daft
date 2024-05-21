from __future__ import annotations

import sys

import pytest

from daft.datatype import DataType, TimeUnit
from daft.series import Series


def test_series_date_day_operation() -> None:
    from datetime import date

    def date_maker(d):
        if d is None:
            return None
        return date(2023, 1, d)

    input = [1, 5, 14, None, 23, None, 28]

    input_dates = list(map(date_maker, input))
    s = Series.from_pylist(input_dates)
    days = s.dt.day()

    assert days.datatype() == DataType.uint32()

    assert input == days.to_pylist()


def test_series_date_month_operation() -> None:
    from datetime import date

    def date_maker(m):
        if m is None:
            return None
        return date(2023, m, 1)

    input = list(range(1, 10)) + [None, 11, 12]

    input_dates = list(map(date_maker, input))
    s = Series.from_pylist(input_dates)
    months = s.dt.month()

    assert months.datatype() == DataType.uint32()

    assert input == months.to_pylist()


def test_series_date_year_operation() -> None:
    from datetime import date

    def date_maker(y):
        if y is None:
            return None
        return date(y, 1, 1)

    input = [1, 1969, 2023 + 5, 2023 + 4, 2023 + 1, None, 2023 + 2, None]

    input_dates = list(map(date_maker, input))
    s = Series.from_pylist(input_dates)
    years = s.dt.year()

    assert years.datatype() == DataType.int32()

    assert input == years.to_pylist()


def test_series_date_day_of_week_operation() -> None:
    from datetime import date

    def date_maker(d):
        if d is None:
            return None
        return date(2023, 4, d)

    # 04/03/2023 is a Monday.
    input = [3, 4, 5, None, 6, 7, None, 8, 9]

    input_dates = list(map(date_maker, input))
    s = Series.from_pylist(input_dates)
    day_of_weeks = s.dt.day_of_week()

    assert day_of_weeks.datatype() == DataType.uint32()

    assert [0, 1, 2, None, 3, 4, None, 5, 6] == day_of_weeks.to_pylist()


@pytest.mark.parametrize("tz", [None, "UTC", "+08:00", "Asia/Singapore"])
def test_series_timestamp_day_operation(tz) -> None:
    from datetime import datetime

    def ts_maker(d):
        if d is None:
            return None
        return datetime(2023, 1, d, 23, 1, 1)

    input = [1, 5, 14, None, 23, None, 28]

    input_ts = list(map(ts_maker, input))
    s = Series.from_pylist(input_ts).cast(DataType.timestamp(TimeUnit.ms(), timezone=tz))
    days = s.dt.day()

    assert days.datatype() == DataType.uint32()

    # If tz is +08:00, then we expect the days to be +1 because our ts_maker makes timestamps with times at 23:00
    expected = [d and d + 1 for d in input] if tz in {"+08:00", "Asia/Singapore"} else input

    assert expected == days.to_pylist()


@pytest.mark.parametrize("tz", [None, "UTC", "+08:00", "Asia/Singapore"])
def test_series_timestamp_hour_operation(tz) -> None:
    from datetime import datetime

    def ts_maker(h):
        if h is None:
            return None
        return datetime(2023, 1, 26, h, 1, 1)

    input = [1, 5, 14, None, 23, None, 21]

    input_ts = list(map(ts_maker, input))
    s = Series.from_pylist(input_ts).cast(DataType.timestamp(TimeUnit.ms(), timezone=tz))
    hours = s.dt.hour()

    assert hours.datatype() == DataType.uint32()

    expected = [h and (h + 8) % 24 for h in input] if tz in {"+08:00", "Asia/Singapore"} else input
    assert expected == hours.to_pylist()


def test_series_time_hour() -> None:
    from datetime import datetime

    def ts_maker(h):
        if h is None:
            return None
        return datetime(2023, 1, 26, h, 1, 1)

    input = [1, 5, 14, None, 23, None, 21]

    input_ts = list(map(ts_maker, input))
    s = Series.from_pylist(input_ts).cast(DataType.time(TimeUnit.ns()))
    hours = s.dt.hour()

    assert hours.datatype() == DataType.uint32()

    assert input == hours.to_pylist()


@pytest.mark.parametrize("tz", [None, "UTC", "+08:00", "Asia/Singapore"])
def test_series_timestamp_minute_operation(tz) -> None:
    from datetime import datetime

    def ts_maker(mi):
        if mi is None:
            return None
        return datetime(2023, 1, 26, 23, mi, 1)

    input = [1, 5, 14, None, 23, None, 21]

    input_ts = list(map(ts_maker, input))
    s = Series.from_pylist(input_ts).cast(DataType.timestamp(TimeUnit.ms(), timezone=tz))
    minutes = s.dt.minute()

    assert minutes.datatype() == DataType.uint32()

    assert input == minutes.to_pylist()


def test_series_time_minute() -> None:
    from datetime import datetime

    def ts_maker(mi):
        if mi is None:
            return None
        return datetime(2023, 1, 26, 23, mi, 1)

    input = [1, 5, 14, None, 23, None, 21]

    input_ts = list(map(ts_maker, input))
    s = Series.from_pylist(input_ts).cast(DataType.time(TimeUnit.ns()))
    minutes = s.dt.minute()

    assert minutes.datatype() == DataType.uint32()

    assert input == minutes.to_pylist()


@pytest.mark.parametrize("tz", [None, "UTC", "+08:00", "Asia/Singapore"])
def test_series_timestamp_second_operation(tz) -> None:
    from datetime import datetime

    def ts_maker(s):
        if s is None:
            return None
        return datetime(2023, 1, 26, 23, 1, s)

    input = [1, 5, 14, None, 23, None, 21]

    input_ts = list(map(ts_maker, input))
    s = Series.from_pylist(input_ts).cast(DataType.timestamp(TimeUnit.ms(), timezone=tz))
    seconds = s.dt.second()

    assert seconds.datatype() == DataType.uint32()

    assert input == seconds.to_pylist()


def test_series_time_second() -> None:
    from datetime import datetime

    def ts_maker(s):
        if s is None:
            return None
        return datetime(2023, 1, 26, 23, 1, s)

    input = [1, 5, 14, None, 23, None, 21]

    input_ts = list(map(ts_maker, input))
    s = Series.from_pylist(input_ts).cast(DataType.time(TimeUnit.ns()))
    seconds = s.dt.second()

    assert seconds.datatype() == DataType.uint32()

    assert input == seconds.to_pylist()


@pytest.mark.parametrize("tz", [None, "UTC", "+08:00", "Asia/Singapore"])
def test_series_timestamp_month_operation(tz) -> None:
    from datetime import datetime

    def ts_maker(m):
        if m is None:
            return None
        return datetime(2023, m, 1, 23, 1, 1)

    input = list(range(1, 10)) + [None, 11, 12]

    input_ts = list(map(ts_maker, input))
    s = Series.from_pylist(input_ts).cast(DataType.timestamp(TimeUnit.ms(), timezone=tz))
    months = s.dt.month()

    assert months.datatype() == DataType.uint32()

    assert input == months.to_pylist()


@pytest.mark.parametrize("tz", [None, "UTC", "+08:00", "Asia/Singapore"])
def test_series_timestamp_year_operation(tz) -> None:
    from datetime import datetime

    def ts_maker(y):
        if y is None:
            return None
        return datetime(y, 1, 1, 23, 1, 1)

    input = [1, 1969, 2023 + 5, 2023 + 4, 2023 + 1, None, 2023 + 2, None]

    input_ts = list(map(ts_maker, input))
    s = Series.from_pylist(input_ts).cast(DataType.timestamp(TimeUnit.ms(), timezone=tz))
    years = s.dt.year()

    assert years.datatype() == DataType.int32()

    assert input == years.to_pylist()


def ts_with_tz_maker(y, m, d, h, mi, s, us, tz):
    from datetime import datetime, timedelta, timezone

    import pytz

    if tz is None:
        return datetime(y, m, d, h, mi, s, us)
    elif tz.startswith("+") or tz.startswith("-"):
        return datetime(y, m, d, h, mi, s, us, timezone(offset=timedelta(hours=int(tz[1:3]))))
    else:
        return datetime(y, m, d, h, mi, s, us, pytz.timezone(tz))


@pytest.mark.skipif(
    sys.version_info < (3, 8),
    reason="Timezone conversions via PyArrow are supported in Python 3.8+",
)
@pytest.mark.parametrize(
    ["input", "interval", "expected"],
    [
        (
            (2024, 1, 2, 3, 4, 5, 6),
            "1 week",
            (2023, 12, 28, 0, 0, 0, 0),
            # 2023/12/12 is 2817 weeks from 1970/01/01, which is what the input should truncate to
        ),
        # yyyy mm dd hh mi ss us
        ((2024, 1, 1, 2, 3, 4, 5), "1 day", (2024, 1, 1, 0, 0, 0, 0)),
        ((2024, 1, 1, 2, 3, 4, 5), "2 days", (2023, 12, 31, 0, 0, 0, 0)),
        ((2024, 1, 1, 1, 2, 3, 4), "1 hour", (2024, 1, 1, 1, 0, 0, 0)),
        ((2024, 1, 1, 1, 2, 3, 4), "6 hours", (2024, 1, 1, 0, 0, 0, 0)),
        ((2024, 1, 1, 1, 1, 2, 3), "1 minute", (2024, 1, 1, 1, 1, 0, 0)),
        ((2024, 1, 1, 1, 1, 2, 3), "30 minutes", (2024, 1, 1, 1, 0, 0, 0)),
        ((2024, 1, 1, 1, 1, 1, 2), "1 second", (2024, 1, 1, 1, 1, 1, 0)),
        ((2024, 1, 1, 1, 1, 1, 2), "30 seconds", (2024, 1, 1, 1, 1, 0, 0)),
        ((2024, 1, 1, 1, 1, 1, 500), "1 millisecond", (2024, 1, 1, 1, 1, 1, 0)),
        ((2024, 1, 1, 1, 1, 1, 500), "500 milliseconds", (2024, 1, 1, 1, 1, 1, 0)),
        ((2024, 1, 1, 1, 1, 1, 1), "1 microsecond", (2024, 1, 1, 1, 1, 1, 1)),
        ((2024, 1, 1, 1, 1, 1, 1), "500 microseconds", (2024, 1, 1, 1, 1, 1, 0)),
    ],
)
@pytest.mark.parametrize("tz", [None, "UTC", "+09:00", "-13:00"])
def test_series_timestamp_truncate_operation(input, interval, expected, tz) -> None:
    input_dt = [ts_with_tz_maker(*input, tz)]
    input_series = Series.from_pylist(input_dt)

    expected_dt = [ts_with_tz_maker(*expected, tz)]
    expected_series = Series.from_pylist(expected_dt)

    truncated = input_series.dt.truncate(interval).to_pylist()
    assert expected_series.to_pylist() == truncated


@pytest.mark.skipif(
    sys.version_info < (3, 8),
    reason="Timezone conversions via PyArrow are supported in Python 3.8+",
)
@pytest.mark.parametrize("tz", [None, "UTC", "+09:00", "-13:00"])
@pytest.mark.parametrize(
    ["input", "interval", "expected", "relative_to"],
    [
        (
            (2024, 1, 2, 3, 4, 5, 6),
            "1 week",
            (2023, 12, 28, 0, 0, 0, 0),
            None,
        ),
        (
            (2024, 1, 2, 3, 4, 5, 6),
            "1 week",
            (2024, 1, 1, 0, 0, 0, 0),
            (2024, 1, 1, 0, 0, 0, 0),
        ),
        (
            (2024, 1, 1, 1, 1, 4, 0),
            "2 seconds",
            (2024, 1, 1, 1, 1, 3, 0),
            (2024, 1, 1, 1, 1, 1, 0),
        ),
    ],
)
def test_series_timestamp_truncate_operation_with_relative_to(tz, input, interval, expected, relative_to) -> None:
    input_dt = [ts_with_tz_maker(*input, tz)]
    input_series = Series.from_pylist(input_dt)

    expected_dt = [ts_with_tz_maker(*expected, tz)]
    expected_series = Series.from_pylist(expected_dt)

    if relative_to is not None:
        relative_to = Series.from_pylist([ts_with_tz_maker(*relative_to, tz)])
    truncated = input_series.dt.truncate(interval, relative_to).to_pylist()
    assert expected_series.to_pylist() == truncated


@pytest.mark.skipif(
    sys.version_info < (3, 8),
    reason="Timezone conversions via PyArrow are supported in Python 3.8+",
)
@pytest.mark.parametrize("tz", [None, "UTC", "+09:00", "-13:00"])
@pytest.mark.parametrize(
    ["input", "interval", "expected", "relative_to"],
    [
        (
            (1969, 12, 31, 12, 0, 0, 0),
            "1 day",
            (1969, 12, 31, 0, 0, 0, 0),
            None,
        ),
        (
            (2023, 12, 31, 23, 0, 0, 0),
            "1 day",
            (2023, 12, 31, 0, 0, 0, 0),
            (2024, 1, 1, 0, 0, 0, 0),
        ),
    ],
)
def test_series_timestamp_truncate_operation_with_times_before_relative_to(
    input, interval, expected, relative_to, tz
) -> None:
    input_dt = [ts_with_tz_maker(*input, tz)]
    input_series = Series.from_pylist(input_dt)

    expected_dt = [ts_with_tz_maker(*expected, tz)]
    expected_series = Series.from_pylist(expected_dt)

    if relative_to is not None:
        relative_to = Series.from_pylist([ts_with_tz_maker(*relative_to, tz)])
    truncated = input_series.dt.truncate(interval, relative_to).to_pylist()
    assert expected_series.to_pylist() == truncated


def test_series_timestamp_truncate_operation_multiple_relative_to() -> None:
    from datetime import datetime

    input_series = Series.from_pylist([datetime(2024, 1, 1, 1, 1, 1, 1)])
    relative_to_series = Series.from_pylist([datetime(2024, 1, 1, 0, 0, 0, 0), datetime(2024, 1, 1, 0, 0, 0, 0)])

    with pytest.raises(ValueError):
        input_series.dt.truncate("1 second", relative_to_series)


@pytest.mark.parametrize("tu", ["s", "ms", "us", "ns"])
def test_series_timestamp_truncate_operation_valid_time_units(tu) -> None:
    from datetime import datetime

    input = [datetime(2024, 1, 2, 3, 4, 5, 6)]
    input_series = Series.from_pylist(input).cast(DataType.timestamp(TimeUnit.from_str(tu)))

    expected = [datetime(2024, 1, 2, 0, 0, 0, 0)]
    expected_series = Series.from_pylist(expected)

    relative_to = Series.from_pylist([datetime(2024, 1, 1, 0, 0, 0, 0)]).cast(DataType.timestamp(TimeUnit.from_str(tu)))

    truncated = input_series.dt.truncate("1 day", relative_to).to_pylist()
    assert expected_series.to_pylist() == truncated


def test_series_timestamp_truncate_operation_different_time_units() -> None:
    from datetime import datetime

    input_series = Series.from_pylist([datetime(2024, 1, 2, 3, 4, 5, 6)]).cast(
        DataType.timestamp(TimeUnit.from_str("s"))
    )

    relative_to = Series.from_pylist([datetime(2024, 1, 1, 0, 0, 0, 0)]).cast(
        DataType.timestamp(TimeUnit.from_str("ms"))
    )

    with pytest.raises(ValueError):
        input_series.dt.truncate("1 day", relative_to)


@pytest.mark.parametrize("interval", ["1 year", "1", "year"])
def test_series_timestamp_truncate_operation_invalid_interval(interval) -> None:
    from datetime import datetime

    input_series = Series.from_pylist([datetime(2024, 1, 1, 1, 1, 1, 1)])

    with pytest.raises(ValueError):
        input_series.dt.truncate(interval)


def test_series_timestamp_truncate_operation_invalid_relative_to() -> None:
    from datetime import datetime

    input_series = Series.from_pylist([datetime(2024, 1, 1, 1, 1, 1, 1)])

    with pytest.raises(ValueError):
        # Start time must be a series of timestamps
        input_series.dt.truncate("1 second", Series.from_pylist([1]))
