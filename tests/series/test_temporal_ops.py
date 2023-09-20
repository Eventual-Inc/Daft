from __future__ import annotations

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
