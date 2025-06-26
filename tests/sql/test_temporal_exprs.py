from __future__ import annotations

import datetime

import daft
from daft.sql.sql import SQLCatalog


def test_temporals():
    df = daft.from_pydict(
        {
            "times": [
                datetime.time(1, 2, 3, 4),
                datetime.time(2, 3, 4, 5),
                datetime.time(3, 4, 5, 6),
                datetime.time(4, 5, 6, 7),
                datetime.time(5, 6, 7, 8),
                None,
            ],
            "datetimes": [
                datetime.datetime(2021, 1, 1, 23, 59, 58, 999_999),
                datetime.datetime(2021, 1, 2, 0, 0, 0, 0),
                datetime.datetime(2021, 1, 2, 1, 2, 3, 500_000),
                datetime.datetime(2021, 2, 2, 1, 2, 3, 100_000),
                datetime.datetime(1999, 1, 31, 1, 1, 1, 50),
                None,
            ],
        }
    )
    catalog = SQLCatalog({"test": df})

    expected = df.select(
        daft.col("datetimes").dt.date().alias("date"),
        daft.col("datetimes").dt.day().alias("day"),
        daft.col("datetimes").dt.day_of_week().alias("day_of_week"),
        daft.col("datetimes").dt.day_of_month().alias("day_of_month"),
        daft.col("datetimes").dt.day_of_year().alias("day_of_year"),
        daft.col("datetimes").dt.week_of_year().alias("week_of_year"),
        daft.col("datetimes").dt.hour().alias("hour"),
        daft.col("datetimes").dt.minute().alias("minute"),
        daft.col("datetimes").dt.month().alias("month"),
        daft.col("datetimes").dt.second().alias("second"),
        daft.col("datetimes").dt.millisecond().alias("millisecond"),
        daft.col("datetimes").dt.microsecond().alias("microsecond"),
        daft.col("datetimes").dt.nanosecond().alias("nanosecond"),
        daft.col("datetimes").dt.quarter().alias("quarter"),
        daft.col("datetimes").dt.year().alias("year"),
        daft.col("datetimes").dt.unix_date().alias("unix_date"),
        daft.col("datetimes").dt.to_unix_epoch().alias("to_unix_epoch"),
        daft.col("datetimes").dt.to_unix_epoch("s").alias("to_unix_epoch_s"),
        daft.col("datetimes").dt.to_unix_epoch("ms").alias("to_unix_epoch_ms"),
        daft.col("datetimes").dt.to_unix_epoch("us").alias("to_unix_epoch_us"),
        daft.col("datetimes").dt.to_unix_epoch("ns").alias("to_unix_epoch_ns"),
        daft.col("datetimes").dt.to_unix_epoch("seconds").alias("to_unix_epoch_seconds"),
        daft.col("datetimes").dt.to_unix_epoch("milliseconds").alias("to_unix_epoch_milliseconds"),
        daft.col("datetimes").dt.to_unix_epoch("microseconds").alias("to_unix_epoch_microseconds"),
        daft.col("datetimes").dt.to_unix_epoch("nanoseconds").alias("to_unix_epoch_nanoseconds"),
        daft.col("datetimes").dt.strftime().alias("date_str"),
        daft.col("times").dt.strftime().alias("time_str"),
    ).collect()

    actual = daft.sql(
        """
    SELECT
        date(datetimes) as date,
        day(datetimes) as day,
        day_of_week(datetimes) as day_of_week,
        day_of_month(datetimes) as day_of_month,
        day_of_year(datetimes) as day_of_year,
        week_of_year(datetimes) as week_of_year,
        hour(datetimes) as hour,
        minute(datetimes) as minute,
        month(datetimes) as month,
        second(datetimes) as second,
        millisecond(datetimes) as millisecond,
        microsecond(datetimes) as microsecond,
        nanosecond(datetimes) as nanosecond,
        quarter(datetimes) as quarter,
        year(datetimes) as year,
        unix_date(datetimes) as unix_date,
        to_unix_epoch(datetimes) as to_unix_epoch,
        to_unix_epoch(datetimes, 's') as to_unix_epoch_s,
        to_unix_epoch(datetimes, 'ms') as to_unix_epoch_ms,
        to_unix_epoch(datetimes, 'us') as to_unix_epoch_us,
        to_unix_epoch(datetimes, 'ns') as to_unix_epoch_ns,
        to_unix_epoch(datetimes, 's') as to_unix_epoch_seconds,
        to_unix_epoch(datetimes, 'ms') as to_unix_epoch_milliseconds,
        to_unix_epoch(datetimes, 'us') as to_unix_epoch_microseconds,
        to_unix_epoch(datetimes, 'ns') as to_unix_epoch_nanoseconds,
        strftime(datetimes) as date_str,
        strftime(times) as time_str,
    FROM test
    """,
        catalog=catalog,
    ).collect()

    assert actual.to_pydict() == expected.to_pydict()


def test_extract():
    df = daft.from_pydict(
        {
            "datetimes": [
                datetime.datetime(2021, 1, 1, 23, 59, 58, 999_999),
                datetime.datetime(2021, 1, 2, 0, 0, 0, 0),
                datetime.datetime(2021, 1, 2, 1, 2, 3, 500_000),
                datetime.datetime(2021, 2, 2, 1, 2, 3, 100_000),
                datetime.datetime(1999, 1, 31, 1, 1, 1, 50),
                None,
            ]
        }
    )

    expected = df.select(
        daft.col("datetimes").dt.date().alias("date"),
        daft.col("datetimes").dt.day().alias("day"),
        daft.col("datetimes").dt.day_of_week().alias("day_of_week"),
        daft.col("datetimes").dt.day_of_month().alias("day_of_month"),
        daft.col("datetimes").dt.day_of_year().alias("day_of_year"),
        daft.col("datetimes").dt.week_of_year().alias("week_of_year"),
        daft.col("datetimes").dt.hour().alias("hour"),
        daft.col("datetimes").dt.minute().alias("minute"),
        daft.col("datetimes").dt.month().alias("month"),
        daft.col("datetimes").dt.second().alias("second"),
        daft.col("datetimes").dt.millisecond().alias("millisecond"),
        daft.col("datetimes").dt.microsecond().alias("microsecond"),
        daft.col("datetimes").dt.nanosecond().alias("nanosecond"),
        daft.col("datetimes").dt.quarter().alias("quarter"),
        daft.col("datetimes").dt.year().alias("year"),
        daft.col("datetimes").dt.unix_date().alias("unix_date"),
    ).collect()

    actual = daft.sql("""
    SELECT
        extract(date from datetimes) as date,
        extract(day from datetimes) as day,
        extract(day_of_week from datetimes) as day_of_week,
        extract(day_of_month from datetimes) as day_of_month,
        extract(day_of_year from datetimes) as day_of_year,
        extract(week_of_year from datetimes) as week_of_year,
        extract(hour from datetimes) as hour,
        extract(minute from datetimes) as minute,
        extract(month from datetimes) as month,
        extract(second from datetimes) as second,
        extract(millisecond from datetimes) as millisecond,
        extract(microsecond from datetimes) as microsecond,
        extract(nanosecond from datetimes) as nanosecond,
        extract(quarter from datetimes) as quarter,
        extract(year from datetimes) as year,
        extract(unix_date from datetimes) as unix_date,
    FROM df
    """).collect()

    assert actual.to_pydict() == expected.to_pydict()


def test_date_comparison():
    date_df = daft.from_pydict({"date_str": ["2020-01-01", "2020-01-02", "2020-01-03"]})
    date_df = date_df.with_column("date", daft.col("date_str").str.to_date("%Y-%m-%d"))
    expected = date_df.filter(daft.col("date") == "2020-01-01").select("date").to_pydict()
    actual = daft.sql("select date from date_df where date == '2020-01-01'").to_pydict()
    assert actual == expected
