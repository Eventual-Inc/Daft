from __future__ import annotations

import datetime

import daft


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
    bindings = {"test": df}

    expected = df.select(
        daft.functions.date(daft.col("datetimes")).alias("date"),
        daft.functions.day(daft.col("datetimes")).alias("day"),
        daft.functions.day_of_week(daft.col("datetimes")).alias("day_of_week"),
        daft.functions.day_of_month(daft.col("datetimes")).alias("day_of_month"),
        daft.functions.day_of_year(daft.col("datetimes")).alias("day_of_year"),
        daft.functions.week_of_year(daft.col("datetimes")).alias("week_of_year"),
        daft.functions.hour(daft.col("datetimes")).alias("hour"),
        daft.functions.minute(daft.col("datetimes")).alias("minute"),
        daft.functions.month(daft.col("datetimes")).alias("month"),
        daft.functions.second(daft.col("datetimes")).alias("second"),
        daft.functions.millisecond(daft.col("datetimes")).alias("millisecond"),
        daft.functions.microsecond(daft.col("datetimes")).alias("microsecond"),
        daft.functions.nanosecond(daft.col("datetimes")).alias("nanosecond"),
        daft.functions.quarter(daft.col("datetimes")).alias("quarter"),
        daft.functions.year(daft.col("datetimes")).alias("year"),
        daft.functions.unix_date(daft.col("datetimes")).alias("unix_date"),
        daft.functions.to_unix_epoch(daft.col("datetimes")).alias("to_unix_epoch"),
        daft.functions.to_unix_epoch(daft.col("datetimes"), "s").alias("to_unix_epoch_s"),
        daft.functions.to_unix_epoch(daft.col("datetimes"), "ms").alias("to_unix_epoch_ms"),
        daft.functions.to_unix_epoch(daft.col("datetimes"), "us").alias("to_unix_epoch_us"),
        daft.functions.to_unix_epoch(daft.col("datetimes"), "ns").alias("to_unix_epoch_ns"),
        daft.functions.to_unix_epoch(daft.col("datetimes"), "seconds").alias("to_unix_epoch_seconds"),
        daft.functions.to_unix_epoch(daft.col("datetimes"), "milliseconds").alias("to_unix_epoch_milliseconds"),
        daft.functions.to_unix_epoch(daft.col("datetimes"), "microseconds").alias("to_unix_epoch_microseconds"),
        daft.functions.to_unix_epoch(daft.col("datetimes"), "nanoseconds").alias("to_unix_epoch_nanoseconds"),
        daft.functions.strftime(daft.col("datetimes")).alias("date_str"),
        daft.functions.strftime(daft.col("times")).alias("time_str"),
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
        **bindings,
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
        daft.functions.date(daft.col("datetimes")).alias("date"),
        daft.functions.day(daft.col("datetimes")).alias("day"),
        daft.functions.day_of_week(daft.col("datetimes")).alias("day_of_week"),
        daft.functions.day_of_month(daft.col("datetimes")).alias("day_of_month"),
        daft.functions.day_of_year(daft.col("datetimes")).alias("day_of_year"),
        daft.functions.week_of_year(daft.col("datetimes")).alias("week_of_year"),
        daft.functions.hour(daft.col("datetimes")).alias("hour"),
        daft.functions.minute(daft.col("datetimes")).alias("minute"),
        daft.functions.month(daft.col("datetimes")).alias("month"),
        daft.functions.second(daft.col("datetimes")).alias("second"),
        daft.functions.millisecond(daft.col("datetimes")).alias("millisecond"),
        daft.functions.microsecond(daft.col("datetimes")).alias("microsecond"),
        daft.functions.nanosecond(daft.col("datetimes")).alias("nanosecond"),
        daft.functions.quarter(daft.col("datetimes")).alias("quarter"),
        daft.functions.year(daft.col("datetimes")).alias("year"),
        daft.functions.unix_date(daft.col("datetimes")).alias("unix_date"),
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
    date_df = date_df.with_column("date", daft.functions.to_date(daft.col("date_str"), "%Y-%m-%d"))
    expected = date_df.filter(daft.col("date") == "2020-01-01").select("date").to_pydict()
    actual = daft.sql("select date from date_df where date == '2020-01-01'").to_pydict()
    assert actual == expected
