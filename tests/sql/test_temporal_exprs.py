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
        daft.col("datetimes").date().alias("date"),
        daft.col("datetimes").day().alias("day"),
        daft.col("datetimes").day_of_week().alias("day_of_week"),
        daft.col("datetimes").day_of_month().alias("day_of_month"),
        daft.col("datetimes").day_of_year().alias("day_of_year"),
        daft.col("datetimes").week_of_year().alias("week_of_year"),
        daft.col("datetimes").hour().alias("hour"),
        daft.col("datetimes").minute().alias("minute"),
        daft.col("datetimes").month().alias("month"),
        daft.col("datetimes").second().alias("second"),
        daft.col("datetimes").millisecond().alias("millisecond"),
        daft.col("datetimes").microsecond().alias("microsecond"),
        daft.col("datetimes").nanosecond().alias("nanosecond"),
        daft.col("datetimes").quarter().alias("quarter"),
        daft.col("datetimes").year().alias("year"),
        daft.col("datetimes").unix_date().alias("unix_date"),
        daft.col("datetimes").to_unix_epoch().alias("to_unix_epoch"),
        daft.col("datetimes").to_unix_epoch("s").alias("to_unix_epoch_s"),
        daft.col("datetimes").to_unix_epoch("ms").alias("to_unix_epoch_ms"),
        daft.col("datetimes").to_unix_epoch("us").alias("to_unix_epoch_us"),
        daft.col("datetimes").to_unix_epoch("ns").alias("to_unix_epoch_ns"),
        daft.col("datetimes").to_unix_epoch("seconds").alias("to_unix_epoch_seconds"),
        daft.col("datetimes").to_unix_epoch("milliseconds").alias("to_unix_epoch_milliseconds"),
        daft.col("datetimes").to_unix_epoch("microseconds").alias("to_unix_epoch_microseconds"),
        daft.col("datetimes").to_unix_epoch("nanoseconds").alias("to_unix_epoch_nanoseconds"),
        daft.col("datetimes").strftime().alias("date_str"),
        daft.col("times").strftime().alias("time_str"),
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
        daft.col("datetimes").date().alias("date"),
        daft.col("datetimes").day().alias("day"),
        daft.col("datetimes").day_of_week().alias("day_of_week"),
        daft.col("datetimes").day_of_month().alias("day_of_month"),
        daft.col("datetimes").day_of_year().alias("day_of_year"),
        daft.col("datetimes").week_of_year().alias("week_of_year"),
        daft.col("datetimes").hour().alias("hour"),
        daft.col("datetimes").minute().alias("minute"),
        daft.col("datetimes").month().alias("month"),
        daft.col("datetimes").second().alias("second"),
        daft.col("datetimes").millisecond().alias("millisecond"),
        daft.col("datetimes").microsecond().alias("microsecond"),
        daft.col("datetimes").nanosecond().alias("nanosecond"),
        daft.col("datetimes").quarter().alias("quarter"),
        daft.col("datetimes").year().alias("year"),
        daft.col("datetimes").unix_date().alias("unix_date"),
    ).collect()

    actual = daft.sql(
        """
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
    """
    ).collect()

    assert actual.to_pydict() == expected.to_pydict()


def test_date_trunc():
    from daft.functions import date_trunc

    df = daft.from_pydict({"ts": ["2024-01-15T10:30:45", "2024-06-20T14:15:30", "2024-12-31T23:59:59"]})
    df = df.with_column("ts", daft.col("ts").cast(daft.DataType.timestamp("s")))

    # Test bare unit name (standard SQL style)
    expected = df.select(date_trunc("1 minute", daft.col("ts")).alias("truncated")).collect()
    actual = daft.sql("SELECT DATE_TRUNC('minute', ts) AS truncated FROM df").collect()
    assert actual.to_pydict() == expected.to_pydict()

    # Test with explicit count (Daft style)
    expected = df.select(date_trunc("1 hour", daft.col("ts")).alias("truncated")).collect()
    actual = daft.sql("SELECT DATE_TRUNC('1 hour', ts) AS truncated FROM df").collect()
    assert actual.to_pydict() == expected.to_pydict()

    # Test day truncation
    expected = df.select(date_trunc("1 day", daft.col("ts")).alias("truncated")).collect()
    actual = daft.sql("SELECT DATE_TRUNC('day', ts) AS truncated FROM df").collect()
    assert actual.to_pydict() == expected.to_pydict()

    # Test case insensitivity of function name
    actual = daft.sql("SELECT date_trunc('day', ts) AS truncated FROM df").collect()
    assert actual.to_pydict() == expected.to_pydict()

    # Test 'truncate' alias
    actual = daft.sql("SELECT truncate('day', ts) AS truncated FROM df").collect()
    assert actual.to_pydict() == expected.to_pydict()

    # Test 3-argument form with relative_to
    ref_df = df.with_column("ref", daft.lit("2024-01-01T00:00:00").cast(daft.DataType.timestamp("s")))
    expected = ref_df.select(
        date_trunc("1 hour", daft.col("ts"), relative_to=daft.col("ref")).alias("truncated")
    ).collect()
    actual = daft.sql("SELECT DATE_TRUNC('hour', ts, ref) AS truncated FROM ref_df").collect()
    assert actual.to_pydict() == expected.to_pydict()


def test_date_comparison():
    date_df = daft.from_pydict({"date_str": ["2020-01-01", "2020-01-02", "2020-01-03"]})
    date_df = date_df.with_column("date", daft.col("date_str").to_date("%Y-%m-%d"))
    expected = date_df.filter(daft.col("date") == "2020-01-01").select("date").to_pydict()
    actual = daft.sql("select date from date_df where date == '2020-01-01'").to_pydict()
    assert actual == expected
