import datetime

import daft
from daft.sql.sql import SQLCatalog


def test_temporals():
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
    catalog = SQLCatalog({"test": df})

    expected = df.select(
        daft.col("datetimes").dt.date().alias("date"),
        daft.col("datetimes").dt.day().alias("day"),
        daft.col("datetimes").dt.day_of_week().alias("day_of_week"),
        daft.col("datetimes").dt.day_of_year().alias("day_of_year"),
        daft.col("datetimes").dt.hour().alias("hour"),
        daft.col("datetimes").dt.minute().alias("minute"),
        daft.col("datetimes").dt.month().alias("month"),
        daft.col("datetimes").dt.second().alias("second"),
        daft.col("datetimes").dt.millisecond().alias("millisecond"),
        daft.col("datetimes").dt.microsecond().alias("microsecond"),
        daft.col("datetimes").dt.nanosecond().alias("nanosecond"),
        daft.col("datetimes").dt.year().alias("year"),
    ).collect()

    actual = daft.sql(
        """
    SELECT
        date(datetimes) as date,
        day(datetimes) as day,
        day_of_week(datetimes) as day_of_week,
        day_of_year(datetimes) as day_of_year,
        hour(datetimes) as hour,
        minute(datetimes) as minute,
        month(datetimes) as month,
        second(datetimes) as second,
        millisecond(datetimes) as millisecond,
        microsecond(datetimes) as microsecond,
        nanosecond(datetimes) as nanosecond,
        year(datetimes) as year,
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
        daft.col("datetimes").dt.day_of_year().alias("day_of_year"),
        daft.col("datetimes").dt.hour().alias("hour"),
        daft.col("datetimes").dt.minute().alias("minute"),
        daft.col("datetimes").dt.month().alias("month"),
        daft.col("datetimes").dt.second().alias("second"),
        daft.col("datetimes").dt.millisecond().alias("millisecond"),
        daft.col("datetimes").dt.microsecond().alias("microsecond"),
        daft.col("datetimes").dt.nanosecond().alias("nanosecond"),
        daft.col("datetimes").dt.year().alias("year"),
    ).collect()

    actual = daft.sql("""
    SELECT
        extract(date from datetimes) as date,
        extract(day from datetimes) as day,
        extract(dayofweek from datetimes) as day_of_week,
        extract(dayofyear from datetimes) as day_of_year,
        extract(hour from datetimes) as hour,
        extract(minute from datetimes) as minute,
        extract(month from datetimes) as month,
        extract(second from datetimes) as second,
        extract(millisecond from datetimes) as millisecond,
        extract(microsecond from datetimes) as microsecond,
        extract(nanosecond from datetimes) as nanosecond,
        extract(year from datetimes) as year,
    FROM df
    """).collect()

    assert actual.to_pydict() == expected.to_pydict()


def test_date_comparison():
    date_df = daft.from_pydict({"date_str": ["2020-01-01", "2020-01-02", "2020-01-03"]})
    date_df = date_df.with_column("date", daft.col("date_str").str.to_date("%Y-%m-%d"))
    expected = date_df.filter(daft.col("date") == "2020-01-01").select("date").to_pydict()
    actual = daft.sql("select date from date_df where date == '2020-01-01'").to_pydict()
    assert actual == expected
