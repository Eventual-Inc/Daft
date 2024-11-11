import datetime

import daft
from daft.sql.sql import SQLCatalog


def test_temporals():
    df = daft.from_pydict(
        {
            "datetimes": [
                datetime.datetime(2021, 1, 1, 23, 59, 58),
                datetime.datetime(2021, 1, 2, 0, 0, 0),
                datetime.datetime(2021, 1, 2, 1, 2, 3),
                datetime.datetime(2021, 1, 2, 1, 2, 3),
                datetime.datetime(1999, 1, 1, 1, 1, 1),
                None,
            ]
        }
    )
    catalog = SQLCatalog({"test": df})

    expected = df.select(
        daft.col("datetimes").dt.date().alias("date"),
        daft.col("datetimes").dt.day().alias("day"),
        daft.col("datetimes").dt.day_of_week().alias("day_of_week"),
        daft.col("datetimes").dt.hour().alias("hour"),
        daft.col("datetimes").dt.minute().alias("minute"),
        daft.col("datetimes").dt.month().alias("month"),
        daft.col("datetimes").dt.second().alias("second"),
        daft.col("datetimes").dt.year().alias("year"),
    ).collect()

    actual = daft.sql(
        """
    SELECT
        date(datetimes) as date,
        day(datetimes) as day,
        dayofweek(datetimes) as day_of_week,
        hour(datetimes) as hour,
        minute(datetimes) as minute,
        month(datetimes) as month,
        second(datetimes) as second,
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
                datetime.datetime(2021, 1, 1, 23, 59, 58),
                datetime.datetime(2021, 1, 2, 0, 0, 0),
                datetime.datetime(2021, 1, 2, 1, 2, 3),
                datetime.datetime(2021, 1, 2, 1, 2, 3),
                datetime.datetime(1999, 1, 1, 1, 1, 1),
                None,
            ]
        }
    )

    expected = df.select(
        daft.col("datetimes").dt.date().alias("date"),
        daft.col("datetimes").dt.day().alias("day"),
        daft.col("datetimes").dt.day_of_week().alias("day_of_week"),
        daft.col("datetimes").dt.hour().alias("hour"),
        daft.col("datetimes").dt.minute().alias("minute"),
        daft.col("datetimes").dt.month().alias("month"),
        daft.col("datetimes").dt.second().alias("second"),
        daft.col("datetimes").dt.year().alias("year"),
    ).collect()

    actual = daft.sql("""
    SELECT
        extract(date from datetimes) as date,
        extract(day from datetimes) as day,
        extract(dayofweek from datetimes) as day_of_week,
        extract(hour from datetimes) as hour,
        extract(minute from datetimes) as minute,
        extract(month from datetimes) as month,
        extract(second from datetimes) as second,
        extract(year from datetimes) as year,
    FROM df
    """).collect()

    assert actual.to_pydict() == expected.to_pydict()
