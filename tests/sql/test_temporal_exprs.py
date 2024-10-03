import datetime

import daft
from daft import col
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
    print(df)

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


def test_interval():
    df = daft.from_pydict(
        {
            "date": ["2021-01-01", "2021-01-02", "2021-01-03"],
            "ts": ["2021-01-01 01:28:40", "2021-01-02 12:12:12", "2011-11-11 11:11:11"],
        }
    ).select(daft.col("date").cast(daft.DataType.date()), daft.col("ts").str.to_datetime("%Y-%m-%d %H:%M:%S"))

    def interval(unit):
        if unit == "year":
            td = datetime.timedelta(days=365)
        else:
            td = datetime.timedelta(**{unit: 1})

        total_microseconds = int(td.total_seconds() * 1_000_000)
        return daft.lit(total_microseconds).cast(daft.DataType.duration("us"))

    actual = (
        daft.sql("""
        SELECT
            date + interval '1' day as date_add_day,
            date + interval '1' week as date_add_week,
            date + interval '1' year as date_add_year,
            date - interval '1' day as date_sub_day,
            date - interval '1' week as date_sub_week,
            date - interval '1' year as date_sub_year,

            ts + interval '1' millisecond as ts_add_millisecond,
            ts + interval '1' second as ts_add_second,
            ts + interval '1' minute as ts_add_minute,
            ts + interval '1' hour as ts_add_hour,
            ts + interval '1' day as ts_add_day ,
            ts + interval '1' week as ts_add_week,
            ts + interval '1' year as ts_add_year,

            ts - interval '1' millisecond as ts_sub_millisecond,
            ts - interval '1' second as ts_sub_second,
            ts - interval '1' minute as ts_sub_minute,
            ts - interval '1' hour as ts_sub_hour,
            ts - interval '1' day as ts_sub_day,
            ts - interval '1' week as ts_sub_week,
            ts - interval '1' year as ts_sub_year,
        FROM df
    """)
        .collect()
        .to_pydict()
    )

    expected = (
        df.select(
            (col("date") + interval("days")).alias("date_add_day"),
            (col("date") + interval("weeks")).alias("date_add_week"),
            (col("date") + interval("year")).alias("date_add_year"),
            (col("date") - interval("days")).alias("date_sub_day"),
            (col("date") - interval("weeks")).alias("date_sub_week"),
            (col("date") - interval("year")).alias("date_sub_year"),
            (col("ts") + interval("milliseconds")).alias("ts_add_millisecond"),
            (col("ts") + interval("seconds")).alias("ts_add_second"),
            (col("ts") + interval("minutes")).alias("ts_add_minute"),
            (col("ts") + interval("hours")).alias("ts_add_hour"),
            (col("ts") + interval("days")).alias("ts_add_day"),
            (col("ts") + interval("weeks")).alias("ts_add_week"),
            (col("ts") + interval("year")).alias("ts_add_year"),
            (col("ts") - interval("milliseconds")).alias("ts_sub_millisecond"),
            (col("ts") - interval("seconds")).alias("ts_sub_second"),
            (col("ts") - interval("minutes")).alias("ts_sub_minute"),
            (col("ts") - interval("hours")).alias("ts_sub_hour"),
            (col("ts") - interval("days")).alias("ts_sub_day"),
            (col("ts") - interval("weeks")).alias("ts_sub_week"),
            (col("ts") - interval("year")).alias("ts_sub_year"),
        )
        .collect()
        .to_pydict()
    )

    assert actual == expected
