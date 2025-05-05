from __future__ import annotations

import itertools
import tempfile
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pytest
import pytz

import daft
from daft import DataType, col

PYARROW_GE_7_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (7, 0, 0)


def test_temporal_arithmetic_with_same_type() -> None:
    now = datetime.now()
    now_tz = datetime.now(timezone.utc)
    df = daft.from_pydict(
        {
            "dt_us": [datetime.min, now],
            "dt_us_tz": [datetime.min.replace(tzinfo=timezone.utc), now_tz],
            "date": [datetime.min.date(), now.date()],
            "duration": [timedelta(days=1), timedelta(microseconds=1)],
        }
    )

    df = df.select(
        (df["dt_us"] - df["dt_us"]).alias("zero1"),
        (df["dt_us_tz"] - df["dt_us_tz"]).alias("zero2"),
        (df["date"] - df["date"]).alias("zero3"),
        (df["duration"] + df["duration"]).alias("add_dur"),
        (df["duration"] - df["duration"]).alias("sub_dur"),
    )

    result = df.to_pydict()
    assert result["zero1"] == [timedelta(0), timedelta(0)]
    assert result["zero2"] == [timedelta(0), timedelta(0)]
    assert result["zero3"] == [timedelta(0), timedelta(0)]
    assert result["add_dur"] == [timedelta(days=2), timedelta(microseconds=2)]
    assert result["sub_dur"] == [timedelta(0), timedelta(0)]


@pytest.mark.parametrize("format", ["csv", "parquet"])
def test_temporal_file_roundtrip(format) -> None:
    data = {
        "date32": pa.array([1], pa.date32()),
        "date64": pa.array([1], pa.date64()),
        # Not supported by pyarrow CSV reader yet.
        # "time32_s": pa.array([1], pa.time32("s")),
        # "time32_ms": pa.array([1], pa.time32("ms")),
        # "time64_us": pa.array([1], pa.time64("us")),
        # "time64_ns": pa.array([1], pa.time64("ns")),
        # "duration_s": pa.array([1], pa.duration("s")),
        # "duration_ms": pa.array([1], pa.duration("ms")),
        # "duration_us": pa.array([1], pa.duration("us")),
        # "duration_ns": pa.array([1], pa.duration("ns")),
        # Nanosecond resolution not yet supported (since we currently use Python temporal objects).
        # "timestamp_ns": pa.array([1], pa.timestamp("ns")),
        # "timestamp_ns_tz": pa.array([1], pa.timestamp("ns", tz='UTC')),
        # pyarrow doesn't support writing interval type.
        # "interval": pa.array([pa.scalar((1, 1, 1), type=pa.month_day_nano_interval()).as_py()]),
    }

    # CSV writing of these files only supported by pyarrow CSV writer in PyArrow >= 7.0.0
    if format == "csv" and PYARROW_GE_7_0_0:
        data = {
            **data,
            "timestamp_s": pa.array([1], pa.timestamp("s")),
            "timestamp_ms": pa.array([1], pa.timestamp("ms")),
            "timestamp_us": pa.array([1], pa.timestamp("us")),
            "timestamp_s_utc_tz": pa.array([1], pa.timestamp("s", tz="UTC")),
            "timestamp_ms_utc_tz": pa.array([1], pa.timestamp("ms", tz="UTC")),
            "timestamp_us_utc_tz": pa.array([1], pa.timestamp("us", tz="UTC")),
            "timestamp_s_tz": pa.array([1], pa.timestamp("s", tz="Asia/Singapore")),
            "timestamp_ms_tz": pa.array([1], pa.timestamp("ms", tz="Asia/Singapore")),
            "timestamp_us_tz": pa.array([1], pa.timestamp("us", tz="Asia/Singapore")),
        }

    pa_table = pa.Table.from_pydict(data)

    df = daft.from_arrow(pa_table)

    with tempfile.TemporaryDirectory() as dirname:
        if format == "csv":
            df.write_csv(dirname)
            df_readback = daft.read_csv(dirname).collect()
        elif format == "parquet":
            df.write_parquet(dirname)
            df_readback = daft.read_parquet(dirname).collect()

        assert df.to_pydict() == df_readback.to_pydict()


@pytest.mark.parametrize(
    "timeunit",
    ["s", "ms", "us", "ns"],
)
@pytest.mark.parametrize(
    "timezone",
    [None, "UTC", "America/Los_Angeles", "+04:00"],
)
def test_arrow_timestamp(timeunit, timezone) -> None:
    # Test roundtrip of Arrow timestamps.
    pa_table = pa.Table.from_pydict({"timestamp": pa.array([1, 0, -1], pa.timestamp(timeunit, tz=timezone))})

    df = daft.from_arrow(pa_table)

    assert df.to_arrow() == pa_table


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="PyArrow conversion of timezoned datetime is broken in 6.0.1",
)
@pytest.mark.parametrize("timezone", [None, timezone.utc, timezone(timedelta(hours=-7))])
def test_python_timestamp(timezone) -> None:
    # Test roundtrip of Python timestamps.
    timestamp = datetime.now(timezone)
    df = daft.from_pydict({"timestamp": [timestamp]})

    res = df.to_pydict()["timestamp"][0]
    assert res.isoformat() == timestamp.isoformat()


@pytest.mark.parametrize(
    "timeunit",
    ["s", "ms", "us", "ns"],
)
def test_arrow_duration(timeunit) -> None:
    # Test roundtrip of Arrow timestamps.
    pa_table = pa.Table.from_pydict({"duration": pa.array([1, 0, -1], pa.duration(timeunit))})

    df = daft.from_arrow(pa_table)

    assert df.to_arrow() == pa_table


def test_python_duration() -> None:
    # Test roundtrip of Python durations.
    duration = timedelta(weeks=1, days=1, hours=1, minutes=1, seconds=1, milliseconds=1, microseconds=1)
    df = daft.from_pydict({"duration": [duration]})

    res = df.to_pydict()["duration"][0]
    assert res == duration


def test_temporal_arithmetic_with_duration_lit() -> None:
    df = daft.from_pydict(
        {
            "duration": [timedelta(days=1)],
            "date": [datetime(2021, 1, 1)],
            "timestamp": [datetime(2021, 1, 1)],
        }
    )

    df = df.select(
        (df["date"] + timedelta(days=1)).alias("add_date"),
        (df["date"] - timedelta(days=1)).alias("sub_date"),
        (df["timestamp"] + timedelta(days=1)).alias("add_timestamp"),
        (df["timestamp"] - timedelta(days=1)).alias("sub_timestamp"),
        (df["duration"] + timedelta(days=1)).alias("add_dur"),
        (df["duration"] - timedelta(days=1)).alias("sub_dur"),
    )

    result = df.to_pydict()
    assert result["add_date"] == [datetime(2021, 1, 2)]
    assert result["sub_date"] == [datetime(2020, 12, 31)]
    assert result["add_timestamp"] == [datetime(2021, 1, 2)]
    assert result["sub_timestamp"] == [datetime(2020, 12, 31)]
    assert result["add_dur"] == [timedelta(days=2)]
    assert result["sub_dur"] == [timedelta(0)]


@pytest.mark.parametrize(
    "timeunit",
    ["s", "ms", "us", "ns"],
)
@pytest.mark.parametrize(
    "timezone",
    [None, "UTC"],
)
def test_temporal_arithmetic_timestamp_with_duration(timeunit, timezone) -> None:
    pa_table = pa.Table.from_pydict(
        {
            "timestamp": pa.array([1, 0, -1], pa.timestamp(timeunit, timezone)),
            "duration": pa.array([1, 0, -1], pa.duration(timeunit)),
        }
    )
    df = daft.from_arrow(pa_table)

    df = df.select(
        (df["timestamp"] + df["duration"]).alias("ladd"),
        (df["duration"] + df["timestamp"]).alias("radd"),
        (df["timestamp"] - df["duration"]).alias("sub"),
    ).collect()

    # Check that the result dtypes are correct.
    expected_daft_dtype = daft.DataType.timestamp(daft.TimeUnit.from_str(timeunit), timezone)
    assert df.schema()["ladd"].dtype == expected_daft_dtype
    assert df.schema()["radd"].dtype == expected_daft_dtype
    assert df.schema()["sub"].dtype == expected_daft_dtype

    # Check that the result values are correct.
    expected_result = daft.from_arrow(
        pa.Table.from_pydict(
            {
                "ladd": pa.array([2, 0, -2], pa.timestamp(timeunit, timezone)),
                "radd": pa.array([2, 0, -2], pa.timestamp(timeunit, timezone)),
                "sub": pa.array([0, 0, 0], pa.timestamp(timeunit, timezone)),
            }
        )
    ).to_pydict()

    assert df.to_pydict() == expected_result


def test_temporal_arithmetic_date_with_duration() -> None:
    day_in_seconds = 60 * 60 * 24
    pa_table = pa.Table.from_pydict(
        {
            "date": pa.array([1, 1, 1, 0, -1, -1, -1], pa.date32()),
            "duration": pa.array(
                [
                    day_in_seconds,
                    day_in_seconds - 1,
                    day_in_seconds + 1,
                    0,
                    -day_in_seconds,
                    -day_in_seconds + 1,
                    -day_in_seconds - 1,
                ],
                pa.duration("s"),
            ),
        }
    )
    df = daft.from_arrow(pa_table)

    df = df.select(
        (df["date"] + df["duration"]).alias("ladd"),
        (df["duration"] + df["date"]).alias("radd"),
        (df["date"] - df["duration"]).alias("sub"),
    ).collect()

    # Check that the result dtypes are correct.
    expected_daft_dtype = daft.DataType.date()
    assert df.schema()["ladd"].dtype == expected_daft_dtype
    assert df.schema()["radd"].dtype == expected_daft_dtype
    assert df.schema()["sub"].dtype == expected_daft_dtype

    # Check that the result values are correct.
    expected_result = daft.from_arrow(
        pa.Table.from_pydict(
            {
                "ladd": pa.array([2, 1, 2, 0, -2, -1, -2], pa.date32()),
                "radd": pa.array([2, 1, 2, 0, -2, -1, -2], pa.date32()),
                "sub": pa.array([0, 1, 0, 0, 0, -1, 0], pa.date32()),
            }
        )
    ).to_pydict()

    assert df.to_pydict() == expected_result


@pytest.mark.parametrize(
    "t_timeunit",
    ["s", "ms", "us", "ns"],
)
@pytest.mark.parametrize(
    "d_timeunit",
    ["s", "ms", "us", "ns"],
)
@pytest.mark.parametrize(
    "timezone",
    [None, "UTC"],
)
def test_temporal_arithmetic_mismatch_granularity(t_timeunit, d_timeunit, timezone) -> None:
    if t_timeunit == d_timeunit:
        return

    pa_table = pa.Table.from_pydict(
        {
            "timestamp": pa.array([1, 0, -1], pa.timestamp(t_timeunit, timezone)),
            "duration": pa.array([1, 0, -1], pa.duration(d_timeunit)),
        }
    )

    df = daft.from_arrow(pa_table)
    for expression in [
        (df["timestamp"] + df["duration"]).alias("ladd"),
        (df["duration"] + df["timestamp"]).alias("radd"),
        (df["timestamp"] - df["duration"]).alias("sub"),
    ]:
        with pytest.raises(ValueError):
            df.select(expression).collect()


@pytest.mark.parametrize("tu1, tu2", itertools.product(["ns", "us", "ms"], repeat=2))
@pytest.mark.parametrize("tz_repr", ["UTC", "+00:00"])
def test_join_timestamp_same_timezone(tu1, tu2, tz_repr):
    tz1 = [datetime(2022, 1, 1, tzinfo=pytz.utc), datetime(2022, 2, 1, tzinfo=pytz.utc)]
    tz2 = [datetime(2022, 1, 2, tzinfo=pytz.utc), datetime(2022, 1, 1, tzinfo=pytz.utc)]
    df1 = daft.from_pydict({"t": tz1, "x": [1, 2]}).with_column("t", col("t").cast(DataType.timestamp(tu1, tz_repr)))
    df2 = daft.from_pydict({"t": tz2, "y": [3, 4]}).with_column("t", col("t").cast(DataType.timestamp(tu2, tz_repr)))
    res = df1.join(df2, on="t")
    assert res.to_pydict() == {
        "t": [datetime(2022, 1, 1, tzinfo=pytz.utc)],
        "x": [1],
        "y": [4],
    }


@pytest.mark.parametrize(
    "op,expected",
    [
        (
            (col("datetimes") + daft.interval(years=1)),
            [
                datetime(2022, 1, 1, 0, 0),
                datetime(2022, 1, 2, 0, 0),
                datetime(2021, 3, 1, 0, 0),
                datetime(2021, 2, 28, 0, 0),
            ],
        ),
        (
            (col("datetimes") + daft.interval(months=1)),
            [
                datetime(2021, 2, 1, 0, 0),
                datetime(2021, 2, 2, 0, 0),
                datetime(2020, 3, 29, 0, 0),
                datetime(2020, 3, 28, 0, 0),
            ],
        ),
        (
            (col("datetimes") + daft.interval(days=1)),
            [
                datetime(2021, 1, 2, 0, 0),
                datetime(2021, 1, 3, 0, 0),
                datetime(2020, 3, 1, 0, 0),
                datetime(2020, 2, 29, 0, 0),
            ],
        ),
        (
            (col("datetimes") + daft.interval(hours=1)),
            [
                datetime(2021, 1, 1, 1, 0),
                datetime(2021, 1, 2, 1, 0),
                datetime(2020, 2, 29, 1, 0),
                datetime(2020, 2, 28, 1, 0),
            ],
        ),
        (
            (col("datetimes") + daft.interval(minutes=1)),
            [
                datetime(2021, 1, 1, 0, 1),
                datetime(2021, 1, 2, 0, 1),
                datetime(2020, 2, 29, 0, 1),
                datetime(2020, 2, 28, 0, 1),
            ],
        ),
        (
            (col("datetimes") + daft.interval(seconds=1)),
            [
                datetime(2021, 1, 1, 0, 0, 1),
                datetime(2021, 1, 2, 0, 0, 1),
                datetime(2020, 2, 29, 0, 0, 1),
                datetime(2020, 2, 28, 0, 0, 1),
            ],
        ),
        (
            (col("datetimes") + daft.interval(millis=1)),
            [
                datetime(2021, 1, 1, 0, 0, 0, 1000),
                datetime(2021, 1, 2, 0, 0, 0, 1000),
                datetime(2020, 2, 29, 0, 0, 0, 1000),
                datetime(2020, 2, 28, 0, 0, 0, 1000),
            ],
        ),
        (
            (col("datetimes") - daft.interval(years=1)),
            [
                datetime(2020, 1, 2, 0, 0),
                datetime(2020, 1, 3, 0, 0),
                datetime(2019, 2, 28, 0, 0),
                datetime(2019, 2, 27, 0, 0),
            ],
        ),
        (
            (col("datetimes") - daft.interval(months=1)),
            [
                datetime(2020, 12, 1, 0, 0),
                datetime(2020, 12, 2, 0, 0),
                datetime(2020, 1, 31, 0, 0),
                datetime(2020, 1, 30, 0, 0),
            ],
        ),
        (
            (col("datetimes") - daft.interval(days=1)),
            [
                datetime(2021, 1, 2, 0, 0),
                datetime(2021, 1, 3, 0, 0),
                datetime(2020, 3, 1, 0, 0),
                datetime(2020, 2, 29, 0, 0),
            ],
        ),
        (
            (col("datetimes") - daft.interval(hours=1)),
            [
                datetime(2020, 12, 31, 23, 0),
                datetime(2021, 1, 1, 23, 0),
                datetime(2020, 2, 28, 23, 0),
                datetime(2020, 2, 27, 23, 0),
            ],
        ),
        (
            col("datetimes") - daft.interval(minutes=1),
            [
                datetime(2020, 12, 31, 23, 59),
                datetime(2021, 1, 1, 23, 59),
                datetime(2020, 2, 28, 23, 59),
                datetime(2020, 2, 27, 23, 59),
            ],
        ),
        (
            (col("datetimes") - daft.interval(seconds=1)),
            [
                datetime(2020, 12, 31, 23, 59, 59),
                datetime(2021, 1, 1, 23, 59, 59),
                datetime(2020, 2, 28, 23, 59, 59),
                datetime(2020, 2, 27, 23, 59, 59),
            ],
        ),
        (
            (col("datetimes") - daft.interval(millis=1)),
            [
                datetime(2020, 12, 31, 23, 59, 59, 999000),
                datetime(2021, 1, 1, 23, 59, 59, 999000),
                datetime(2020, 2, 28, 23, 59, 59, 999000),
                datetime(2020, 2, 27, 23, 59, 59, 999000),
            ],
        ),
    ],
)
def test_intervals(op, expected):
    datetimes = [
        datetime(2021, 1, 1, 0, 0, 0),
        datetime(2021, 1, 2, 0, 0, 0),
        # add a datetime with a leap event
        datetime(2020, 2, 29, 0, 0, 0),
        # and another one that should land on a leap event
        datetime(2020, 2, 28, 0, 0, 0),
    ]
    actual = (
        daft.from_pydict(
            {
                "datetimes": datetimes,
            }
        )
        .select(op)
        .collect()
        .to_pydict()
    )
    expected = {"datetimes": expected}

    assert actual == expected


@pytest.mark.parametrize(
    "value",
    [
        date(2020, 1, 1),  # explicit date
        "2020-01-01",  # implicit coercion
    ],
)
def test_date_comparison(value):
    date_df = daft.from_pydict({"date_str": ["2020-01-01", "2020-01-02", "2020-01-03"]})
    date_df = date_df.with_column("date", col("date_str").str.to_date("%Y-%m-%d"))
    actual = date_df.filter(col("date") == value).select("date").to_pydict()

    expected = {"date": [date(2020, 1, 1)]}

    assert actual == expected


def test_date_and_datetime_day_of_week():
    df = daft.from_pydict(
        {
            "date": [date(2020, 1, 1), date(2020, 12, 31), date(2021, 12, 31)],
            "datetime": [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 12, 31, 23, 59, 59),
                datetime(2021, 12, 31, 23, 59, 59),
            ],
        }
    )

    df = df.select(df["date"].dt.day_of_week().alias("date_dow"), df["datetime"].dt.day_of_week().alias("datetime_dow"))

    expected = {"date_dow": [2, 3, 4], "datetime_dow": [2, 3, 4]}

    assert df.to_pydict() == expected


def test_date_and_datetime_day_of_month():
    df = daft.from_pydict(
        {
            "date": [date(2020, 1, 1), date(2020, 12, 31), date(2021, 12, 31)],
            "datetime": [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 12, 31, 23, 59, 59),
                datetime(2021, 12, 31, 23, 59, 59),
            ],
        }
    )

    df = df.select(
        df["date"].dt.day_of_month().alias("date_dom"), df["datetime"].dt.day_of_month().alias("datetime_dom")
    )

    expected = {"date_dom": [1, 31, 31], "datetime_dom": [1, 31, 31]}

    assert df.to_pydict() == expected


def test_date_and_datetime_day_of_year():
    df = daft.from_pydict(
        {
            "date": [date(2020, 1, 1), date(2020, 12, 31), date(2021, 12, 31)],
            "datetime": [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 12, 31, 23, 59, 59),
                datetime(2021, 12, 31, 23, 59, 59),
            ],
        }
    )

    df = df.select(df["date"].dt.day_of_year().alias("date_doy"), df["datetime"].dt.day_of_year().alias("datetime_doy"))

    expected = {"date_doy": [1, 366, 365], "datetime_doy": [1, 366, 365]}

    assert df.to_pydict() == expected


def test_date_and_datetime_week_of_year():
    df = daft.from_pydict(
        {
            "date": [date(2020, 1, 1), date(2020, 12, 31), date(2021, 12, 31)],
            "datetime": [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 12, 31, 23, 59, 59),
                datetime(2021, 12, 31, 23, 59, 59),
            ],
        }
    )

    df = df.select(
        df["date"].dt.week_of_year().alias("date_woy"), df["datetime"].dt.week_of_year().alias("datetime_woy")
    )

    expected = {"date_woy": [1, 53, 52], "datetime_woy": [1, 53, 52]}

    assert df.to_pydict() == expected


def test_date_to_unix_epoch():
    df = daft.from_pydict(
        {
            "date": [date(2020, 1, 1), date(2020, 12, 31), date(2021, 12, 31)],
            "datetime": [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 12, 31, 23, 59, 59),
                datetime(2021, 12, 31, 23, 59, 59),
            ],
        }
    )
    actual = df.select(
        df["date"].dt.to_unix_epoch().alias("date_epoch"),
        df["date"].dt.to_unix_epoch("s").alias("date_epoch_s"),
        df["date"].dt.to_unix_epoch("ms").alias("date_epoch_ms"),
        df["date"].dt.to_unix_epoch("us").alias("date_epoch_us"),
        df["date"].dt.to_unix_epoch("ns").alias("date_epoch_ns"),
        df["datetime"].dt.to_unix_epoch().alias("datetime_epoch"),
        df["datetime"].dt.to_unix_epoch("s").alias("datetime_epoch_s"),
        df["datetime"].dt.to_unix_epoch("ms").alias("datetime_epoch_ms"),
        df["datetime"].dt.to_unix_epoch("us").alias("datetime_epoch_us"),
        df["datetime"].dt.to_unix_epoch("ns").alias("datetime_epoch_ns"),
    ).to_pydict()

    expected = {
        "date_epoch": [1577836800, 1609372800, 1640908800],
        "date_epoch_s": [1577836800, 1609372800, 1640908800],
        "date_epoch_ms": [1577836800000, 1609372800000, 1640908800000],
        "date_epoch_us": [1577836800000000, 1609372800000000, 1640908800000000],
        "date_epoch_ns": [1577836800000000000, 1609372800000000000, 1640908800000000000],
        "datetime_epoch": [1577836800, 1609459199, 1640995199],
        "datetime_epoch_s": [1577836800, 1609459199, 1640995199],
        "datetime_epoch_ms": [1577836800000, 1609459199000, 1640995199000],
        "datetime_epoch_us": [1577836800000000, 1609459199000000, 1640995199000000],
        "datetime_epoch_ns": [1577836800000000000, 1609459199000000000, 1640995199000000000],
    }

    assert actual == expected


def test_date_to_string():
    df = daft.from_pydict(
        {
            "dates": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
            "datetimes": [
                datetime(2023, 1, 1, 12, 1),
                datetime(2023, 1, 2, 12, 0, 0, 0),
                datetime(2023, 1, 3, 12, 0, 0, 999_999),
            ],
        }
    )

    df = df.with_column("datetimes_s", daft.col("datetimes").cast(daft.DataType.timestamp("s")))

    actual = df.select(
        daft.col("dates").dt.strftime().alias("iso_date"),
        daft.col("dates").dt.strftime(format="%m/%d/%Y").alias("custom_date"),
        daft.col("datetimes_s").dt.strftime().alias("iso_datetime"),
        daft.col("datetimes_s").dt.strftime(format="%Y/%m/%d %H:%M:%S").alias("custom_datetime"),
    ).to_pydict()

    expected = {
        "iso_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "custom_date": ["01/01/2023", "01/02/2023", "01/03/2023"],
        "iso_datetime": ["2023-01-01T12:01:00", "2023-01-02T12:00:00", "2023-01-03T12:00:00"],
        "custom_datetime": ["2023/01/01 12:01:00", "2023/01/02 12:00:00", "2023/01/03 12:00:00"],
    }

    assert actual == expected


@pytest.mark.parametrize(
    "timeunit",
    ["s", "ms", "us", "ns", "seconds", "milliseconds", "microseconds", "nanoseconds"],
)
def test_date_to_unix_epoch_valid_timeunits(timeunit):
    df = daft.from_pydict(
        {
            "date": [date(2020, 1, 1), date(2020, 12, 31), date(2021, 12, 31)],
        }
    )
    try:
        df.select(
            df["date"].dt.to_unix_epoch(timeunit).alias("date_epoch"),
        ).to_pydict()
    except ValueError:
        pytest.fail(f"to_unix_epoch with timeunit {timeunit} raised an exception.")


@pytest.mark.parametrize(
    "timeunit",
    ["second", "millis", "nanos", "millisecond", "nanosecond", "millis", "micros"],
)
def test_date_to_unix_epoch_invalid_timeunits(timeunit):
    df = daft.from_pydict(
        {
            "date": [date(2020, 1, 1)],
        }
    )
    try:
        df.select(
            df["date"].dt.to_unix_epoch(timeunit).alias("date_epoch"),
        ).to_pydict()
        pytest.fail(f"to_unix_epoch with timeunit {timeunit} did not raise an exception.")
    except ValueError:
        pass


@pytest.mark.parametrize(
    "fmt,expected",
    [
        ("%Y-%m-%d", ["2023-01-01", "2023-01-02", "2023-01-03"]),
        ("%m/%d/%Y", ["01/01/2023", "01/02/2023", "01/03/2023"]),
        ("%Y/%m/%d %H:%M:%S", ["2023/01/01 12:01:00", "2023/01/02 12:00:00", "2023/01/03 12:00:00"]),
        ("%c", ["Sun Jan  1 12:01:00 2023", "Mon Jan  2 12:00:00 2023", "Tue Jan  3 12:00:00 2023"]),
        ("%x", ["01/01/23", "01/02/23", "01/03/23"]),
    ],
)
def test_datetime_to_string(fmt, expected):
    df = daft.from_pydict(
        {
            "dates": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
            "datetimes": [
                datetime(2023, 1, 1, 12, 1),
                datetime(2023, 1, 2, 12, 0, 0, 0),
                datetime(2023, 1, 3, 12, 0, 0, 999_999),
            ],
        }
    )
    df = df.with_column("datetimes_s", daft.col("datetimes").cast(daft.DataType.timestamp("s")))

    actual = df.select(
        daft.col("datetimes").dt.strftime(fmt).alias("formatted_datetime"),
    ).to_pydict()

    expected = {
        "formatted_datetime": expected,
    }

    assert actual == expected


@pytest.mark.parametrize(
    "fmt,expected",
    [
        ("%H-%M-%S", ["01-02-03", "02-03-04", "03-04-05"]),
        ("%H:%M:%S", ["01:02:03", "02:03:04", "03:04:05"]),
        ("H:%H, M:%M, S:%S", ["H:01, M:02, S:03", "H:02, M:03, S:04", "H:03, M:04, S:05"]),
    ],
)
def test_time_to_string(fmt, expected):
    from datetime import time

    df = daft.from_pydict(
        {
            "times": [time(1, 2, 3), time(2, 3, 4), time(3, 4, 5)],
        }
    )

    actual = df.select(
        daft.col("times").dt.strftime(fmt).alias("formatted_time"),
    ).to_pydict()

    expected = {
        "formatted_time": expected,
    }

    assert actual == expected


@pytest.mark.parametrize(
    "value",
    [
        1,
        True,
        "foo",
        1.0,
        [1],
        [True],
        [1.0],
    ],
)
def test_datetime_to_string_errors(value):
    df = daft.from_pydict({"invalid": [value]})

    with pytest.raises(daft.exceptions.DaftCoreException):
        df.select(daft.col("invalid").dt.strftime(format="%Y-%m-%d")).to_pydict()
