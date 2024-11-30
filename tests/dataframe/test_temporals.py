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
@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_temporal_file_roundtrip(format, use_native_downloader) -> None:
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
            df_readback = daft.read_csv(dirname, use_native_downloader=use_native_downloader).collect()
        elif format == "parquet":
            df.write_parquet(dirname)
            df_readback = daft.read_parquet(dirname, use_native_downloader=use_native_downloader).collect()

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
