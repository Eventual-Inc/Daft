from __future__ import annotations

import tempfile
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

import daft

PYARROW_GE_7_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (7, 0, 0)


def test_temporal_arithmetic() -> None:
    now = datetime.now()
    now_tz = datetime.now(timezone.utc)
    df = daft.from_pydict(
        {
            "dt_us": [datetime.min, now],
            "dt_us_tz": [datetime.min.replace(tzinfo=timezone.utc), now_tz],
            "duration": [timedelta(days=1), timedelta(microseconds=1)],
        }
    )

    df = df.select(
        (df["dt_us"] - df["dt_us"]).alias("zero1"),
        (df["dt_us_tz"] - df["dt_us_tz"]).alias("zero2"),
        (df["dt_us"] + (2 * df["duration"]) - df["duration"]).alias("addsub"),
        (df["dt_us_tz"] + (2 * df["duration"]) - df["duration"]).alias("addsub_tz"),
        (df["duration"] + df["duration"]).alias("add_dur"),
    )

    result = df.to_pydict()
    assert result["zero1"] == [timedelta(0), timedelta(0)]
    assert result["zero2"] == [timedelta(0), timedelta(0)]
    assert result["addsub"] == [datetime.min + timedelta(days=1), now + timedelta(microseconds=1)]
    assert result["addsub_tz"] == [
        (datetime.min + timedelta(days=1)).replace(tzinfo=timezone.utc),
        now_tz + timedelta(microseconds=1),
    ]
    assert result["add_dur"] == [timedelta(days=2), timedelta(microseconds=2)]


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
            "timestamp_s_tz": pa.array([1], pa.timestamp("s", tz="UTC")),
            "timestamp_ms_tz": pa.array([1], pa.timestamp("ms", tz="UTC")),
            "timestamp_us_tz": pa.array([1], pa.timestamp("us", tz="UTC")),
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


@pytest.mark.skipif(not PYARROW_GE_7_0_0, reason="PyArrow conversion of timezoned datetime is broken in 6.0.1")
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


@pytest.mark.parametrize(
    "timeunit",
    ["s", "ms", "us", "ns"],
)
@pytest.mark.parametrize(
    "timezone",
    [None, "UTC"],
)
def test_temporal_arithmetic(timeunit, timezone) -> None:
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
