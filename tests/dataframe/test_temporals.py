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
