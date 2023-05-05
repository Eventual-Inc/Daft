from __future__ import annotations

from datetime import datetime, timedelta, timezone

import daft


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
