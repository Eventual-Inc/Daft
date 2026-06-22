"""Tests for the UUIDv7 timestamp-extraction partition transforms.

`extract_minute_uuid7` / `extract_hour_uuid7` / `extract_day_uuid7` /
`extract_month_uuid7` decode the 48-bit Unix-millisecond timestamp embedded in
the first 6 bytes of a UUIDv7 and bucket it, mirroring the Iceberg-style
`partition_hours` / `partition_days` / `partition_months` transforms. The input
is a `Uuid` or a `FixedSizeBinary(16)` (128 bits); the output is `Int64`.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import daft
from daft import DataType, col
from daft.functions import (
    extract_day_uuid7,
    extract_hour_uuid7,
    extract_minute_uuid7,
    extract_month_uuid7,
)
from daft.functions import uuid as uuid_fn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _uuid7_bytes(ms: int, *, filler: int = 0xAB) -> bytes:
    """Builds a 16-byte UUIDv7-shaped value with `ms` in the leading 48 bits.

    The version (0x7) and variant (0b10) bits are set as a real UUIDv7 would
    have them; the remaining bytes are `filler` to prove they don't leak into
    the extracted timestamp.
    """
    b = bytearray([filler] * 16)
    b[0:6] = ms.to_bytes(6, "big")
    b[6] = 0x70 | (b[6] & 0x0F)
    b[8] = 0x80 | (b[8] & 0x3F)
    return bytes(b)


def _ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _fsb16_df(values: list[bytes | None]) -> daft.DataFrame:
    """A single-column DataFrame `id` of dtype FixedSizeBinary(16)."""
    return daft.from_pydict({"id": values}).select(col("id").cast(DataType.fixed_size_binary(16)))


def _df_schema_dtypes(df: daft.DataFrame) -> list[DataType]:
    return [df.schema()[name].dtype for name in df.column_names]


# Known instants and their expected buckets, computed independently in Python.
_INSTANTS = [
    datetime(1970, 1, 1),
    datetime(1970, 1, 1, 0, 1),  # one minute after epoch
    datetime(1999, 12, 31, 23, 59, 59),
    datetime(2024, 1, 1),
    datetime(2024, 3, 15, 12, 30, 45),
    datetime(2026, 5, 30, 8, 0, 0),
]


def _expected(dt: datetime) -> dict[str, int]:
    ms = _ms(dt)
    return {
        "minute": ms // 60_000,
        "hour": ms // 3_600_000,
        "day": ms // 86_400_000,
        "month": (dt.year - 1970) * 12 + (dt.month - 1),
    }


# ---------------------------------------------------------------------------
# Core correctness over FixedSizeBinary(16)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dt", _INSTANTS)
def test_extract_all_units_known_instants(dt: datetime):
    df = _fsb16_df([_uuid7_bytes(_ms(dt))])
    out = df.select(
        extract_minute_uuid7(col("id")).alias("minute"),
        extract_hour_uuid7(col("id")).alias("hour"),
        extract_day_uuid7(col("id")).alias("day"),
        extract_month_uuid7(col("id")).alias("month"),
    )
    # All four transforms return Int64.
    for dtype in _df_schema_dtypes(out):
        assert dtype == DataType.int64()
    result = out.to_pydict()
    exp = _expected(dt)
    assert result["minute"] == [exp["minute"]]
    assert result["hour"] == [exp["hour"]]
    assert result["day"] == [exp["day"]]
    assert result["month"] == [exp["month"]]


def test_epoch_buckets_to_zero():
    df = _fsb16_df([_uuid7_bytes(0)])
    out = df.select(
        extract_minute_uuid7(col("id")).alias("minute"),
        extract_hour_uuid7(col("id")).alias("hour"),
        extract_day_uuid7(col("id")).alias("day"),
        extract_month_uuid7(col("id")).alias("month"),
    ).to_pydict()
    assert out == {"minute": [0], "hour": [0], "day": [0], "month": [0]}


@pytest.mark.parametrize(
    "dt,expected_month",
    [
        (datetime(1970, 1, 1), 0),
        (datetime(1970, 2, 1), 1),
        (datetime(1970, 12, 31), 11),
        (datetime(1971, 1, 1), 12),
        (datetime(2000, 12, 31, 23, 59, 59), (2000 - 1970) * 12 + 11),
        (datetime(2024, 3, 15), (2024 - 1970) * 12 + 2),
    ],
)
def test_month_is_calendar_months_since_epoch(dt: datetime, expected_month: int):
    df = _fsb16_df([_uuid7_bytes(_ms(dt))])
    out = df.select(extract_month_uuid7(col("id")).alias("month")).to_pydict()
    assert out["month"] == [expected_month]


def test_multiple_rows_and_nulls():
    rows = [_uuid7_bytes(_ms(datetime(2024, 1, 1))), None, _uuid7_bytes(_ms(datetime(2024, 3, 15)))]
    df = _fsb16_df(rows)
    out = df.select(extract_hour_uuid7(col("id")).alias("hour")).to_pydict()
    assert out["hour"] == [
        _ms(datetime(2024, 1, 1)) // 3_600_000,
        None,
        _ms(datetime(2024, 3, 15)) // 3_600_000,
    ]


def test_version_and_variant_bits_do_not_affect_extraction():
    ms = _ms(datetime(2024, 3, 15, 12, 30, 45))
    # Two values with the same timestamp but different filler / version-variant nibbles.
    a = _uuid7_bytes(ms, filler=0x00)
    b = _uuid7_bytes(ms, filler=0xFF)
    df = _fsb16_df([a, b])
    out = df.select(
        extract_minute_uuid7(col("id")).alias("minute"),
        extract_hour_uuid7(col("id")).alias("hour"),
        extract_day_uuid7(col("id")).alias("day"),
        extract_month_uuid7(col("id")).alias("month"),
    ).to_pydict()
    assert out["minute"][0] == out["minute"][1]
    assert out["hour"][0] == out["hour"][1]
    assert out["day"][0] == out["day"][1]
    assert out["month"][0] == out["month"][1]


# ---------------------------------------------------------------------------
# Uuid logical-type input
# ---------------------------------------------------------------------------
def test_uuid_logical_type_matches_fixed_size_binary_path():
    """Extraction on a Uuid column equals extraction on its FixedSizeBinary(16) bytes."""
    gen = daft.from_pydict({"x": list(range(16))}).select(uuid_fn("v7").alias("id"))
    out = gen.select(
        extract_hour_uuid7(col("id")).alias("uuid_hour"),
        extract_minute_uuid7(col("id")).alias("uuid_minute"),
        extract_day_uuid7(col("id")).alias("uuid_day"),
        extract_month_uuid7(col("id")).alias("uuid_month"),
        # Cast the same Uuid to FixedSizeBinary(16) and extract from that.
        extract_hour_uuid7(col("id").cast(DataType.fixed_size_binary(16))).alias("fsb_hour"),
        extract_minute_uuid7(col("id").cast(DataType.fixed_size_binary(16))).alias("fsb_minute"),
        extract_day_uuid7(col("id").cast(DataType.fixed_size_binary(16))).alias("fsb_day"),
        extract_month_uuid7(col("id").cast(DataType.fixed_size_binary(16))).alias("fsb_month"),
    ).to_pydict()
    assert out["uuid_hour"] == out["fsb_hour"]
    assert out["uuid_minute"] == out["fsb_minute"]
    assert out["uuid_day"] == out["fsb_day"]
    assert out["uuid_month"] == out["fsb_month"]


def test_extracted_units_are_internally_consistent():
    gen = daft.from_pydict({"x": list(range(8))}).select(uuid_fn("v7").alias("id"))
    out = gen.select(
        extract_minute_uuid7(col("id")).alias("minute"),
        extract_hour_uuid7(col("id")).alias("hour"),
        extract_day_uuid7(col("id")).alias("day"),
    ).to_pydict()
    for minute, hour, day in zip(out["minute"], out["hour"], out["day"]):
        assert minute // 60 == hour
        assert hour // 24 == day
        # uuidv7() uses the current time; sanity-check it is well past 2020.
        assert day > _ms(datetime(2020, 1, 1)) // 86_400_000


# ---------------------------------------------------------------------------
# Type validation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "df",
    [
        daft.from_pydict({"id": [1, 2, 3]}),  # Int64
        daft.from_pydict({"id": ["a", "b"]}),  # Utf8
        daft.from_pydict({"id": [b"abcdefgh"]}).select(
            col("id").cast(DataType.fixed_size_binary(8))
        ),  # wrong fixed size
    ],
)
def test_rejects_invalid_input_types(df: daft.DataFrame):
    with pytest.raises(Exception):
        df.select(extract_hour_uuid7(col("id"))).collect()


# ---------------------------------------------------------------------------
# SQL accessibility
# ---------------------------------------------------------------------------
def test_sql_matches_expression_api():
    df = _fsb16_df([_uuid7_bytes(_ms(dt)) for dt in _INSTANTS])
    expected = df.select(
        extract_minute_uuid7(col("id")).alias("minute"),
        extract_hour_uuid7(col("id")).alias("hour"),
        extract_day_uuid7(col("id")).alias("day"),
        extract_month_uuid7(col("id")).alias("month"),
    ).to_pydict()
    actual = daft.sql(
        """
        SELECT
            extract_minute_uuid7(id) AS minute,
            extract_hour_uuid7(id) AS hour,
            extract_day_uuid7(id) AS day,
            extract_month_uuid7(id) AS month
        FROM df
        """,
        df=df,
    ).to_pydict()
    assert actual == expected
