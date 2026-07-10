from __future__ import annotations

import json

import pytest

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa

import daft


def stats_of(tmp_path):
    log = tmp_path / "_delta_log" / "00000000000000000000.json"
    adds = [
        json.loads(line)["add"]
        for line in log.read_text().splitlines()
        if "add" in json.loads(line)
    ]
    assert len(adds) == 1
    return json.loads(adds[0]["stats"])


def test_non_utf8_binary_bounds_are_omitted(tmp_path):
    """b"\\xff\\xfe" must not be reported as the string "ÿþ" — that is not the data."""
    tbl = pa.table({"b": pa.array([b"apple", b"\xff\xfe", None], pa.binary())})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    s = stats_of(tmp_path)
    assert "b" not in s["minValues"]
    assert "b" not in s["maxValues"]
    # nullCount is still reported — only the bounds are unsafe.
    assert s["nullCount"]["b"] == 1
    assert s["numRecords"] == 3


def test_utf8_decodable_binary_keeps_its_bounds(tmp_path):
    tbl = pa.table({"b": pa.array([b"apple", b"cherry"], pa.binary())})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    s = stats_of(tmp_path)
    assert s["minValues"]["b"] == "apple"
    assert s["maxValues"]["b"] == "cherry"


def test_binary_data_round_trips_regardless_of_stats(tmp_path):
    tbl = pa.table({"b": pa.array([b"apple", b"\xff\xfe"], pa.binary())})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    got = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("b").to_pylist()
    assert sorted(got) == sorted([b"apple", b"\xff\xfe"])


import datetime
import decimal


def test_date_bounds(tmp_path):
    tbl = pa.table({"d": pa.array([datetime.date(2020, 12, 31), datetime.date(2021, 1, 2)])})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert s["minValues"]["d"] == "2020-12-31"
    assert s["maxValues"]["d"] == "2021-01-02"


def test_decimal_bounds(tmp_path):
    tbl = pa.table(
        {"x": pa.array([decimal.Decimal("-0.05"), decimal.Decimal("123.45")], pa.decimal128(10, 2))}
    )
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert decimal.Decimal(s["minValues"]["x"]) == decimal.Decimal("-0.05")
    assert decimal.Decimal(s["maxValues"]["x"]) == decimal.Decimal("123.45")


def test_tz_aware_timestamp_bounds(tmp_path):
    lo = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    hi = datetime.datetime(2021, 6, 1, 12, 30, tzinfo=datetime.timezone.utc)
    daft.from_pydict({"t": [hi, lo]}).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)

    def instant(v):
        return datetime.datetime.fromisoformat(v.replace("Z", "+00:00"))

    assert instant(s["minValues"]["t"]) == lo
    assert instant(s["maxValues"]["t"]) == hi


def test_naive_timestamp_bounds(tmp_path):
    lo = datetime.datetime(2021, 1, 1, 0, 0, 1)
    hi = datetime.datetime(2021, 6, 1)
    daft.from_pydict({"t": [hi, lo]}).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert datetime.datetime.fromisoformat(s["minValues"]["t"]) == lo
    assert datetime.datetime.fromisoformat(s["maxValues"]["t"]) == hi


def test_int_and_string_bounds(tmp_path):
    daft.from_pydict({"i": [3, -7, 20], "s": ["banana", "apple", "cherry"]}).write_deltalake(
        str(tmp_path)
    )
    s = stats_of(tmp_path)
    assert s["minValues"]["i"] == -7 and s["maxValues"]["i"] == 20
    assert s["minValues"]["s"] == "apple" and s["maxValues"]["s"] == "cherry"


def test_nan_bound_is_dropped(tmp_path):
    daft.from_pydict({"f": [1.0, float("nan")]}).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    # Parquet stats ignore NaN; the finite value survives as the bound.
    assert s["minValues"]["f"] == 1.0


def test_infinite_bounds_are_dropped(tmp_path):
    daft.from_pydict({"f": [float("-inf"), float("inf")]}).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    # Infinity cannot be serialized to JSON; a missing bound is safe.
    assert "f" not in s["minValues"]
    assert "f" not in s["maxValues"]
    # The stats blob must be strict JSON: no NaN/Infinity literals.
    log = (tmp_path / "_delta_log" / "00000000000000000000.json").read_text()
    add = [json.loads(l)["add"] for l in log.splitlines() if "add" in json.loads(l)][0]
    json.loads(add["stats"], parse_constant=lambda c: pytest.fail(f"invalid JSON constant {c}"))


def test_all_null_column_has_null_count_but_no_bounds(tmp_path):
    tbl = pa.table({"a": pa.array([None, None, None], pa.int64())})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert s["nullCount"]["a"] == 3
    assert "a" not in s["minValues"]
    assert "a" not in s["maxValues"]


def test_nested_struct_uses_dotted_stat_keys(tmp_path):
    tbl = pa.table(
        {
            "point": pa.array(
                [{"x": 1, "y": "a"}, {"x": 9, "y": "b"}],
                type=pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.string())]),
            )
        }
    )
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert s["minValues"]["point.x"] == 1 and s["maxValues"]["point.x"] == 9
    assert s["minValues"]["point.y"] == "a" and s["maxValues"]["point.y"] == "b"


def test_long_string_bounds_are_valid(tmp_path):
    """PyArrow emits exact bounds for strings past parquet's 64-byte truncation limit."""
    lo, hi = "a" * 80, "z" * 80
    daft.from_pydict({"s": [lo, hi]}).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert s["minValues"]["s"] <= lo
    assert s["maxValues"]["s"] >= hi
