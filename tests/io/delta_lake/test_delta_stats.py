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
