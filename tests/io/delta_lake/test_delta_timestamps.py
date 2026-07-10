from __future__ import annotations

import datetime

import pytest

deltalake = pytest.importorskip("deltalake")

import daft


def test_tz_aware_timestamp_commits(tmp_path):
    """Daft infers tz '+00:00' from python datetimes; deltalake>=1.0 rejects that
    spelling in Schema.from_arrow. Without the normalization this raises at commit."""
    ts = datetime.datetime(2021, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc)
    daft.from_pydict({"t": [ts]}).write_deltalake(str(tmp_path))

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table()
    got = table.column("t").to_pylist()[0]
    assert got.replace(tzinfo=datetime.timezone.utc) == ts


def test_tz_aware_append_against_existing_table(tmp_path):
    """Append re-derives the delta schema and compares it against the committed one.
    A '+00:00' vs 'UTC' mismatch used to raise ValueError here."""
    ts1 = datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc)
    ts2 = datetime.datetime(2021, 6, 1, tzinfo=datetime.timezone.utc)
    daft.from_pydict({"t": [ts1]}).write_deltalake(str(tmp_path))
    daft.from_pydict({"t": [ts2]}).write_deltalake(str(tmp_path), mode="append")

    rows = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("t").to_pylist()
    assert len(rows) == 2


def test_tz_aware_timestamp_nested_in_struct(tmp_path):
    """The normalization recurses into nested types."""
    import pyarrow as pa

    ts = datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc)
    struct_type = pa.struct([pa.field("at", pa.timestamp("us", tz="+00:00"))])
    tbl = pa.table({"s": pa.array([{"at": ts}], type=struct_type)})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    got = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("s").to_pylist()[0]
    assert got["at"].replace(tzinfo=datetime.timezone.utc) == ts


def test_naive_timestamp_is_timestamp_ntz(tmp_path):
    """Naive timestamps must not be relabelled as UTC-aware."""
    import json

    naive = datetime.datetime(2021, 1, 1, 0, 0, 1)
    daft.from_pydict({"t": [naive]}).write_deltalake(str(tmp_path))

    schema = json.loads(deltalake.DeltaTable(str(tmp_path)).schema().to_json())
    assert schema["fields"][0]["type"] == "timestamp_ntz"
