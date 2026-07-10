from __future__ import annotations

import json

import pytest

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa

import daft


def _delta_type(path, field="u"):
    schema = json.loads(deltalake.DeltaTable(str(path)).schema().to_json())
    return {f["name"]: f["type"] for f in schema["fields"]}[field]


@pytest.mark.parametrize(
    ("arrow_type", "max_value", "expected_delta_type"),
    [
        (pa.uint8(), 255, "short"),
        (pa.uint16(), 65535, "integer"),
        (pa.uint32(), 4294967295, "long"),
        (pa.uint64(), (1 << 63) - 1, "long"),
    ],
)
def test_unsigned_max_value_round_trips(tmp_path, arrow_type, max_value, expected_delta_type):
    """Today uint8>127 and uint32>2^31-1 commit a table that raises on read."""
    tbl = pa.table({"u": pa.array([0, max_value], arrow_type)})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    assert _delta_type(tmp_path) == expected_delta_type
    got = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("u").to_pylist()
    assert sorted(got) == [0, max_value]


def test_uint64_above_i64_max_raises_naming_the_column(tmp_path):
    tbl = pa.table({"u": pa.array([1, (1 << 63) + 42], pa.uint64())})
    with pytest.raises(ValueError) as excinfo:
        daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    msg = str(excinfo.value)
    assert "u" in msg
    assert "uint64" in msg.lower()


def test_uint64_above_i64_max_in_struct_raises_naming_the_column(tmp_path):
    """A uint64 overflow nested inside a struct must raise the helpful ValueError, not let the raw ArrowInvalid escape."""
    struct_type = pa.struct([pa.field("n", pa.uint64())])
    tbl = pa.table({"s": pa.array([{"n": (1 << 63) + 42}], type=struct_type)})
    with pytest.raises(ValueError) as excinfo:
        daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    msg = str(excinfo.value)
    assert "Failed to write column" in msg
    assert "s" in msg
    assert "uint64" in msg.lower()


def test_uint64_above_i64_max_in_list_raises_naming_the_column(tmp_path):
    """A uint64 overflow nested inside a list must raise the helpful ValueError, not let the raw ArrowInvalid escape."""
    tbl = pa.table({"l": pa.array([[(1 << 63) + 42]], type=pa.list_(pa.uint64()))})
    with pytest.raises(ValueError) as excinfo:
        daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    msg = str(excinfo.value)
    assert "Failed to write column" in msg
    assert "l" in msg
    assert "uint64" in msg.lower()


def test_uint64_overflow_error_names_only_uint64_column(tmp_path):
    """A non-overflowing uint8 column must not be named alongside the uint64 column that actually overflowed."""
    tbl = pa.table(
        {
            "small": pa.array([0, 255], pa.uint8()),
            "big": pa.array([1, (1 << 63) + 42], pa.uint64()),
        }
    )
    with pytest.raises(ValueError) as excinfo:
        daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    msg = str(excinfo.value)
    assert "big" in msg
    assert "small" not in msg


def test_unsigned_nested_in_struct_is_widened(tmp_path):
    struct_type = pa.struct([pa.field("n", pa.uint32())])
    tbl = pa.table({"s": pa.array([{"n": 3_000_000_000}], type=struct_type)})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    got = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("s").to_pylist()[0]
    assert got["n"] == 3_000_000_000


def test_unsigned_nested_in_list_is_widened(tmp_path):
    tbl = pa.table({"l": pa.array([[3_000_000_000]], type=pa.list_(pa.uint32()))})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    got = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("l").to_pylist()[0]
    assert got == [3_000_000_000]


def test_signed_types_are_untouched(tmp_path):
    tbl = pa.table({"u": pa.array([-1, 5], pa.int32())})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    assert _delta_type(tmp_path) == "integer"
