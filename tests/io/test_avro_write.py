from __future__ import annotations

import pytest

import daft


def test_write_avro_basic(tmp_path):
    """Basic write and read back verification."""
    data = {"x": [1, 2, 3], "y": ["a", "b", "c"]}
    df = daft.from_pydict(data)
    path = str(tmp_path)
    result = df.write_avro(path, write_mode="overwrite")
    assert result is not None

    read_df = daft.read_avro(path)
    assert read_df.to_pydict() == data


def test_write_avro_null_compression(tmp_path):
    data = {"x": [1, 2]}
    df = daft.from_pydict(data)
    path = str(tmp_path)
    df.write_avro(path, compression="null", write_mode="overwrite")
    result = daft.read_avro(path)
    assert result.to_pydict() == data


def test_write_avro_deflate_compression(tmp_path):
    data = {"x": [1, 2, 3]}
    df = daft.from_pydict(data)
    path = str(tmp_path)
    df.write_avro(path, compression="deflate", write_mode="overwrite")
    result = daft.read_avro(path)
    assert result.to_pydict() == data


def test_write_avro_overwrite_mode(tmp_path):
    """Overwrite should replace existing data."""
    data1 = {"x": [1, 2]}
    data2 = {"x": [3, 4]}
    df1 = daft.from_pydict(data1)
    df2 = daft.from_pydict(data2)
    path = str(tmp_path)

    df1.write_avro(path, write_mode="overwrite")
    df2.write_avro(path, write_mode="overwrite")

    result = daft.read_avro(path)
    assert sorted(result.to_pydict()["x"]) == [3, 4]


def test_write_avro_append_mode(tmp_path):
    """Append mode should add data alongside existing files."""
    data1 = {"x": [1, 2]}
    data2 = {"x": [3, 4]}
    df1 = daft.from_pydict(data1)
    df2 = daft.from_pydict(data2)
    path = str(tmp_path)

    df1.write_avro(path, write_mode="overwrite")
    df2.write_avro(path, write_mode="append")

    result = daft.read_avro(path)
    assert result.count_rows() == 4


def test_write_avro_empty_dataframe(tmp_path):
    """Empty DataFrame write should not error (may not produce a file)."""
    data = {"x": []}
    df = daft.from_pydict(data)
    path = str(tmp_path)
    df.write_avro(path, write_mode="overwrite")
    # Empty writes may not create a file, which is acceptable


def test_write_avro_invalid_compression(tmp_path):
    data = {"x": [1]}
    df = daft.from_pydict(data)
    path = str(tmp_path)
    with pytest.raises(Exception):
        df.write_avro(path, compression="snappy", write_mode="overwrite")


# === End-to-end round-trip tests ===


def test_roundtrip_all_nullable_types(tmp_path):
    """Write and read back all nullable types to ensure round-trip consistency."""
    data = {
        "ints": [1, None, 3],
        "floats": [1.0, None, 3.0],
        "strings": ["a", None, "c"],
        "bools": [True, False, None],
    }
    df = daft.from_pydict(data)
    path = str(tmp_path)
    df.write_avro(path, write_mode="overwrite")
    result = daft.read_avro(path)
    assert result.to_pydict() == data


def test_roundtrip_multiple_writes(tmp_path):
    """Data from multiple writes should be visible (regression for buffer-overwrite bug)."""
    data1 = {"x": [1, 2]}
    df1 = daft.from_pydict(data1)
    path = str(tmp_path)
    df1.write_avro(path, write_mode="overwrite")

    data2 = {"x": [3, 4]}
    df2 = daft.from_pydict(data2)
    df2.write_avro(path, write_mode="append")

    result = daft.read_avro(path)
    assert result.count_rows() == 4


def test_roundtrip_large_uint64_rejected(tmp_path):
    """UInt64 values exceeding i64::MAX should be rejected during write."""
    import pyarrow as pa

    data = pa.table({"x": pa.array([2**63], type=pa.uint64())})
    df = daft.from_arrow(data)
    path = str(tmp_path)
    with pytest.raises(Exception, match="exceeds Avro Long"):
        df.write_avro(path, write_mode="overwrite")


def test_cross_tool_compatibility_with_fastavro(tmp_path):
    """Verify Daft-written Avro files are readable by external Avro tools."""
    fastavro = pytest.importorskip("fastavro")

    data = {
        "ints": [1, 2, 3, None, 5],
        "floats": [1.0, 2.5, None, 4.0, 5.5],
        "strings": ["a", None, "c", "d", ""],
        "bools": [True, False, None, True, False],
    }
    df = daft.from_pydict(data)
    path = str(tmp_path)
    df.write_avro(path, compression="deflate", write_mode="overwrite")

    avro_files = list(tmp_path.glob("*.avro"))
    assert len(avro_files) == 1, f"Expected 1 avro file, got {len(avro_files)}"

    with open(avro_files[0], "rb") as f:
        reader = fastavro.reader(f)
        records = list(reader)

    assert len(records) == 5
    for i, rec in enumerate(records):
        for key in data:
            expected = data[key][i]
            actual = rec.get(key)
            if expected is None:
                assert actual is None, f"row {i} col {key}: expected None, got {actual}"
            else:
                assert actual == expected, f"row {i} col {key}: expected {expected}, got {actual}"
