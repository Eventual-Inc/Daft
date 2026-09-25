from __future__ import annotations

import pytest

import daft
from daft import DataType


def write_avro_and_read(path, data, compression="null"):
    """Helper: write a DataFrame to Avro and read it back."""
    df = daft.from_pydict(data)
    df.write_avro(path, compression=compression, write_mode="overwrite")
    return daft.read_avro(path)


def test_read_avro_single_file(tmp_path):
    data = {"x": [1, 2, 3], "y": ["a", "b", "c"]}
    path = str(tmp_path)
    write_avro_and_read(path, data)
    result = daft.read_avro(path)
    assert result.to_pydict() == data


def test_read_avro_directory_glob(tmp_path):
    data = {"x": [1, 2], "y": ["a", "b"]}
    path = str(tmp_path)
    write_avro_and_read(path, data)
    result = daft.read_avro(path)
    assert result.to_pydict() == data


def test_read_avro_schema_inference(tmp_path):
    data = {"a": [1, 2, 3], "b": [1.0, 2.0, 3.0], "c": ["x", "y", "z"], "d": [True, False, True]}
    path = str(tmp_path)
    write_avro_and_read(path, data)
    df = daft.read_avro(path, infer_schema=True)
    cols = df.column_names
    assert "a" in cols
    assert "b" in cols
    assert "c" in cols
    assert "d" in cols


def test_read_avro_user_schema(tmp_path):
    data = {"x": [1, 2, 3]}
    path = str(tmp_path)
    write_avro_and_read(path, data)
    df = daft.read_avro(path, schema={"x": DataType.int32()})
    assert df.to_pydict() == data


def test_read_avro_file_path_column(tmp_path):
    data = {"x": [1, 2]}
    path = str(tmp_path)
    write_avro_and_read(path, data)
    df = daft.read_avro(path, file_path_column="source_path")
    assert "source_path" in df.column_names


def test_read_avro_hive_partitioning(tmp_path):
    """Write partitioned Avro and read back with hive partitioning."""
    data = {"x": [1, 2], "part": ["a", "b"]}
    path = str(tmp_path)
    df = daft.from_pydict(data)
    df.write_avro(path, partition_cols=["part"], write_mode="overwrite")
    # Read back from a specific partition directory
    result = daft.read_avro(str(tmp_path) + "/part=a/").to_pydict()
    assert result == {"x": [1]}


def test_read_avro_nonexistent_path(tmp_path):
    with pytest.raises(Exception):
        daft.read_avro(str(tmp_path / "nonexistent")).collect()


def test_read_avro_nullable_values(tmp_path):
    data = {"x": [1, None, 3], "y": ["a", None, "c"]}
    path = str(tmp_path)
    write_avro_and_read(path, data)
    result = daft.read_avro(path)
    assert result.to_pydict() == data


def test_read_avro_boolean(tmp_path):
    data = {"b": [True, False, True, None]}
    path = str(tmp_path)
    write_avro_and_read(path, data)
    result = daft.read_avro(path)
    assert result.to_pydict() == data


def test_read_avro_float_types(tmp_path):
    data = {"f32": [1.0, 2.0, 3.0], "f64": [1.1, 2.2, 3.3]}
    path = str(tmp_path)
    write_avro_and_read(path, data)
    result = daft.read_avro(path)
    assert result.to_pydict() == data


def test_read_avro_integer_types(tmp_path):
    data = {"i8": [1, 2, 3], "i16": [100, 200, 300], "i32": [1000, 2000, 3000], "i64": [100000, 200000, 300000]}
    path = str(tmp_path)
    write_avro_and_read(path, data)
    result = daft.read_avro(path)
    assert result.to_pydict() == data


def test_read_avro_wildcard_pattern(tmp_path):
    data = {"x": [1, 2]}
    (tmp_path / "data").mkdir(exist_ok=True)
    df = daft.from_pydict(data)
    df.write_avro(str(tmp_path / "data"), write_mode="overwrite")
    result = daft.read_avro(str(tmp_path / "data" / "*.avro"))
    assert result.to_pydict() == data


def test_read_avro_column_projection(tmp_path):
    """Column pushdown: selecting a subset of columns should work correctly."""
    data = {"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [10.0, 20.0, 30.0]}
    path = str(tmp_path)
    write_avro_and_read(path, data)
    df = daft.read_avro(path).select("a", "c")
    result = df.to_pydict()
    assert result == {"a": [1, 2, 3], "c": [10.0, 20.0, 30.0]}
    assert "b" not in result


def test_read_avro_column_projection_single_col(tmp_path):
    """Column pushdown: selecting a single column should work."""
    data = {"x": [1, 2, 3], "y": ["a", "b", "c"], "z": [True, False, True]}
    path = str(tmp_path)
    write_avro_and_read(path, data)
    df = daft.read_avro(path).select("z")
    result = df.to_pydict()
    assert result == {"z": [True, False, True]}
    assert "x" not in result
    assert "y" not in result


def test_read_avro_column_projection_roundtrip(tmp_path):
    """Write then read back with column projection for full pipeline coverage."""
    data = {"a": [1, 2], "b": ["hello", "world"], "c": [3.0, 4.0]}
    df = daft.from_pydict(data)
    path = str(tmp_path)
    df.write_avro(path, write_mode="overwrite")
    result = daft.read_avro(path).select("b", "c")
    assert sorted(result.to_pydict()["b"]) == ["hello", "world"]
    assert sorted(result.to_pydict()["c"]) == [3.0, 4.0]
