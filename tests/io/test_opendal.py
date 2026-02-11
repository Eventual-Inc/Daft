"""Integration tests for OpenDAL backend support via IOConfig(backends={...})."""

from __future__ import annotations

import csv as csv_mod
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft.daft import IOConfig


@pytest.fixture
def parquet_data(tmp_path):
    """Create a temporary parquet file with sample data."""
    table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    papq.write_table(table, str(tmp_path / "data.parquet"))
    return tmp_path


@pytest.fixture
def csv_data(tmp_path):
    """Create a temporary CSV file with sample data."""
    path = tmp_path / "data.csv"
    with open(path, "w", newline="") as f:
        writer = csv_mod.writer(f)
        writer.writerow(["x", "y"])
        writer.writerows([[1, "a"], [2, "b"], [3, "c"]])
    return tmp_path


def _fs_io_config(root_dir: Path) -> IOConfig:
    """Create an IOConfig using OpenDAL's 'fs' (filesystem) backend."""
    return IOConfig(
        backends={
            "fs": {
                "root": str(root_dir),
            }
        }
    )


def test_opendal_fs_read_parquet(parquet_data):
    """Test reading a parquet file through the OpenDAL fs backend."""
    io_config = _fs_io_config(parquet_data)
    df = daft.read_parquet("fs://localhost/data.parquet", io_config=io_config)
    result = df.collect()
    assert result.to_pydict() == {"x": [1, 2, 3], "y": ["a", "b", "c"]}


def test_opendal_fs_read_csv(csv_data):
    """Test reading a CSV file through the OpenDAL fs backend."""
    io_config = _fs_io_config(csv_data)
    df = daft.read_csv("fs://localhost/data.csv", io_config=io_config)
    result = df.collect()
    assert result.to_pydict() == {"x": [1, 2, 3], "y": ["a", "b", "c"]}


def test_opendal_fs_glob_parquet(tmp_path):
    """Test globbing parquet files through the OpenDAL fs backend."""
    for i in range(3):
        table = pa.table({"val": [i]})
        papq.write_table(table, str(tmp_path / f"part_{i}.parquet"))

    io_config = _fs_io_config(tmp_path)
    df = daft.read_parquet("fs://localhost/*.parquet", io_config=io_config)
    result = df.sort("val").collect()
    assert result.to_pydict() == {"val": [0, 1, 2]}


def test_opendal_unconfigured_scheme_error():
    """Test that an unconfigured scheme gives a helpful error message."""
    with pytest.raises(Exception, match="Configure it via IOConfig"):
        daft.read_parquet("unknownscheme://bucket/data.parquet").collect()


def test_opendal_ioconfig_roundtrip():
    """Test that IOConfig with backends survives serialization roundtrip."""
    import pickle

    config = IOConfig(
        backends={
            "oss": {"bucket": "my-bucket", "access_key_id": "test"},
            "cos": {"bucket": "other-bucket"},
        }
    )

    restored = pickle.loads(pickle.dumps(config))
    assert restored.backends == config.backends
    assert hash(config) == hash(restored)


def test_opendal_ioconfig_replace():
    """Test that IOConfig.replace works with backends."""
    config = IOConfig(backends={"oss": {"bucket": "original"}})
    replaced = config.replace(backends={"cos": {"bucket": "new"}})

    assert replaced.backends == {"cos": {"bucket": "new"}}
    assert config.backends == {"oss": {"bucket": "original"}}


def test_opendal_ioconfig_default_empty_backends():
    """Test that default IOConfig has empty backends."""
    config = IOConfig()
    assert config.backends == {}
