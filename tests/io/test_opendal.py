"""Integration tests for OpenDAL backend support via IOConfig(opendal_backends={...})."""

from __future__ import annotations

import csv as csv_mod
import pickle
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft.daft import HdfsConfig, IOConfig


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
        opendal_backends={
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
    with pytest.raises(Exception, match="IOConfig\\(opendal_backends="):
        daft.read_parquet("unknownscheme://bucket/data.parquet").collect()


def test_hdfs_config_defaults():
    """Test HdfsConfig default values."""
    cfg = HdfsConfig()
    assert cfg.name_node is None
    assert cfg.root is None


def test_hdfs_config_explicit():
    """Test HdfsConfig with explicit name_node and root."""
    cfg = HdfsConfig(name_node="hdfs://namenode:9000", root="/data")
    assert cfg.name_node == "hdfs://namenode:9000"
    assert cfg.root == "/data"


def test_hdfs_ioconfig_integration():
    """Test that HdfsConfig can be passed to IOConfig and roundtrips."""
    cfg = IOConfig(hdfs=HdfsConfig(name_node="hdfs://nn:9000", root="/warehouse"))
    assert cfg.hdfs.name_node == "hdfs://nn:9000"
    assert cfg.hdfs.root == "/warehouse"

    # Default HdfsConfig in default IOConfig
    default_cfg = IOConfig()
    assert default_cfg.hdfs.name_node is None
    assert default_cfg.hdfs.root is None


def test_hdfs_ioconfig_replace():
    """Test IOConfig.replace with HdfsConfig."""
    original = IOConfig()
    assert original.hdfs.name_node is None

    replaced = original.replace(hdfs=HdfsConfig(name_node="hdfs://nn:9000"))
    assert replaced.hdfs.name_node == "hdfs://nn:9000"
    # Original unchanged
    assert original.hdfs.name_node is None


def test_hdfs_ioconfig_pickle():
    """Test IOConfig with HdfsConfig survives pickle roundtrip."""
    cfg = IOConfig(hdfs=HdfsConfig(name_node="hdfs://nn:9000", root="/tmp"))
    restored = pickle.loads(pickle.dumps(cfg))
    assert restored.hdfs.name_node == "hdfs://nn:9000"
    assert restored.hdfs.root == "/tmp"
    assert hash(cfg) == hash(restored)


def test_hdfs_url_parsing():
    """Test that an hdfs:// URL is recognized without explicit IOConfig.

    This test does not require a running HDFS cluster — it verifies that
    the scheme is routed to the OpenDAL HDFS backend (error message will
    mention 'hdfs' in the list of available schemes if the JVM / Hadoop
    classpath is not available).
    """
    with pytest.raises(Exception) as exc_info:
        daft.read_parquet("hdfs://localhost:9000/data.parquet").collect()
    msg = str(exc_info.value)
    # Either the error is about HDFS connection (JVM available) or about
    # the scheme being available in OpenDAL backends.
    assert "hdfs" in msg.lower()


def test_opendal_ioconfig_roundtrip():
    """Test that IOConfig with opendal_backends survives serialization roundtrip."""
    config = IOConfig(
        opendal_backends={
            "oss": {"bucket": "my-bucket", "access_key_id": "test"},
            "cos": {"bucket": "other-bucket"},
        }
    )

    restored = pickle.loads(pickle.dumps(config))
    assert restored.opendal_backends == config.opendal_backends
    assert hash(config) == hash(restored)


def test_opendal_ioconfig_replace():
    """Test that IOConfig.replace works with opendal_backends."""
    config = IOConfig(opendal_backends={"oss": {"bucket": "original"}})
    replaced = config.replace(opendal_backends={"cos": {"bucket": "new"}})

    assert replaced.opendal_backends == {"cos": {"bucket": "new"}}
    assert config.opendal_backends == {"oss": {"bucket": "original"}}


def test_opendal_fs_write_parquet(tmp_path):
    """Test writing a parquet file through the OpenDAL fs backend and reading it back."""
    io_config = _fs_io_config(tmp_path)
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    df.write_parquet("fs://localhost/out", io_config=io_config)

    result = daft.read_parquet("fs://localhost/out/*.parquet", io_config=io_config).sort("a").collect()
    assert result.to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}


def test_opendal_fs_write_csv(tmp_path):
    """Test writing CSV files through the OpenDAL fs backend and reading them back."""
    io_config = _fs_io_config(tmp_path)
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    df.write_csv("fs://localhost/out", io_config=io_config)

    result = daft.read_csv("fs://localhost/out/*.csv", io_config=io_config).sort("a").collect()
    assert result.to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}


def test_opendal_fs_roundtrip_parquet_multiple_columns(tmp_path):
    """Roundtrip parquet with ints, floats, strings, bools, and nulls."""
    io_config = _fs_io_config(tmp_path)
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "value": [1.5, 2.5, 3.5],
            "label": ["foo", "bar", "baz"],
            "flag": [True, False, True],
            "nullable": [10, None, 30],
        }
    )
    df.write_parquet("fs://localhost/out", io_config=io_config)

    result = daft.read_parquet("fs://localhost/out/*.parquet", io_config=io_config).sort("id").collect()
    assert result.to_pydict() == {
        "id": [1, 2, 3],
        "value": [1.5, 2.5, 3.5],
        "label": ["foo", "bar", "baz"],
        "flag": [True, False, True],
        "nullable": [10, None, 30],
    }


def test_opendal_fs_roundtrip_csv_multiple_columns(tmp_path):
    """Roundtrip CSV with ints, floats, and strings."""
    io_config = _fs_io_config(tmp_path)
    df = daft.from_pydict(
        {
            "id": [1, 2, 3],
            "value": [1.5, 2.5, 3.5],
            "label": ["foo", "bar", "baz"],
        }
    )
    df.write_csv("fs://localhost/out", io_config=io_config)

    result = daft.read_csv("fs://localhost/out/*.csv", io_config=io_config).sort("id").collect()
    assert result.to_pydict() == {
        "id": [1, 2, 3],
        "value": [1.5, 2.5, 3.5],
        "label": ["foo", "bar", "baz"],
    }


def test_opendal_fs_roundtrip_parquet_empty(tmp_path):
    """Roundtrip an empty dataframe through parquet."""
    io_config = _fs_io_config(tmp_path)
    df = daft.from_pydict({"x": [], "y": []}).with_columns(
        {
            "x": daft.col("x").cast(daft.DataType.int64()),
            "y": daft.col("y").cast(daft.DataType.string()),
        }
    )
    df.write_parquet("fs://localhost/out", io_config=io_config)

    result = daft.read_parquet("fs://localhost/out/*.parquet", io_config=io_config).collect()
    assert result.to_pydict() == {"x": [], "y": []}


def test_opendal_fs_roundtrip_parquet_partitioned(tmp_path):
    """Roundtrip parquet with partition_cols produces Hive-partitioned output."""
    io_config = _fs_io_config(tmp_path)
    df = daft.from_pydict(
        {
            "group": ["a", "a", "b", "b"],
            "val": [1, 2, 3, 4],
        }
    )
    df.write_parquet("fs://localhost/out", partition_cols=["group"], io_config=io_config)

    result = (
        daft.read_parquet("fs://localhost/out/**/*.parquet", io_config=io_config, hive_partitioning=True)
        .sort("val")
        .collect()
    )
    assert result.to_pydict() == {"val": [1, 2, 3, 4], "group": ["a", "a", "b", "b"]}


def test_opendal_fs_roundtrip_parquet_large(tmp_path):
    """Roundtrip a larger dataset to exercise multipart buffering."""
    io_config = _fs_io_config(tmp_path)
    n = 10_000
    df = daft.from_pydict(
        {
            "id": list(range(n)),
            "data": [f"row-{i}" for i in range(n)],
        }
    )
    df.write_parquet("fs://localhost/out", io_config=io_config)

    result = daft.read_parquet("fs://localhost/out/*.parquet", io_config=io_config).sort("id").collect()
    out = result.to_pydict()
    assert out["id"] == list(range(n))
    assert out["data"] == [f"row-{i}" for i in range(n)]


def test_opendal_ioconfig_default_empty_opendal_backends():
    """Test that default IOConfig has empty opendal_backends."""
    config = IOConfig()
    assert config.opendal_backends == {}
