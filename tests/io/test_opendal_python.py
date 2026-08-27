"""Tests for the Python-based OpenDAL fallback backend (daft[extra-fs]).

These tests exercise the `PythonOpenDALSource` Rust backend, which drives the
`opendal` Python package through its `AsyncOperator` API. They require the
`opendal` package to be installed (`pip install "daft[extra-fs]"`).
"""

from __future__ import annotations

import csv as csv_mod

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

pytest.importorskip("opendal")

from opendal import exceptions as opendal_exceptions

import daft
from daft.daft import IOConfig


def _fs_io_config(root_dir) -> IOConfig:
    """Create an IOConfig using OpenDAL's 'fs' (filesystem) backend."""
    return IOConfig(
        opendal_backends={
            "fs": {
                "root": str(root_dir),
            }
        }
    )


@pytest.fixture
def forced_python_fallback(monkeypatch):
    """Route OpenDAL schemes through the Python backend for this test.

    Counts calls into the Python bridge and asserts at teardown that the
    Python backend was actually exercised. This guards against
    DAFT_FORCE_PYTHON_OPENDAL silently failing to create the Python client
    (e.g. when the `opendal` package misbehaves) and the test degrading to
    the native path without anyone noticing.
    """
    monkeypatch.setenv("DAFT_FORCE_PYTHON_OPENDAL", "1")

    import daft.io.opendal_bridge as bridge

    calls = {"count": 0}
    real_call = bridge.call

    def counting_call(*args, **kwargs):
        calls["count"] += 1
        return real_call(*args, **kwargs)

    monkeypatch.setattr(bridge, "call", counting_call)
    yield calls
    assert calls["count"] > 0, "no bridge.call recorded: the Python OpenDAL backend was not exercised"


def test_python_fallback_read_parquet(tmp_path, forced_python_fallback):
    """Read a parquet file through the Python OpenDAL fallback."""
    table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    papq.write_table(table, str(tmp_path / "data.parquet"))

    io_config = _fs_io_config(tmp_path)
    df = daft.read_parquet("fs://localhost/data.parquet", io_config=io_config)
    assert df.collect().to_pydict() == {"x": [1, 2, 3], "y": ["a", "b", "c"]}


def test_python_fallback_read_csv(tmp_path, forced_python_fallback):
    """Read a CSV file through the Python OpenDAL fallback."""
    path = tmp_path / "data.csv"
    with open(path, "w", newline="") as f:
        writer = csv_mod.writer(f)
        writer.writerow(["x", "y"])
        writer.writerows([[1, "a"], [2, "b"], [3, "c"]])

    io_config = _fs_io_config(tmp_path)
    df = daft.read_csv("fs://localhost/data.csv", io_config=io_config)
    assert df.collect().to_pydict() == {"x": [1, 2, 3], "y": ["a", "b", "c"]}


def test_python_fallback_write_parquet(tmp_path, forced_python_fallback):
    """Write parquet through the Python fallback (buffered multipart emulation)."""
    io_config = _fs_io_config(tmp_path)
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    df.write_parquet("fs://localhost/out", io_config=io_config)

    result = daft.read_parquet("fs://localhost/out/*.parquet", io_config=io_config).sort("a").collect()
    assert result.to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}


def test_python_fallback_ls(tmp_path, forced_python_fallback):
    """List files through the Python OpenDAL fallback."""
    for i in range(3):
        table = pa.table({"val": [i]})
        papq.write_table(table, str(tmp_path / f"part_{i}.parquet"))

    io_config = _fs_io_config(tmp_path)
    df = daft.read_parquet("fs://localhost/*.parquet", io_config=io_config)
    assert df.sort("val").collect().to_pydict() == {"val": [0, 1, 2]}


def test_python_fallback_get_range(tmp_path, forced_python_fallback):
    """Exercise ranged reads (parquet footers) through the Python fallback."""
    io_config = _fs_io_config(tmp_path)
    table = pa.table({"x": [42]})
    papq.write_table(table, str(tmp_path / "r.parquet"))

    df = daft.read_parquet("fs://localhost/r.parquet", io_config=io_config)
    assert df.collect().to_pydict() == {"x": [42]}


def test_opendal_pyarrow_filesystem_memory_roundtrip():
    """The pyarrow wrapper (OpenDALFileSystem) works against the memory backend."""
    from daft.io.opendal_filesystem import OpenDALFileSystem

    fs = OpenDALFileSystem(protocol="memory")

    with fs.open_output_stream("data.txt") as out:
        out.write(b"hello opendal")

    info = fs.get_file_info("data.txt")
    assert info.size == 13

    with fs.open_input_stream("data.txt") as inp:
        assert inp.read() == b"hello opendal"

    fs.delete_file("data.txt")
    with pytest.raises(opendal_exceptions.NotFound):
        fs.get_file_info("data.txt")


def test_opendal_pyarrow_filesystem_missing_package(monkeypatch):
    """A helpful error is raised when the opendal package is not installed."""
    import builtins

    from daft.io.opendal_filesystem import OpenDALFileSystem

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "opendal":
            raise ImportError("No module named 'opendal'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match="extra-fs"):
        OpenDALFileSystem(protocol="oss")


def test_python_fallback_unconfigured_service_error_message():
    """Installed-but-unconfigured services point at IOConfig, not installation."""
    with pytest.raises(Exception, match="IOConfig\\(opendal_backends"):
        daft.read_parquet("oss://some-bucket/key.parquet").collect()


def test_hdfs_not_shipped_in_wheel_error_message():
    """`hdfs` is absent from the opendal wheel; point at the native feature."""
    with pytest.raises(Exception, match="--features native-hdfs"):
        daft.read_parquet("hdfs://namenode:9000/data.parquet").collect()
