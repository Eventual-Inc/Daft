"""Tests for the lineage introspection API."""

from __future__ import annotations

import daft

# Detect whether openlineage-python SDK is installed
try:
    from openlineage.client.event_v2 import Dataset as OLDataset
    from openlineage.client.event_v2 import Job as OLJob
    from openlineage.client.event_v2 import Run as OLRun

    _HAS_OL = True
except ImportError:
    _HAS_OL = False


def _get_namespace(obj):
    """Extract namespace from OL Dataset or plain dict."""
    if _HAS_OL and isinstance(obj, OLDataset):
        return obj.namespace
    return obj["namespace"]


def _get_name(obj):
    """Extract name from OL Dataset or plain dict."""
    if _HAS_OL and isinstance(obj, OLDataset):
        return obj.name
    return obj["name"]


def _get_schema_fields(obj):
    """Extract schema fields from OL Dataset or plain dict."""
    if _HAS_OL and isinstance(obj, OLDataset):
        schema = obj.facets.get("schema")
        if schema:
            return [{"name": f.name, "type": f.type} for f in schema.fields]
        return []
    facets = obj.get("facets", {})
    schema = facets.get("schema", {})
    return schema.get("fields", [])


def test_lineage_in_memory_source():
    """lineage() on an in-memory DataFrame should return empty inputs."""
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    info = df.lineage()
    assert "inputs" in info
    assert "outputs" in info
    assert "run" in info
    assert "job" in info
    # In-memory sources should not appear as inputs (no persistent identity)
    assert info["inputs"] == []
    assert info["outputs"] == []


def test_lineage_globscan_parquet(tmp_path):
    """lineage() should identify GlobScan inputs from read_parquet."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    data_path = tmp_path / "data"
    data_path.mkdir()
    tbl = pa.table({"x": [1, 2, 3]})
    pq.write_table(tbl, str(data_path / "part-0.parquet"))

    df = daft.read_parquet(str(data_path / "*.parquet"))
    info = df.lineage()
    assert len(info["inputs"]) == 1
    inp = info["inputs"][0]
    assert _get_namespace(inp) == "file"
    assert str(data_path) in _get_name(inp)
    schema_fields = _get_schema_fields(inp)
    assert any(f["name"] == "x" for f in schema_fields)


def test_lineage_with_select(tmp_path):
    """lineage() after select should still identify inputs."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    data_path = tmp_path / "data"
    data_path.mkdir()
    tbl = pa.table({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
    pq.write_table(tbl, str(data_path / "part-0.parquet"))

    df = daft.read_parquet(str(data_path / "*.parquet"))
    df = df.select(df["a"], df["b"])
    info = df.lineage()
    assert len(info["inputs"]) == 1
    inp = info["inputs"][0]
    schema_fields = _get_schema_fields(inp)
    assert any(f["name"] == "a" for f in schema_fields)
    assert any(f["name"] == "b" for f in schema_fields)


def test_lineage_with_write(tmp_path):
    """lineage() should identify Sink outputs from a write plan builder."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from daft.daft import FileFormat, PyFormatSinkOption, WriteMode
    from daft.dataframe.dataframe import DataFrame

    data_path = tmp_path / "data"
    data_path.mkdir()
    out_path = tmp_path / "output"
    out_path.mkdir()

    tbl = pa.table({"x": [1, 2, 3]})
    pq.write_table(tbl, str(data_path / "part-0.parquet"))

    df = daft.read_parquet(str(data_path / "*.parquet"))
    builder = df._builder.write_tabular(
        root_dir=str(out_path),
        partition_cols=None,
        write_mode=WriteMode.from_str("append"),
        write_success_file=True,
        file_format=FileFormat.Parquet,
        file_format_option=PyFormatSinkOption.parquet(),
        compression=None,
        io_config=None,
    )
    write_df = DataFrame(builder)
    info = write_df.lineage()
    assert len(info["outputs"]) >= 1
    out = info["outputs"][0]
    assert _get_namespace(out) == "file"
    assert str(out_path) in _get_name(out)


def test_lineage_has_unique_run_id():
    """Each call to lineage() should produce a unique run ID."""
    df = daft.from_pydict({"a": [1]})
    info1 = df.lineage()
    info2 = df.lineage()

    if _HAS_OL:
        assert info1["run"].runId != info2["run"].runId
    else:
        assert info1["run"] != info2["run"]


def test_lineage_with_filter(tmp_path):
    """lineage() after filter should still find inputs."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    data_path = tmp_path / "data"
    data_path.mkdir()
    tbl = pa.table({"x": [1, 2, 3, 4]})
    pq.write_table(tbl, str(data_path / "part-0.parquet"))

    df = daft.read_parquet(str(data_path / "*.parquet"))
    df = df.where(df["x"] > 2)
    info = df.lineage()
    assert len(info["inputs"]) == 1


def test_lineage_job_info():
    """lineage() should return correct job info."""
    df = daft.from_pydict({"a": [1]})
    info = df.lineage()
    if _HAS_OL:
        assert isinstance(info["job"], OLJob)
        assert info["job"].namespace == "daft"
        assert info["job"].name == "daft_query"
    else:
        assert info["job"]["namespace"] == "daft"
        assert info["job"]["name"] == "daft_query"


def test_lineage_returns_ol_objects_when_sdk_installed():
    """When openlineage-python is installed, lineage() should return OL objects."""
    if not _HAS_OL:
        import pytest

        pytest.skip("openlineage-python not installed")

    import tempfile
    from pathlib import Path

    import pyarrow as pa
    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory() as tmp:
        data_path = Path(tmp) / "data"
        data_path.mkdir()
        tbl = pa.table({"x": [1, 2, 3]})
        pq.write_table(tbl, str(data_path / "part-0.parquet"))

        df = daft.read_parquet(str(data_path / "*.parquet"))
        info = df.lineage()

        assert isinstance(info["inputs"][0], OLDataset)
        assert isinstance(info["run"], OLRun)
        assert isinstance(info["job"], OLJob)
