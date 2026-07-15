import json

import pyarrow as pa
import pyarrow.parquet as pq

import daft
from daft.functions import st_point, st_x


def _geo_df():
    # st_point returns DataType::Geometry (WKB), which triggers GeoParquet metadata emission
    return daft.from_pydict({"id": [1, 2], "x": [1.0, 3.0], "y": [2.0, 4.0]}).select(
        daft.col("id"), st_point(daft.col("x"), daft.col("y")).alias("geom")
    )


def test_write_parquet_emits_geo_metadata(tmp_path):
    _geo_df().write_parquet(str(tmp_path))
    files = list(tmp_path.rglob("*.parquet"))
    assert files
    meta = pq.read_metadata(files[0]).metadata  # bytes-keyed dict of footer kv-metadata
    geo = json.loads(meta[b"geo"])
    assert geo["version"] == "1.1.0"
    assert geo["primary_column"] == "geom"
    assert geo["columns"]["geom"]["encoding"] == "WKB"


def test_write_parquet_no_geo_metadata_for_non_geometry(tmp_path):
    daft.from_pydict({"id": [1, 2], "x": [1.0, 3.0]}).write_parquet(str(tmp_path))
    files = list(tmp_path.rglob("*.parquet"))
    assert files
    meta = pq.read_metadata(files[0]).metadata  # bytes-keyed footer kv-metadata (or None)
    assert meta is None or b"geo" not in meta


def test_geoparquet_roundtrip(tmp_path):
    """GeoParquet round-trip: write Geometry, read back as Geometry and verify st_x values."""
    _geo_df().write_parquet(str(tmp_path))
    df = daft.read_parquet(str(tmp_path))
    assert df.schema()["geom"].dtype == daft.DataType.geometry(), (
        f"Expected Geometry dtype, got {df.schema()['geom'].dtype}"
    )
    out = df.select(st_x(daft.col("geom")).alias("px")).sort("px").to_pydict()
    assert out["px"] == [1.0, 3.0]


def test_geometry_false_keeps_binary(tmp_path):
    """geometry=False suppresses geo re-typing; WKB column stays as Binary."""
    _geo_df().write_parquet(str(tmp_path))
    df = daft.read_parquet(str(tmp_path), geometry=False)
    assert df.schema()["geom"].dtype == daft.DataType.binary(), (
        f"Expected Binary dtype, got {df.schema()['geom'].dtype}"
    )
    out = df.to_pydict()
    assert all(isinstance(v, (bytes, bytearray)) for v in out["geom"])  # raw WKB bytes, not parsed geometry


def test_read_foreign_geoparquet(tmp_path):
    """Read a GeoParquet file authored by pyarrow (not Daft); Daft must auto-detect and re-type."""
    # WKB for POINT(1 2), little-endian: 01 01000000 + 8-byte x=1.0 + 8-byte y=2.0
    wkb = bytes.fromhex("0101000000000000000000F03F0000000000000040")
    tbl = pa.table({"geom": pa.array([wkb], type=pa.binary())})
    geo = {
        "version": "1.1.0",
        "primary_column": "geom",
        "columns": {"geom": {"encoding": "WKB", "geometry_types": []}},
    }
    tbl = tbl.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    path = tmp_path / "foreign.parquet"
    pq.write_table(tbl, path)
    df = daft.read_parquet(str(path))
    assert df.schema()["geom"].dtype == daft.DataType.geometry(), (
        f"Expected Geometry dtype from foreign GeoParquet, got {df.schema()['geom'].dtype}"
    )


def test_plain_parquet_unaffected(tmp_path):
    """Plain Parquet without 'geo' footer metadata is unaffected by GeoParquet detection."""
    daft.from_pydict({"a": [1, 2]}).write_parquet(str(tmp_path))
    df = daft.read_parquet(str(tmp_path))
    assert df.schema()["a"].dtype == daft.DataType.int64()


def test_python_helper_matches_rust_geo_json(tmp_path):
    """Python Delta helper must emit byte-identical 'geo' JSON to the Rust Parquet writer.

    This pins that build_geo_metadata() and the Rust writer stay in sync, so a
    GeoParquet → Delta → read round-trip stays consistent.
    """
    from daft.io._geoparquet import build_geo_metadata

    df = daft.from_pydict({"id": [1], "x": [1.0], "y": [2.0]}).select(
        daft.col("id"), st_point(daft.col("x"), daft.col("y")).alias("geom")
    )
    df.write_parquet(str(tmp_path))
    rust_geo = json.loads(pq.read_metadata(next(tmp_path.rglob("*.parquet"))).metadata[b"geo"])
    py_geo = json.loads(build_geo_metadata(df.schema()))
    assert py_geo == rust_geo, (
        f"Python helper JSON differs from Rust writer JSON.\n"
        f"  py_geo:   {py_geo}\n"
        f"  rust_geo: {rust_geo}"
    )
