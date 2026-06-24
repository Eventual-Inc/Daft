import json

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
