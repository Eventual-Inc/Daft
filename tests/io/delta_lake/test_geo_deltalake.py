"""Round-trip test: Geometry columns survive a write→read cycle through Delta Lake."""

from __future__ import annotations

import pytest

import daft
from daft.functions import st_astext, st_point


def test_deltalake_geo_roundtrip(tmp_path):
    """Write a DataFrame with a Geometry column to Delta Lake, read it back, and verify the dtype is preserved."""
    pytest.importorskip("deltalake")

    df = daft.from_pydict({"id": [1, 2, 3], "x": [1.0, 2.0, 3.0], "y": [4.0, 5.0, 6.0]}).select(
        daft.col("id"),
        st_point(daft.col("x"), daft.col("y")).alias("geom"),
    )

    # Verify source dtype is Geometry
    assert df.schema()["geom"].dtype == daft.DataType.geometry(), (
        f"Expected Geometry source dtype, got {df.schema()['geom'].dtype}"
    )

    # Write to Delta Lake
    df.write_deltalake(str(tmp_path))

    # Read back and verify Geometry dtype is preserved
    back = daft.read_deltalake(str(tmp_path))
    assert back.schema()["geom"].dtype == daft.DataType.geometry(), (
        f"Expected Geometry dtype after round-trip, got {back.schema()['geom'].dtype}"
    )

    # Verify the geometry data is usable (st_astext returns WKT)
    out = back.select(daft.col("id"), st_astext(daft.col("geom")).alias("wkt")).sort("id").to_pydict()
    assert len(out["wkt"]) == 3
    for wkt in out["wkt"]:
        assert wkt is not None
        assert wkt.upper().startswith("POINT"), f"Expected POINT WKT, got: {wkt}"


def test_deltalake_geo_roundtrip_non_geo_table_unaffected(tmp_path):
    """Non-geometry Delta tables should be unaffected by geo round-trip logic."""
    pytest.importorskip("deltalake")

    df = daft.from_pydict({"id": [1, 2], "value": ["a", "b"]})
    df.write_deltalake(str(tmp_path))

    back = daft.read_deltalake(str(tmp_path))
    assert back.schema()["id"].dtype == daft.DataType.int64()
    assert back.schema()["value"].dtype == daft.DataType.string()
    result = back.to_pydict()
    assert result["id"] == [1, 2]
    assert result["value"] == ["a", "b"]


def test_deltalake_geo_roundtrip_append(tmp_path):
    """Geometry dtype survives an append to an existing geo Delta table (field metadata inherited from the table schema)."""
    pytest.importorskip("deltalake")

    df1 = daft.from_pydict({"id": [1, 2], "x": [1.0, 2.0], "y": [4.0, 5.0]}).select(
        daft.col("id"),
        st_point(daft.col("x"), daft.col("y")).alias("geom"),
    )
    df1.write_deltalake(str(tmp_path))

    df2 = daft.from_pydict({"id": [3], "x": [3.0], "y": [6.0]}).select(
        daft.col("id"),
        st_point(daft.col("x"), daft.col("y")).alias("geom"),
    )
    df2.write_deltalake(str(tmp_path), mode="append")

    back = daft.read_deltalake(str(tmp_path))
    assert back.schema()["geom"].dtype == daft.DataType.geometry(), (
        f"Expected Geometry dtype after append, got {back.schema()['geom'].dtype}"
    )
    out = back.select(daft.col("id"), st_astext(daft.col("geom")).alias("wkt")).sort("id").to_pydict()
    assert out["id"] == [1, 2, 3]
    for wkt in out["wkt"]:
        assert wkt is not None and wkt.upper().startswith("POINT")
