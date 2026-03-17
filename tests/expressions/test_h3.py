from __future__ import annotations

import pytest

import daft
from daft import col
from daft.functions.h3 import (
    h3_cell_is_valid,
    h3_cell_parent,
    h3_cell_resolution,
    h3_cell_to_lat,
    h3_cell_to_lng,
    h3_cell_to_str,
    h3_grid_distance,
    h3_latlng_to_cell,
    h3_str_to_cell,
)

# Known test vector: San Francisco (37.7749, -122.4194) at resolution 7
SF_LAT = 37.7749
SF_LNG = -122.4194
SF_RES7_HEX = "872830828ffffff"


class TestH3LatLngToCell:
    def test_basic(self):
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        result = df.select(h3_latlng_to_cell(col("lat"), col("lng"), 7)).to_pydict()
        cell = result["lat"][0]
        assert cell is not None
        assert isinstance(cell, int)

    def test_multiple_rows(self):
        df = daft.from_pydict(
            {
                "lat": [SF_LAT, 48.8566, 0.0],
                "lng": [SF_LNG, 2.3522, 0.0],
            }
        )
        result = df.select(h3_latlng_to_cell(col("lat"), col("lng"), 7)).to_pydict()
        assert len(result["lat"]) == 3
        assert all(v is not None for v in result["lat"])

    def test_null_handling(self):
        df = daft.from_pydict({"lat": [SF_LAT, None], "lng": [SF_LNG, -122.0]})
        result = df.select(h3_latlng_to_cell(col("lat"), col("lng"), 7)).to_pydict()
        assert result["lat"][0] is not None
        assert result["lat"][1] is None

    def test_resolution_range(self):
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        for res in [0, 7, 15]:
            result = df.select(h3_latlng_to_cell(col("lat"), col("lng"), res)).to_pydict()
            assert result["lat"][0] is not None


class TestH3CellToLatLng:
    def test_roundtrip_uint64(self):
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        df = df.with_column("cell", h3_latlng_to_cell(col("lat"), col("lng"), 7))
        result = df.select(
            h3_cell_to_lat(col("cell")).alias("out_lat"),
            h3_cell_to_lng(col("cell")).alias("out_lng"),
        ).to_pydict()
        assert pytest.approx(result["out_lat"][0], abs=0.01) == SF_LAT
        assert pytest.approx(result["out_lng"][0], abs=0.01) == SF_LNG

    def test_roundtrip_string(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = df.select(
            h3_cell_to_lat(col("hex")).alias("lat"),
            h3_cell_to_lng(col("hex")).alias("lng"),
        ).to_pydict()
        assert pytest.approx(result["lat"][0], abs=0.01) == SF_LAT
        assert pytest.approx(result["lng"][0], abs=0.01) == SF_LNG

    def test_null_handling(self):
        df = daft.from_pydict({"cell": [None]}).with_column("cell", col("cell").cast(daft.DataType.uint64()))
        result = df.select(h3_cell_to_lat(col("cell"))).to_pydict()
        assert result["cell"][0] is None

    def test_invalid_cell(self):
        df = daft.from_pydict({"cell": [0, 1, 999]}).with_column("cell", col("cell").cast(daft.DataType.uint64()))
        result = df.select(h3_cell_to_lat(col("cell"))).to_pydict()
        assert all(v is None for v in result["cell"])

    def test_invalid_string(self):
        df = daft.from_pydict({"cell": ["not_valid", ""]})
        result = df.select(h3_cell_to_lat(col("cell"))).to_pydict()
        assert all(v is None for v in result["cell"])


class TestH3CellStr:
    def test_cell_to_str_from_uint64(self):
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        df = df.with_column("cell", h3_latlng_to_cell(col("lat"), col("lng"), 7))
        result = df.select(h3_cell_to_str(col("cell")).alias("hex")).to_pydict()
        hex_str = result["hex"][0]
        assert hex_str is not None
        assert isinstance(hex_str, str)
        assert len(hex_str) == 15

    def test_cell_to_str_from_string(self):
        # Passing a string through cell_to_str normalizes it
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = df.select(h3_cell_to_str(col("hex")).alias("out")).to_pydict()
        assert result["out"][0] == SF_RES7_HEX

    def test_str_to_cell_roundtrip(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.with_column("cell", h3_str_to_cell(col("hex")))
        df = df.with_column("back", h3_cell_to_str(col("cell")))
        result = df.to_pydict()
        assert result["back"][0] == SF_RES7_HEX

    def test_str_to_cell_invalid(self):
        df = daft.from_pydict({"hex": ["not_a_cell", "", None]})
        result = df.select(h3_str_to_cell(col("hex"))).to_pydict()
        assert all(v is None for v in result["hex"])


class TestH3CellInfo:
    def test_resolution_uint64(self):
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        df = df.with_column("cell", h3_latlng_to_cell(col("lat"), col("lng"), 7))
        result = df.select(h3_cell_resolution(col("cell")).alias("res")).to_pydict()
        assert result["res"][0] == 7

    def test_resolution_string(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = df.select(h3_cell_resolution(col("hex")).alias("res")).to_pydict()
        assert result["res"][0] == 7

    def test_is_valid_uint64(self):
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        df = df.with_column("cell", h3_latlng_to_cell(col("lat"), col("lng"), 7))
        result = df.select(h3_cell_is_valid(col("cell")).alias("valid")).to_pydict()
        assert result["valid"][0] is True

    def test_is_valid_string(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX, "not_valid", None]})
        result = df.select(h3_cell_is_valid(col("hex"))).to_pydict()
        assert result["hex"][0] is True
        assert result["hex"][1] is False
        assert result["hex"][2] is None

    def test_is_valid_invalid_uint64(self):
        df = daft.from_pydict({"cell": [0, 42, 999999]}).with_column("cell", col("cell").cast(daft.DataType.uint64()))
        result = df.select(h3_cell_is_valid(col("cell"))).to_pydict()
        assert all(v is False for v in result["cell"])


class TestH3CellParent:
    def test_parent_uint64(self):
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        df = df.with_column("cell", h3_latlng_to_cell(col("lat"), col("lng"), 7))
        df = df.with_column("parent", h3_cell_parent(col("cell"), 5))
        result = df.select(h3_cell_resolution(col("parent")).alias("res")).to_pydict()
        assert result["res"][0] == 5

    def test_parent_string(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        df = df.with_column("parent", h3_cell_parent(col("hex"), 5))
        result = df.to_pydict()
        # String in -> string out
        assert isinstance(result["parent"][0], str)
        # And it's a valid hex string at the right resolution
        parent_df = daft.from_pydict({"p": result["parent"]})
        res = parent_df.select(h3_cell_resolution(col("p")).alias("res")).to_pydict()
        assert res["res"][0] == 5

    def test_parent_null_on_finer_resolution(self):
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        df = df.with_column("cell", h3_latlng_to_cell(col("lat"), col("lng"), 3))
        result = df.select(h3_cell_parent(col("cell"), 5)).to_pydict()
        assert result["cell"][0] is None


class TestH3GridDistance:
    def test_same_cell(self):
        df = daft.from_pydict({"lat": [SF_LAT], "lng": [SF_LNG]})
        df = df.with_column("cell", h3_latlng_to_cell(col("lat"), col("lng"), 7))
        result = df.select(h3_grid_distance(col("cell"), col("cell")).alias("dist")).to_pydict()
        assert result["dist"][0] == 0

    def test_same_cell_string(self):
        df = daft.from_pydict({"hex": [SF_RES7_HEX]})
        result = df.select(h3_grid_distance(col("hex"), col("hex")).alias("dist")).to_pydict()
        assert result["dist"][0] == 0

    def test_neighbors(self):
        df = daft.from_pydict(
            {
                "lat_a": [SF_LAT],
                "lng_a": [SF_LNG],
                "lat_b": [SF_LAT + 0.001],
                "lng_b": [SF_LNG],
            }
        )
        df = df.with_column("a", h3_latlng_to_cell(col("lat_a"), col("lng_a"), 7))
        df = df.with_column("b", h3_latlng_to_cell(col("lat_b"), col("lng_b"), 7))
        result = df.select(h3_grid_distance(col("a"), col("b")).alias("dist")).to_pydict()
        dist = result["dist"][0]
        assert dist is not None
        assert dist >= 0

    def test_null_handling(self):
        df = (
            daft.from_pydict({"a": [None], "b": [None]})
            .with_column("a", col("a").cast(daft.DataType.uint64()))
            .with_column("b", col("b").cast(daft.DataType.uint64()))
        )
        result = df.select(h3_grid_distance(col("a"), col("b")).alias("dist")).to_pydict()
        assert result["dist"][0] is None
