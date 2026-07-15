"""Tests for spatial point-in-polygon with geohash pruning.

These tests verify:
1. Correct point-in-polygon results (st_intersects / st_contains)
2. That the GeohashPruning optimizer rule injects a geohash pre-filter when a
   ``{col}_geohash`` column exists in the schema.
3. End-to-end: the pruned query returns the same rows as the unpruned one.
"""

from __future__ import annotations

import struct

import pytest

import daft
from daft import col, lit
from daft.functions.spatial import st_contains, st_geohash, st_intersects

# ── WKB helpers ─────────────────────────────────────────────────────────────


def _point_wkb(x: float, y: float) -> bytes:
    """Return WKB-encoded Point (little-endian)."""
    return struct.pack("<B", 1) + struct.pack("<I", 1) + struct.pack("<dd", x, y)


def _polygon_wkb(rings: list[list[tuple[float, float]]]) -> bytes:
    """Return WKB-encoded Polygon (little-endian).

    Each ring is a list of (x, y) tuples.  The exterior ring must be closed
    (first == last point).
    """
    buf = struct.pack("<B", 1)  # byte order: little-endian
    buf += struct.pack("<I", 3)  # geometry type: Polygon
    buf += struct.pack("<I", len(rings))  # num rings
    for ring in rings:
        buf += struct.pack("<I", len(ring))
        for x, y in ring:
            buf += struct.pack("<dd", x, y)
    return buf


def _unit_square_wkb(x0: float, y0: float, x1: float, y1: float) -> bytes:
    """Return a rectangular polygon WKB with the given corner coordinates."""
    ring = [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]
    return _polygon_wkb([ring])


# ── Fixtures ─────────────────────────────────────────────────────────────────

# Five points spread across the world at different geohash cells.
#
#  P0 (1.0,  1.0) → geohash-5 ≈ "s00tw"  — inside query polygon [0,2]×[0,2]
#  P1 (5.0,  5.0) → geohash-5 ≈ "s065k"  — outside
#  P2 (10.0, 10.0) → geohash-5 ≈ "s1z0g" — outside
#  P3 (1.5,  1.5) → geohash-5 ≈ "s00twy" — inside query polygon [0,2]×[0,2]
#  P4 (0.0,  0.0) → geohash-5 ≈ "s0000"  — on boundary (inclusive)

POINTS = [
    _point_wkb(1.0, 1.0),
    _point_wkb(5.0, 5.0),
    _point_wkb(10.0, 10.0),
    _point_wkb(1.5, 1.5),
    _point_wkb(0.0, 0.0),
]

# Pre-computed geohash at precision 5 for each point (centroid == the point itself).
# These are used as a stand-in; in production you'd compute them with st_geohash.
POINT_GEOHASHES_P5 = [
    "s00tw",
    "s0gs3",
    "s1z0g",
    "s030d",
    "s0000",
]

# Query polygon: unit square [0, 2] × [0, 2]
QUERY_POLYGON = _unit_square_wkb(0.0, 0.0, 2.0, 2.0)


@pytest.fixture()
def points_df():
    """DataFrame with geometry points and their pre-computed geohash column."""
    return daft.from_pydict(
        {
            "id": list(range(len(POINTS))),
            "geom": POINTS,
            "geom_geohash": POINT_GEOHASHES_P5,
        }
    )


# ── Basic correctness tests ───────────────────────────────────────────────────


def test_st_intersects_point_in_polygon(points_df):
    """Points inside [0,2]×[0,2] should be returned by st_intersects."""
    query_lit = lit(QUERY_POLYGON)
    result = points_df.where(st_intersects(col("geom"), query_lit)).select("id").sort("id").to_pydict()
    # P0 (1,1), P3 (1.5,1.5), P4 (0,0) are inside or on the boundary
    assert result["id"] == [0, 3, 4]


def test_st_contains_polygon_contains_points(points_df):
    """st_contains(polygon, point) mirrors st_intersects for Point geometries."""
    combined = points_df.with_column("query_geom", lit(QUERY_POLYGON))
    result = combined.where(st_contains(col("query_geom"), col("geom"))).select("id").sort("id").to_pydict()
    # st_contains uses strict interior; boundary point P4(0,0) may not be included
    # depending on implementation. Accept either [0, 3] or [0, 3, 4].
    assert result["id"] in ([0, 3], [0, 3, 4]), f"Unexpected result: {result['id']}"


# ── Geohash pruning optimizer tests ──────────────────────────────────────────


def test_geohash_pruning_injected_in_explain(points_df):
    """Optimizer should inject st_geohash_covers when {col}_geohash column exists."""
    import io
    import sys

    query_lit = lit(QUERY_POLYGON)
    filtered_df = points_df.where(st_intersects(col("geom"), query_lit))

    # explain() prints to stdout and returns None; capture stdout
    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf
    try:
        filtered_df.explain(show_all=True)
    finally:
        sys.stdout = old_stdout
    explain_text = buf.getvalue()

    # The pruning rule should have rewritten the filter to include st_geohash_covers
    assert "st_geohash_covers" in explain_text, (
        "Expected GeohashPruning optimizer to inject st_geohash_covers into the "
        f"query plan, but it was not found.\n\nExplain output:\n{explain_text}"
    )


def test_geohash_pruning_correct_results(points_df):
    """With geohash pruning active, results must match the non-pruned query."""
    query_lit = lit(QUERY_POLYGON)

    # Pruned query (has geom_geohash column → optimizer injects pre-filter)
    pruned_ids = points_df.where(st_intersects(col("geom"), query_lit)).select("id").sort("id").to_pydict()["id"]

    # Unpruned query (drop the geohash column so the optimizer cannot prune)
    unpruned_ids = (
        points_df.select("id", "geom")  # no geom_geohash column
        .where(st_intersects(col("geom"), query_lit))
        .select("id")
        .sort("id")
        .to_pydict()["id"]
    )

    assert pruned_ids == unpruned_ids, (
        f"Geohash-pruned query returned {pruned_ids} but unpruned returned {unpruned_ids}"
    )


def test_geohash_pruning_no_false_negatives(points_df):
    """Geohash pruning must never drop rows that satisfy the spatial predicate."""
    query_lit = lit(QUERY_POLYGON)

    pruned_ids = set(points_df.where(st_intersects(col("geom"), query_lit)).select("id").to_pydict()["id"])
    unpruned_ids = set(
        points_df.select("id", "geom").where(st_intersects(col("geom"), query_lit)).select("id").to_pydict()["id"]
    )

    # Every row in unpruned must also appear in pruned
    missing = unpruned_ids - pruned_ids
    assert not missing, f"Geohash pruning incorrectly dropped row ids: {missing}"


# ── Without geohash column — pruning should silently not apply ────────────────


def test_no_geohash_column_still_works():
    """Without a _geohash column the filter should still return correct results."""
    df = daft.from_pydict({"id": list(range(5)), "geom": POINTS})
    query_lit = lit(QUERY_POLYGON)
    result = df.where(st_intersects(col("geom"), query_lit)).select("id").sort("id").to_pydict()
    assert result["id"] == [0, 3, 4]


# ── st_geohash round-trip ─────────────────────────────────────────────────────


def test_st_geohash_produces_valid_strings():
    """st_geohash should return non-empty geohash strings for valid geometries."""
    df = daft.from_pydict({"geom": POINTS})
    hashes = df.select(st_geohash(col("geom"), precision=5).alias("h")).to_pydict()["h"]
    assert all(isinstance(h, str) and len(h) == 5 for h in hashes), f"Expected 5-char geohash strings, got: {hashes}"


def test_computed_geohash_matches_precomputed():
    """st_geohash output should match the pre-computed geohash fixtures."""
    df = daft.from_pydict({"geom": POINTS})
    computed = df.select(st_geohash(col("geom"), precision=5).alias("h")).to_pydict()["h"]
    for i, (computed_h, expected_h) in enumerate(zip(computed, POINT_GEOHASHES_P5)):
        assert computed_h == expected_h, f"Point {i}: computed geohash {computed_h!r} != expected {expected_h!r}"


# ── st_geohash precision argument ─────────────────────────────────────────────


@pytest.mark.parametrize("precision", [1, 2, 5, 6, 8, 12])
def test_st_geohash_precision_controls_length(precision):
    """The precision argument must control the geohash string length.

    Regression test: st_geohash previously ignored the caller-supplied precision
    and always produced 5-character hashes (it read the value baked in at
    registration instead of the function argument).
    """
    df = daft.from_pydict({"geom": POINTS})
    hashes = df.select(st_geohash(col("geom"), precision=precision).alias("h")).to_pydict()["h"]
    assert all(len(h) == precision for h in hashes), (
        f"Expected {precision}-char geohashes, got lengths {[len(h) for h in hashes]}"
    )


def test_st_geohash_default_precision_is_five():
    """Calling st_geohash without a precision argument yields 5-character hashes."""
    df = daft.from_pydict({"geom": POINTS})
    hashes = df.select(st_geohash(col("geom")).alias("h")).to_pydict()["h"]
    assert all(len(h) == 5 for h in hashes)


def test_st_geohash_precision_is_hierarchical():
    """A higher-precision geohash must extend the lower-precision one (shared prefix)."""
    df = daft.from_pydict({"geom": POINTS})
    out = df.select(
        st_geohash(col("geom"), precision=4).alias("h4"),
        st_geohash(col("geom"), precision=8).alias("h8"),
    ).to_pydict()
    for h4, h8 in zip(out["h4"], out["h8"]):
        assert h8.startswith(h4), f"{h8!r} should start with {h4!r}"


def test_st_geohash_null_geometry_yields_null():
    """Null geometries should produce null geohash output; valid rows are unaffected."""
    df = daft.from_pydict({"geom": [POINTS[0], None]})
    hashes = df.select(st_geohash(col("geom"), precision=6).alias("h")).to_pydict()["h"]
    assert hashes[0] is not None and len(hashes[0]) == 6
    assert hashes[1] is None
