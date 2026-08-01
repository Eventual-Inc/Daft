"""Tests for the SpatialBboxPruning optimizer rule.

When a filter applies a spatial predicate between a geometry column and a
literal query geometry, and the schema carries `rtree_min_x/rtree_min_y/
rtree_max_x/rtree_max_y` Float64 columns, the rule ANDs in plain float
comparisons against the query geometry's MBR so ordinary filter pushdown +
parquet min/max stats can prune data.

The bbox columns are TRUSTED (same contract as the spatial join's bbox fast
path): deliberately-wrong bbox values dropping matching rows is how we prove
end-to-end that the rewrite actually fires.
"""

from __future__ import annotations

import struct

import daft
from daft import col, lit
from daft.functions.spatial import st_contains, st_dwithin, st_intersects


def _point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<B", 1) + struct.pack("<I", 1) + struct.pack("<dd", x, y)


def _rect_wkb(x0: float, y0: float, x1: float, y1: float) -> bytes:
    ring = [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]
    buf = struct.pack("<B", 1) + struct.pack("<I", 3) + struct.pack("<I", 1)
    buf += struct.pack("<I", len(ring))
    for x, y in ring:
        buf += struct.pack("<dd", x, y)
    return buf


def _points_df(honest_bbox: bool) -> daft.DataFrame:
    """Five points; ids 0/3 fall inside the [0,2]x[0,2] query rectangle.

    With ``honest_bbox`` the rtree_* columns hold each point's true bbox;
    otherwise they are shifted +1000 (far outside every query).
    """
    xs = [1.0, 5.0, 10.0, 1.5, -3.0]
    ys = [1.0, 5.0, 10.0, 1.5, -3.0]
    shift = 0.0 if honest_bbox else 1000.0
    return daft.from_pydict(
        {
            "id": list(range(5)),
            "geom": [_point_wkb(x, y) for x, y in zip(xs, ys)],
            "rtree_min_x": [x + shift for x in xs],
            "rtree_min_y": [y + shift for y in ys],
            "rtree_max_x": [x + shift for x in xs],
            "rtree_max_y": [y + shift for y in ys],
        }
    )


QUERY = _rect_wkb(0.0, 0.0, 2.0, 2.0)


def test_bbox_pruning_preserves_results():
    """With honest bbox columns the rewrite must not change the answer."""
    with_bbox = (
        _points_df(honest_bbox=True)
        .where(st_intersects(col("geom"), lit(QUERY)))
        .select("id")
        .sort("id")
        .to_pydict()
    )
    without_bbox = (
        _points_df(honest_bbox=True)
        .select("id", "geom")
        .where(st_intersects(col("geom"), lit(QUERY)))
        .select("id")
        .sort("id")
        .to_pydict()
    )
    assert with_bbox["id"] == [0, 3]
    assert with_bbox["id"] == without_bbox["id"]


def test_bbox_pruning_is_actually_applied():
    """Deliberately-wrong bbox columns must eliminate matching rows: the only
    way that happens is the injected bbox conjuncts — proving the rule fires
    end-to-end. (Without the rule this filter returns ids [0, 3].)"""
    got = (
        _points_df(honest_bbox=False)
        .where(st_intersects(col("geom"), lit(QUERY)))
        .select("id")
        .to_pydict()
    )
    assert got["id"] == [], "bbox conjuncts were not applied"


def test_bbox_pruning_contains_directional():
    """st_contains(Q, col): the column must lie within Q — same rows, and the
    corrupt-bbox variant proves the within-form conjuncts fire too."""
    honest = (
        _points_df(honest_bbox=True)
        .where(st_contains(lit(QUERY), col("geom")))
        .select("id")
        .sort("id")
        .to_pydict()
    )
    assert honest["id"] == [0, 3]

    corrupt = (
        _points_df(honest_bbox=False)
        .where(st_contains(lit(QUERY), col("geom")))
        .select("id")
        .to_pydict()
    )
    assert corrupt["id"] == []


def test_bbox_pruning_dwithin_padding_keeps_true_matches():
    """st_dwithin's conjuncts must pad the query bbox by the distance: a point
    at distance 4 from the query point (with honest bbox columns) survives a
    d=5 filter — an unpadded bbox conjunct would wrongly drop it."""
    got = (
        _points_df(honest_bbox=True)
        .where(st_dwithin(col("geom"), lit(_point_wkb(5.0, 2.0)), lit(5.0)))
        .select("id")
        .sort("id")
        .to_pydict()
    )
    # distances to (5,2): id0 (1,1)->4.12, id1 (5,5)->3.0, id2 ->9.43,
    # id3 (1.5,1.5)->3.54, id4 ->9.43
    assert got["id"] == [0, 1, 3]
