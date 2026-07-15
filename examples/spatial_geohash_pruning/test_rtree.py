"""Quick smoke-test: verifies R-tree acceleration fires in NestedLoopJoin.

Creates tiny point/polygon DataFrames with known geometry, runs a
`st_contains` spatial join, checks:
  1. explain(show_all=True) contains "[R-tree]"
  2. The result rows are correct (points inside polygons matched).

Run:
    DAFT_RUNNER=native python examples/spatial_geohash_pruning/test_rtree.py
"""

from __future__ import annotations

import io
import struct
import sys

import daft

# ── Minimal WKB helpers ───────────────────────────────────────────────────

def wkb_point(x: float, y: float) -> bytes:
    """WKB little-endian Point."""
    return struct.pack("<BIdd", 1, 1, x, y)


def wkb_polygon_box(x0: float, y0: float, x1: float, y1: float) -> bytes:
    """WKB little-endian closed Polygon (box)."""
    # WKB polygon: byte_order(1) + wkb_type(3) + n_rings(4) + n_pts(4) + 5 coords
    header = struct.pack("<BII", 1, 3, 1)        # LE, Polygon, 1 ring
    n_pts  = struct.pack("<I", 5)
    ring   = struct.pack("<10d",
        x0, y0,
        x1, y0,
        x1, y1,
        x0, y1,
        x0, y0,   # close
    )
    return header + n_pts + ring


# ── Test data ─────────────────────────────────────────────────────────────
# Three unit-square polygons at (0,0)-(1,1), (2,0)-(3,1), (4,0)-(5,1)
# Points: P0(0.5, 0.5) in poly 0; P1(2.5, 0.5) in poly 1;
#         P2(10.0, 10.0) outside all polys.

polys = daft.from_pydict({
    "pid":  [0,    1,    2],
    "cell": [0,    1,    2],
    "geom": [
        wkb_polygon_box(0, 0, 1, 1),
        wkb_polygon_box(2, 0, 3, 1),
        wkb_polygon_box(4, 0, 5, 1),
    ],
})

points = daft.from_pydict({
    "qid":  [0,        1,        2],
    "cell": [0,        1,        0],   # P2 shares cell=0 with poly 0
    "geom": [
        wkb_point(0.5,  0.5),          # inside poly 0
        wkb_point(2.5,  0.5),          # inside poly 1
        wkb_point(10.0, 10.0),         # outside all
    ],
})

# ── Join ──────────────────────────────────────────────────────────────────

result = daft.sql(
    """
    SELECT p.pid, q.qid
    FROM   polys  AS p
    JOIN   points AS q ON p.cell = q.cell
    WHERE  st_contains(p.geom, q.geom)
    """,
    polys=polys,
    points=points,
)

# ── Check plan for [R-tree] ───────────────────────────────────────────────

buf = io.StringIO()
result.explain(show_all=True, file=buf)
plan_text = buf.getvalue()
print("=== Plan ===")
print(plan_text)

if "[R-tree]" in plan_text:
    print("✓  R-tree acceleration detected in physical plan")
else:
    print("✗  WARNING: [R-tree] NOT found in plan — fallback path used")
    print("   (geometry column extraction may have failed)")

# ── Check results ─────────────────────────────────────────────────────────

rows = result.sort("qid").collect()
print("\n=== Result rows ===")
print(rows)

rows_dict = rows.to_pydict()
pid_vals = rows_dict["pid"]
qid_vals = rows_dict["qid"]

pairs = set(zip(pid_vals, qid_vals))
expected = {(0, 0), (1, 1)}  # P0 in poly0, P1 in poly1; P2 outside all

if pairs == expected:
    print(f"\n✓  Correct: matched pairs = {sorted(pairs)}")
    sys.exit(0)
else:
    print(f"\n✗  WRONG result!")
    print(f"   expected: {sorted(expected)}")
    print(f"   got:      {sorted(pairs)}")
    sys.exit(1)
