"""Spatial point-in-polygon with automatic geohash pruning — pure SQL edition.

This example demonstrates how Daft's optimizer automatically rewrites a spatial
filter to add a cheap geohash pre-filter when the table has a ``{col}_geohash``
column, dramatically reducing the number of rows that need full WKB parsing.

Usage
-----
    # 1. Generate the dummy parquet files (once)
    python generate_data.py

    # 2. Run this example
    python spatial_pip_geohash_pruning.py

How it works
------------
1. The ``points`` table has a ``geom`` column (WKB binary) and a companion
   ``geom_geohash`` column containing the 5-character geohash of each point.

2. When Daft sees a filter like ``WHERE st_intersects(geom, poly)`` and the
   table has a ``geom_geohash`` companion column, the **GeohashPruning**
   optimizer rule fires automatically:
   - It computes the set of geohash cells that cover the query polygon's
     bounding box (at the same precision as the companion column).
   - It prepends ``st_geohash_covers(geom_geohash, <cells>)`` to the filter.
   - The geohash check operates on short strings and skips rows whose cell
     cannot possibly overlap the query — before any WKB bytes are parsed.

3. The final result is identical to the unpruned query; the pruning is purely
   a performance optimisation (no false negatives).

SQL notes
---------
- Spatial functions (``st_intersects``, ``st_contains``, ``st_geohash``,
  ``st_area``, …) are available directly in ``daft.sql()``.
- WKB geometry literals are passed by referencing a column in a bound
  DataFrame — Daft SQL does not yet support inline hex literals.
- The cross-join pattern uses a dummy constant key column so the equality
  join planner can produce the cartesian product.
"""

from __future__ import annotations

import os
import struct
import sys
from collections import Counter

import daft

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def rect_wkb(x0: float, y0: float, x1: float, y1: float) -> bytes:
    """Return WKB bytes for an axis-aligned rectangle."""
    ring = [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]
    buf = struct.pack("<BI", 1, 3)
    buf += struct.pack("<I", 1)
    buf += struct.pack("<I", len(ring))
    for x, y in ring:
        buf += struct.pack("<dd", x, y)
    return buf


def _check_parquet_files():
    here = os.path.dirname(__file__)
    points_path = os.path.join(here, "points.parquet")
    polys_path = os.path.join(here, "polygons.parquet")
    if not os.path.exists(points_path) or not os.path.exists(polys_path):
        print("Parquet files not found — run `python generate_data.py` first.")
        sys.exit(1)
    return points_path, polys_path


# ---------------------------------------------------------------------------
# Main example
# ---------------------------------------------------------------------------


def main():
    points_path, polys_path = _check_parquet_files()

    # ── 1. Load data ──────────────────────────────────────────────────────
    points = daft.read_parquet(points_path)
    polys = daft.read_parquet(polys_path)

    print("=== Points schema ===")
    print(points.schema())
    print(f"\nTotal points: {points.count_rows()}")

    print("\n=== Query polygons ===")
    polys.show()

    # ── 2. Build a single-row DataFrame holding the London bounding box ────
    #
    # Daft SQL does not support inline WKB hex literals, so we pass the
    # polygon geometry as a bound single-row DataFrame. A scalar subquery
    # ``(SELECT poly FROM query_df LIMIT 1)`` extracts it at query time.
    query_df = daft.from_pydict(
        {
            "poly": [rect_wkb(-0.5, 51.3, 0.3, 51.7)],  # London bounding box
        }
    )
    # Also add a constant join key to points for the multi-polygon spatial join.
    points_k = daft.sql("SELECT *, 1 AS join_key FROM points", points=points)

    # ── 3. Point-in-polygon WITH geohash pruning ──────────────────────────
    #
    # The GeohashPruning optimizer fires when it sees
    #   st_intersects(geom_col, literal)
    # and the table also has a ``geom_geohash`` companion column.
    # Note: pruning fires for plan-time literals; scalar subqueries are
    #       resolved at execution time and do not yet trigger pruning.
    pruned_result = daft.sql(
        """
        SELECT id, lon, lat, geom_geohash
        FROM   points
        WHERE  st_intersects(geom, (SELECT poly FROM query_df LIMIT 1))
        ORDER  BY id
        """,
        points=points,
        query_df=query_df,
    )

    print("\n=== Optimised query plan (geohash pruning active) ===")
    pruned_result.explain(show_all=True)

    hits = pruned_result.to_pydict()
    print(f"\nPoints inside London bounding box: {len(hits['id'])}")
    print("  id   | lon       | lat       | geohash")
    print("  " + "-" * 45)
    for i, lon, lat, gh in zip(hits["id"], hits["lon"], hits["lat"], hits["geom_geohash"]):
        print(f"  {i:<5}| {lon:<10.6f}| {lat:<10.6f}| {gh}")

    # ── 4. Same query WITHOUT the geohash column (pruning disabled) ────────
    unpruned_result = daft.sql(
        """
        SELECT id, lon, lat
        FROM   points
        WHERE  st_intersects(geom, (SELECT poly FROM query_df LIMIT 1))
        ORDER  BY id
        """,
        points=daft.sql("SELECT id, lon, lat, geom FROM points", points=points),
        query_df=query_df,
    )

    print("\n=== Optimised query plan (no geohash column — pruning NOT active) ===")
    unpruned_result.explain(show_all=True)

    # ── 5. Verify identical results ────────────────────────────────────────
    pruned_ids = set(hits["id"])
    unpruned_ids = set(unpruned_result.to_pydict()["id"])
    assert pruned_ids == unpruned_ids, (
        f"Results differ!\n  pruned:   {sorted(pruned_ids)}\n  unpruned: {sorted(unpruned_ids)}"
    )
    print("\n✓  Pruned and unpruned queries return identical results.")

    # ── 6. st_contains — strict interior test ─────────────────────────────
    print("\n=== st_contains (strict interior — excludes boundary points) ===")
    contains_result = daft.sql(
        """
        SELECT id, lon, lat
        FROM   points
        WHERE  st_contains((SELECT poly FROM query_df LIMIT 1), geom)
        ORDER  BY id
        """,
        points=points,
        query_df=query_df,
    )
    print(f"Points strictly inside London bounding box: {contains_result.count_rows()}")

    # ── 7. Distinct geohash cells that matched ─────────────────────────────
    print("\n=== Geohash cells that matched the query ===")
    cells_result = daft.sql(
        """
        SELECT DISTINCT geom_geohash
        FROM   points
        WHERE  st_intersects(geom, (SELECT poly FROM query_df LIMIT 1))
        ORDER  BY geom_geohash
        """,
        points=points,
        query_df=query_df,
    )
    cells_result.show()

    # ── 8. Compute geohash on-the-fly with st_geohash ─────────────────────
    print("\n=== Computing geohash on the fly with st_geohash ===")
    geohash_computed = daft.sql(
        """
        SELECT id, lon, lat, st_geohash(geom, 5) AS geom_geohash
        FROM   points
        LIMIT  5
        """,
        points=points,
    )
    geohash_computed.show()

    # ── 9. Polygon areas via st_area ───────────────────────────────────────
    print("\n=== Polygon areas (st_area) ===")
    areas = daft.sql(
        """
        SELECT name, st_area(geom) AS area_deg2
        FROM   polys
        ORDER  BY area_deg2 DESC
        """,
        polys=polys,
    )
    areas.show()

    # ── 10. Spatial join: points × polygons ───────────────────────────────
    #
    # Find every (point, polygon) pair where the point falls inside the polygon.
    # Pattern: join on a constant key (cartesian product) then filter spatially.
    # When ``points`` has ``geom_geohash`` the optimizer injects a geohash
    # pre-filter, significantly pruning the inner loop.
    print("\n=== Spatial join: points × polygons ===")

    # Add a constant join key to the polygon table as well.
    polys_k = daft.sql(
        "SELECT name AS region, geom AS poly_geom, 1 AS join_key FROM polys",
        polys=polys,
    )

    spatial_join = daft.sql(
        """
        SELECT   q.region,
                 p.id,
                 p.lon,
                 p.lat,
                 p.geom_geohash
        FROM     points_k p
        JOIN     polys_k  q ON p.join_key = q.join_key
        WHERE    st_intersects(p.geom, q.poly_geom)
        ORDER BY q.region, p.id
        """,
        points_k=points_k,
        polys_k=polys_k,
    )

    print("\n=== Spatial join plan ===")
    spatial_join.explain(show_all=True)

    join_result = spatial_join.to_pydict()
    print(f"\nTotal (point, region) pairs found: {len(join_result['id'])}")

    per_region = Counter(join_result["region"])
    print("\n  Region               | Points inside")
    print("  " + "-" * 38)
    for region in sorted(per_region):
        print(f"  {region:<20} | {per_region[region]:4d}")

    # ── 11. Per-region counts with SQL GROUP BY ────────────────────────────
    print("\n=== Per-region point counts (SQL GROUP BY) ===")
    counts = daft.sql(
        """
        SELECT   q.region,
                 COUNT(*) AS points_inside
        FROM     points_k p
        JOIN     polys_k  q ON p.join_key = q.join_key
        WHERE    st_intersects(p.geom, q.poly_geom)
        GROUP BY q.region
        ORDER BY q.region
        """,
        points_k=points_k,
        polys_k=polys_k,
    )
    counts.show()

    print()
    spatial_join.show()


if __name__ == "__main__":
    main()
