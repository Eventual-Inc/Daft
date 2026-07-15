"""Point-in-polygon SQL query with geohash partition pruning.

Runs the query:

    SELECT p.id AS point_id,
           z.id AS polygon_id,
           p.geom AS point_geom,
           z.geom AS polygon_geom
    FROM   points   AS p
    JOIN   polygons AS z ON p.partition_gh = z.partition_gh
    WHERE  st_contains(z.geom, p.geom);

Joining on ``partition_gh`` prunes the cross-product to matching geohash
cells only, then the spatial filter is pushed down as a NestedLoopJoin.
Without pruning this would be ~100K × 50K comparisons per partition.

Usage
-----
    # 1. Generate data (once)
    python generate_data.py --points 100_000 --polys 50_000 --partitions 12 --no-single-file

    # 2. Run this query
    DAFT_RUNNER=native python pip_sql.py
"""

from __future__ import annotations

import os
import time

import daft

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_POINTS_DIR = os.path.join(_SCRIPT_DIR, "points")
_POLYS_DIR = os.path.join(_SCRIPT_DIR, "polygons")


def run() -> None:
    if not os.path.isdir(_POINTS_DIR) or not os.path.isdir(_POLYS_DIR):
        raise SystemExit(
            "Data not found. Run first:\n"
            "  python generate_data.py --points 10_000 --polys 5_000 "
            "--partitions 12 --no-single-file"
        )

    points = daft.read_parquet(_POINTS_DIR + "/**/*.parquet", hive_partitioning=True).with_column("_jk", daft.lit(1))
    polygons = daft.read_parquet(_POLYS_DIR + "/**/*.parquet", hive_partitioning=True).with_column("_jk", daft.lit(1))

    t0 = time.perf_counter()

    result = daft.sql(
        """
        SELECT
            p.id   AS point_id,
            z.id   AS polygon_id,
            p.geom AS point_geom,
            z.geom AS polygon_geom
        FROM   points   AS p,
            polygons AS z
        WHERE
            p.partition_gh = z.partition_gh
            AND p.lon BETWEEN z.min_lon AND z.max_lon
            AND p.lat BETWEEN z.min_lat AND z.max_lat
            AND st_contains(z.geom, p.geom)
        """,
        points=points,
        polygons=polygons,
    )

    n_out = result.count_rows()
    print(f"\nMatched rows  : {n_out:,}")
    print(f"Total time    : {time.perf_counter() - t0:.2f}s")
    result.show(10)


if __name__ == "__main__":
    run()
