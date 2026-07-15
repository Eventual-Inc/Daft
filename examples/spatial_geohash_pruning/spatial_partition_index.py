"""Spatial partitioning + distributed R-tree index example.

Demonstrates the full workflow:

  1. Generate 1 000 random points.
  2. Write them into **spatially partitioned** parquet files — each file
     holds points that share the same coarse geohash cell (precision 3),
     so all geometries in one file are spatially adjacent.
  3. Build a ``_spatial_index.idx`` sidecar that records the H3 cells of each
     file.
  4. Query with ``read_parquet_spatial()`` — the index prunes files before
     Daft even creates scan tasks (Python-level pruning).
  5. Query with the standard ``read_parquet + where(st_intersects(…))``
     path — the ``SpatialPartitionPruning`` optimizer rule prunes scan
     tasks automatically (optimizer-level pruning).
  6. Compare both results and measure skip rates.

Usage
-----
    python spatial_partition_index.py
"""

from __future__ import annotations

import os
import random
import shutil
import struct
import tempfile

import daft
from daft.functions.spatial_index import (
    build_spatial_index,
    load_spatial_index,
)

# ── WKB helpers ──────────────────────────────────────────────────────────────


def point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, x, y)


def rect_wkb(x0: float, y0: float, x1: float, y1: float) -> bytes:
    ring = [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]
    buf = struct.pack("<BI", 1, 3) + struct.pack("<I", 1)
    buf += struct.pack("<I", len(ring))
    for x, y in ring:
        buf += struct.pack("<dd", x, y)
    return buf


# ── Geohash (pure Python) ─────────────────────────────────────────────────

_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def geohash_encode(lon: float, lat: float, precision: int = 5) -> str:
    min_lat, max_lat = -90.0, 90.0
    min_lon, max_lon = -180.0, 180.0
    bits = [16, 8, 4, 2, 1]
    bit_idx = char_idx = 0
    even = True
    result: list[str] = []
    while len(result) < precision:
        if even:
            mid = (min_lon + max_lon) / 2
            if lon >= mid:
                char_idx |= bits[bit_idx]
                min_lon = mid
            else:
                max_lon = mid
        else:
            mid = (min_lat + max_lat) / 2
            if lat >= mid:
                char_idx |= bits[bit_idx]
                min_lat = mid
            else:
                max_lat = mid
        even = not even
        if bit_idx < 4:
            bit_idx += 1
        else:
            result.append(_BASE32[char_idx])
            bit_idx = char_idx = 0
    return "".join(result)


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    random.seed(42)
    N = 1000
    PRECISION = 2  # coarse geohash for partitioning (each cell ≈ 1250 km × 625 km)

    # ── 1. Generate random points (western Europe) ────────────────────────
    # Generate lon/lat interleaved (same order as generate_data.py) so
    # the data matches the original example, including the London hit at id=644.
    print("=== Step 1: generate data ===")
    ids: list[int] = []
    lons: list[float] = []
    lats: list[float] = []
    geoms: list[bytes] = []
    gh5: list[str] = []
    gh3: list[str] = []
    for i in range(N):
        lon = round(random.uniform(-10.0, 20.0), 6)
        lat = round(random.uniform(40.0, 60.0), 6)
        ids.append(i)
        lons.append(lon)
        lats.append(lat)
        geoms.append(point_wkb(lon, lat))
        gh5.append(geohash_encode(lon, lat, 5))
        gh3.append(geohash_encode(lon, lat, PRECISION))

    df = daft.from_pydict(
        {
            "id": ids,
            "lon": lons,
            "lat": lats,
            "geom": geoms,
            "geom_geohash": gh5,  # companion column for GeohashPruning rule
            "partition_gh": gh3,  # used to drive the write partitioning
        }
    )

    print(
        f"  {N} points generated, unique p{PRECISION} geohash cells: "
        f"{len(set(gh3))} → {len(set(gh3))} output files/dirs"
    )

    # ── 2. Write spatially partitioned parquet ────────────────────────────
    # ``partition_cols`` groups rows by the partition column value and writes
    # each group to its own subdirectory, e.g. ``points/partition_gh=u1/``.
    # ``repartition`` ensures spatially adjacent points end up in the same
    # Daft partition before the write (requires RayRunner for a true shuffle;
    # on NativeRunner it is a no-op, but ``partition_cols`` still groups by
    # value before writing).
    print("\n=== Step 2: write spatially partitioned parquet ===")
    outdir = tempfile.mkdtemp(prefix="daft_spatial_")
    try:
        _run_example(df, outdir, lons, lats, gh5)
    finally:
        shutil.rmtree(outdir, ignore_errors=True)


def _run_example(df, outdir: str, lons, lats, gh5):
    # Each distinct precision-3 cell → its own subdirectory via partition_cols.
    # Files inside each subdirectory each hold one p3 cell worth of points.
    parquet_root = os.path.join(outdir, "points")
    os.makedirs(parquet_root)

    written = df.repartition(None, "partition_gh").write_parquet(  # shuffle so same cell → same partition
        parquet_root, partition_cols=["partition_gh"]
    )
    written_paths = written.to_pydict()["path"]
    print(
        f"  Wrote {len(written_paths)} file(s) across "
        f"{len(set(os.path.dirname(p) for p in written_paths))} partition subdirectories"
    )

    # ── 3. Build spatial index per partition directory ─────────────────────
    print("\n=== Step 3: build spatial index ===")

    # Collect unique partition directories
    part_dirs = sorted({os.path.dirname(p) for p in written_paths})
    total_indexed = 0
    for pdir in part_dirs:
        mbrs = build_spatial_index(pdir, geom_col="geom", glob_pattern="*.parquet")
        total_indexed += len(mbrs)

    print(f"  Indexed {total_indexed} file(s) across {len(part_dirs)} partition dir(s)")

    # ── 4. Show index content for one partition ────────────────────────────
    print("\n=== Step 4: inspect one partition index ===")
    sample_dir = part_dirs[0]
    idx = load_spatial_index(sample_dir)
    print(f"  {idx}")
    idx.load_full()
    for fname, cells in idx.file_h3_cells.items():
        print(f"  {fname}: {len(cells)} H3 cell(s), e.g. {cells[:3]}")

    # ── 5. Query: London bounding box ─────────────────────────────────────
    print("\n=== Step 5: spatial query (London bbox) ===")
    london = rect_wkb(-0.5, 51.3, 0.3, 51.7)

    # 5a. Python-level pruning via SpatialIndex.filter_paths
    #     Only reads files in partitions that overlap the query.
    kept_files: list[str] = []
    skipped_files = 0
    total_files = 0
    for pdir in part_dirs:
        all_files = sorted(os.path.join(pdir, f) for f in os.listdir(pdir) if f.endswith(".parquet"))
        total_files += len(all_files)
        idx = load_spatial_index(pdir)
        if idx is not None:
            kept = idx.filter_paths(all_files, london)
            skipped_files += len(all_files) - len(kept)
            kept_files.extend(kept)
        else:
            kept_files.extend(all_files)

    # Wrap the query geometry in a 1-row DataFrame so SQL can join on it.
    london_df = daft.from_pydict({"poly": [london], "join_key": [1]})

    if kept_files:
        pruned_pts = daft.sql("SELECT *, 1 AS join_key FROM pts", pts=daft.read_parquet(kept_files))
        result_py = daft.sql(
            """
            SELECT   p.id, p.lon, p.lat, p.geom_geohash
            FROM     pruned_pts p
            JOIN     london_df  q ON p.join_key = q.join_key
            WHERE    st_intersects(p.geom, q.poly)
            ORDER BY p.id
            """,
            pruned_pts=pruned_pts,
            london_df=london_df,
        )
        rows_py = result_py.to_pydict()
    else:
        rows_py = {"id": [], "lon": [], "lat": [], "geom_geohash": []}

    print("\n  [Python-level pruning]")
    print(f"  Files skipped : {skipped_files}/{total_files}")
    print(f"  Points found  : {len(rows_py['id'])}")
    for i, lo, la, gh in zip(rows_py["id"], rows_py["lon"], rows_py["lat"], rows_py["geom_geohash"]):
        print(f"    id={i:<5} lon={lo:<9.5f} lat={la:<9.5f} geohash={gh}")

    # 5b. SQL join path — reads all partition files and joins with london_df.
    print("\n  [SQL join — full scan with st_intersects join condition]")
    all_parquet = []
    for pdir in part_dirs:
        all_parquet.extend(os.path.join(pdir, f) for f in os.listdir(pdir) if f.endswith(".parquet"))
    flat_df = daft.read_parquet(all_parquet)
    pts_with_key = daft.sql("SELECT *, 1 AS join_key FROM pts", pts=flat_df)
    result_opt = daft.sql(
        """
        SELECT   p.id, p.lon, p.lat, p.geom_geohash
        FROM     pts_with_key p
        JOIN     london_df    q ON p.join_key = q.join_key
        WHERE    st_intersects(p.geom, q.poly)
        ORDER BY p.id
        """,
        pts_with_key=pts_with_key,
        london_df=london_df,
    )
    rows_opt = result_opt.to_pydict()
    print(f"  Points found  : {len(rows_opt['id'])}")
    for i, lo, la, gh in zip(rows_opt["id"], rows_opt["lon"], rows_opt["lat"], rows_opt["geom_geohash"]):
        print(f"    id={i:<5} lon={lo:<9.5f} lat={la:<9.5f} geohash={gh}")

    # ── 6. Correctness check ──────────────────────────────────────────────
    print("\n=== Step 6: correctness ===")
    assert set(rows_py["id"]) == set(rows_opt["id"]), (
        f"Mismatch!\n  python: {sorted(rows_py['id'])}\n  optimizer: {sorted(rows_opt['id'])}"
    )
    print(f"  ✓  Both paths return identical {len(rows_py['id'])} result(s).")

    # ── 7. Explain plan (optimizer pruning visible) ────────────────────────
    print("\n=== Step 7: explain plan (optimizer pruning) ===")
    result_opt.explain(show_all=True)

    # ── 8. Show per-partition file counts ─────────────────────────────────
    print("\n=== Step 8: partition summary ===")
    print(f"  {'Partition dir':<40} {'Files':>5} {'Has index':>9}")
    print("  " + "-" * 57)
    for pdir in sorted(part_dirs):
        files = [f for f in os.listdir(pdir) if f.endswith(".parquet")]
        has_idx = os.path.exists(os.path.join(pdir, "_spatial_index.idx"))
        label = os.path.relpath(pdir, os.path.dirname(pdir))
        print(f"  {label:<40} {len(files):>5} {'yes' if has_idx else 'no':>9}")

    # ── 9. Multi-region query ─────────────────────────────────────────────
    print("\n=== Step 9: multi-region query (SQL GROUP BY) ===")
    regions = daft.from_pydict(
        {
            "region": ["London", "Paris", "Amsterdam", "Berlin", "Madrid"],
            "poly": [
                rect_wkb(-0.5, 51.3, 0.3, 51.7),
                rect_wkb(2.2, 48.7, 2.5, 49.0),
                rect_wkb(4.7, 52.2, 5.1, 52.5),
                rect_wkb(13.2, 52.4, 13.6, 52.6),
                rect_wkb(-3.9, 40.3, -3.5, 40.6),
            ],
            "join_key": [1, 1, 1, 1, 1],
        }
    )

    points_k = daft.sql("SELECT *, 1 AS join_key FROM pts", pts=flat_df)

    counts = daft.sql(
        """
        SELECT   q.region, COUNT(*) AS points_inside
        FROM     points_k p
        JOIN     regions  q ON p.join_key = q.join_key
        WHERE    st_intersects(p.geom, q.poly)
        GROUP BY q.region
        ORDER BY q.region
        """,
        points_k=points_k,
        regions=regions,
    )
    print()
    counts.show()
    print("  Done.")


if __name__ == "__main__":
    main()
