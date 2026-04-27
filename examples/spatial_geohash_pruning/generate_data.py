"""Generate spatial datasets for the geohash pruning / partition index examples.

Creates (by default):
  - points/        : 10 000 000 random points, written as spatially partitioned
                     parquet files with a ``_spatial_index.json`` sidecar per
                     partition directory.
  - polygons/      : 5 000 000 random small bounding-box polygons, similarly
                     partitioned and indexed.
  - points.parquet : single-file version (backward compat, 1 000 rows by default
                     unless --points is given and --no-single-file is NOT set)
  - polygons.parquet : single-file version (backward compat, 5 query polygons)

All geometries cover western Europe: lon ∈ [-10, 20], lat ∈ [40, 60].

Usage
-----
    # Full scale with defaults (10M pts, 5M polys, 32 partitions)
    python generate_data.py

    # Quick smoke test
    python generate_data.py --points 1_000 --polys 500 --partitions 4

    # Custom output directory, skip single-file compat files
    python generate_data.py --output /data/spatial --no-single-file

    # Control batch size for memory-constrained environments
    python generate_data.py --batch-size 500_000
"""

from __future__ import annotations

import argparse
import os
import struct
import sys
import time
from typing import Optional

import numpy as np

# ---------------------------------------------------------------------------
# Vectorised WKB generation
# ---------------------------------------------------------------------------

_POINT_HEADER = np.frombuffer(struct.pack("<BI", 1, 1), dtype=np.uint8)  # 5 bytes
_POLY_RING_HDR = np.frombuffer(struct.pack("<II", 1, 5), dtype=np.uint8)  # 8 bytes: n_rings=1, n_pts=5
_POLY_GEOM_HDR = np.frombuffer(struct.pack("<BI", 1, 3), dtype=np.uint8)  # 5 bytes


def points_wkb_batch(lons: np.ndarray, lats: np.ndarray) -> list[bytes]:
    """Vectorised WKB for an array of Points.  21 bytes each."""
    n = len(lons)
    buf = np.empty((n, 21), dtype=np.uint8)
    buf[:, :5] = _POINT_HEADER          # byte order + type
    buf[:, 5:13] = lons.astype("<f8").view(np.uint8).reshape(n, 8)
    buf[:, 13:21] = lats.astype("<f8").view(np.uint8).reshape(n, 8)
    return [bytes(row) for row in buf]


def rects_wkb_batch(
    x0: np.ndarray, y0: np.ndarray,
    x1: np.ndarray, y1: np.ndarray,
) -> list[bytes]:
    """Vectorised WKB for an array of axis-aligned rectangle Polygons.
    Ring: (x0,y0)→(x1,y0)→(x1,y1)→(x0,y1)→(x0,y0)  — 5 points.
    93 bytes each.
    """
    n = len(x0)
    buf = np.empty((n, 93), dtype=np.uint8)

    def _f8(arr: np.ndarray) -> np.ndarray:
        return arr.astype("<f8").view(np.uint8).reshape(n, 8)

    buf[:, :5]   = _POLY_GEOM_HDR        # byte order + type=3
    buf[:, 5:13] = _POLY_RING_HDR        # n_rings=1, n_pts=5
    # vertex 0: (x0, y0)
    buf[:, 13:21] = _f8(x0); buf[:, 21:29] = _f8(y0)
    # vertex 1: (x1, y0)
    buf[:, 29:37] = _f8(x1); buf[:, 37:45] = _f8(y0)
    # vertex 2: (x1, y1)
    buf[:, 45:53] = _f8(x1); buf[:, 53:61] = _f8(y1)
    # vertex 3: (x0, y1)
    buf[:, 61:69] = _f8(x0); buf[:, 69:77] = _f8(y1)
    # vertex 4: close (x0, y0)
    buf[:, 77:85] = _f8(x0); buf[:, 85:93] = _f8(y0)
    return [bytes(row) for row in buf]


# ---------------------------------------------------------------------------
# Vectorised geohash
# ---------------------------------------------------------------------------

_BASE32_CHARS = np.array(list("0123456789bcdefghjkmnpqrstuvwxyz"), dtype="U1")
_BIT_WEIGHTS  = np.array([16, 8, 4, 2, 1], dtype=np.int32)


def geohash_batch(lons: np.ndarray, lats: np.ndarray, precision: int = 5) -> list[str]:
    """Vectorised geohash encoding (numpy, no Python loop over rows)."""
    n = len(lons)
    result = np.empty((n, precision), dtype=np.uint8)

    min_lat = np.full(n, -90.0);  max_lat = np.full(n, 90.0)
    min_lon = np.full(n, -180.0); max_lon = np.full(n, 180.0)
    char_idx = np.zeros(n, dtype=np.int32)
    bit_pos   = 0
    char_pos  = 0
    even      = True  # even bit → split longitude

    for _ in range(precision * 5):
        if even:
            mid = (min_lon + max_lon) * 0.5
            mask = lons >= mid
            char_idx = np.where(mask, char_idx | _BIT_WEIGHTS[bit_pos], char_idx)
            min_lon  = np.where(mask, mid, min_lon)
            max_lon  = np.where(~mask, mid, max_lon)
        else:
            mid = (min_lat + max_lat) * 0.5
            mask = lats >= mid
            char_idx = np.where(mask, char_idx | _BIT_WEIGHTS[bit_pos], char_idx)
            min_lat  = np.where(mask, mid, min_lat)
            max_lat  = np.where(~mask, mid, max_lat)
        even = not even
        if bit_pos < 4:
            bit_pos += 1
        else:
            result[:, char_pos] = char_idx
            bit_pos = 0; char_pos += 1
            char_idx = np.zeros(n, dtype=np.int32)

    return ["".join(row) for row in _BASE32_CHARS[result]]


# ---------------------------------------------------------------------------
# Partition writing helpers
# ---------------------------------------------------------------------------

def _write_batch_partitioned(
    pydict: dict,
    out_root: str,
    partition_col: str,
    write_mode: str = "append",
) -> None:
    """Write one batch DataFrame to spatially partitioned parquet."""
    import daft
    df = daft.from_pydict(pydict)
    df.write_parquet(out_root, partition_cols=[partition_col], write_mode=write_mode)


def _build_all_indexes(root: str, geom_col: str) -> None:
    """Build _spatial_index.json for every partition subdirectory under root."""
    from daft.functions.spatial_index import build_spatial_index

    subdirs = sorted(
        os.path.join(root, d)
        for d in os.listdir(root)
        if os.path.isdir(os.path.join(root, d))
    )
    for subdir in subdirs:
        parquets = [f for f in os.listdir(subdir) if f.endswith(".parquet")]
        if parquets:
            build_spatial_index(subdir, geom_col=geom_col, glob_pattern="*.parquet")
    print(f"  Built spatial indexes in {len(subdirs)} partition dir(s) under '{root}'")


# ---------------------------------------------------------------------------
# Single-row helpers (backward compat)
# ---------------------------------------------------------------------------

def _single_rect_wkb(x0, y0, x1, y1) -> bytes:
    ring = [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]
    buf  = struct.pack("<BI", 1, 3) + struct.pack("<I", 1) + struct.pack("<I", len(ring))
    for x, y in ring:
        buf += struct.pack("<dd", x, y)
    return buf


_QUERY_POLYS = [
    ("London area",    _single_rect_wkb(-0.5, 51.3,  0.3, 51.7)),
    ("Paris area",     _single_rect_wkb( 2.2, 48.7,  2.5, 49.0)),
    ("Amsterdam area", _single_rect_wkb( 4.7, 52.2,  5.1, 52.5)),
    ("Berlin area",    _single_rect_wkb(13.2, 52.4, 13.6, 52.6)),
    ("Madrid area",    _single_rect_wkb(-3.9, 40.3, -3.5, 40.6)),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--points",     type=lambda s: int(s.replace("_", "")),
                   default=10_000_000,
                   help="Number of random points to generate (default: 10_000_000)")
    p.add_argument("--polys",      type=lambda s: int(s.replace("_", "")),
                   default=5_000_000,
                   help="Number of random polygons to generate (default: 5_000_000)")
    p.add_argument("--partitions", type=int, default=32,
                   help="Target number of output parquet files (default: 32).  "
                        "Actual file count equals the number of distinct p2 geohash "
                        "cells, so this is advisory.")
    p.add_argument("--batch-size", type=lambda s: int(s.replace("_", "")),
                   default=1_000_000,
                   help="Rows generated and written per batch (default: 1_000_000)")
    p.add_argument("--output", default=None,
                   help="Output directory (default: same directory as this script)")
    p.add_argument("--no-single-file", action="store_true",
                   help="Skip writing backward-compat single-file points.parquet / "
                        "polygons.parquet")
    p.add_argument("--seed", type=int, default=42, help="RNG seed (default: 42)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    rng  = np.random.default_rng(args.seed)

    out_dir = args.output or os.path.dirname(os.path.abspath(__file__))
    os.makedirs(out_dir, exist_ok=True)

    pts_root  = os.path.join(out_dir, "points")
    poly_root = os.path.join(out_dir, "polygons")
    os.makedirs(pts_root,  exist_ok=True)
    os.makedirs(poly_root, exist_ok=True)

    N_PTS   = args.points
    N_POLY  = args.polys
    BATCH   = args.batch_size

    # ── Points ──────────────────────────────────────────────────────────────
    print(f"Generating {N_PTS:,} points in batches of {BATCH:,} …")
    t0 = time.perf_counter()
    written_pts = 0
    batch_num   = 0

    while written_pts < N_PTS:
        size = min(BATCH, N_PTS - written_pts)
        lons = rng.uniform(-10.0, 20.0, size)
        lats = rng.uniform(40.0,  60.0, size)

        gh5 = geohash_batch(lons, lats, precision=5)   # companion for GeohashPruning
        gh2 = geohash_batch(lons, lats, precision=3)   # partition key

        batch = {
            "id":           np.arange(written_pts, written_pts + size, dtype=np.int64).tolist(),
            "lon":          np.round(lons, 6).tolist(),
            "lat":          np.round(lats, 6).tolist(),
            "geom":         points_wkb_batch(lons, lats),
            "geom_geohash": gh5,
            "partition_gh": gh2,
        }
        _write_batch_partitioned(batch, pts_root, "partition_gh")
        written_pts += size
        batch_num   += 1
        elapsed = time.perf_counter() - t0
        rate = written_pts / elapsed
        print(f"  batch {batch_num:3d}: {written_pts:>12,} / {N_PTS:,} pts "
              f"({100*written_pts/N_PTS:.1f}%)  {rate/1e6:.2f} M rows/s", end="\r")

    print(f"\n  Points written in {time.perf_counter()-t0:.1f}s  →  {pts_root}/")

    # Build spatial index for every partition dir
    print("Building spatial indexes for points …")
    _build_all_indexes(pts_root, geom_col="geom")

    # ── Polygons ─────────────────────────────────────────────────────────────
    print(f"\nGenerating {N_POLY:,} polygons in batches of {BATCH:,} …")
    t0 = time.perf_counter()
    written_poly = 0
    batch_num    = 0

    while written_poly < N_POLY:
        size = min(BATCH, N_POLY - written_poly)

        # Random center point
        cx = rng.uniform(-10.0, 20.0, size)
        cy = rng.uniform( 40.0, 60.0, size)
        # Random half-size: 0.05° … 2.0° (~5 km … 200 km)
        hw = rng.uniform(0.05, 2.0, size)
        hh = rng.uniform(0.05, 1.0, size)

        x0 = np.clip(cx - hw, -180, 180)
        y0 = np.clip(cy - hh,  -90,  90)
        x1 = np.clip(cx + hw, -180, 180)
        y1 = np.clip(cy + hh,  -90,  90)

        # Geohash of polygon center
        gh5 = geohash_batch(cx, cy, precision=5)
        gh2 = geohash_batch(cx, cy, precision=3)

        batch = {
            "id":           np.arange(written_poly, written_poly + size, dtype=np.int64).tolist(),
            "geom":         rects_wkb_batch(x0, y0, x1, y1),
            "geom_geohash": gh5,
            "partition_gh": gh2,
            "min_lon":      np.round(x0, 6).tolist(),
            "min_lat":      np.round(y0, 6).tolist(),
            "max_lon":      np.round(x1, 6).tolist(),
            "max_lat":      np.round(y1, 6).tolist(),
        }
        _write_batch_partitioned(batch, poly_root, "partition_gh")
        written_poly += size
        batch_num    += 1
        elapsed = time.perf_counter() - t0
        rate = written_poly / elapsed
        print(f"  batch {batch_num:3d}: {written_poly:>12,} / {N_POLY:,} polys "
              f"({100*written_poly/N_POLY:.1f}%)  {rate/1e6:.2f} M rows/s", end="\r")

    print(f"\n  Polygons written in {time.perf_counter()-t0:.1f}s  →  {poly_root}/")

    print("Building spatial indexes for polygons …")
    _build_all_indexes(poly_root, geom_col="geom")

    # ── Backward-compat single files ─────────────────────────────────────────
    if not args.no_single_file:
        import daft
        print("\nWriting backward-compat single-file parquet …")

        # points.parquet — first 1 000 rows from the already-generated data
        rng2 = np.random.default_rng(args.seed)
        n    = min(1_000, N_PTS)
        lo   = rng2.uniform(-10.0, 20.0, n)
        la   = rng2.uniform( 40.0, 60.0, n)
        pts_compat = daft.from_pydict({
            "id":           list(range(n)),
            "lon":          np.round(lo, 6).tolist(),
            "lat":          np.round(la, 6).tolist(),
            "geom":         points_wkb_batch(lo, la),
            "geom_geohash": geohash_batch(lo, la, precision=5),
        })
        pts_path = os.path.join(out_dir, "points.parquet")
        pts_compat.write_parquet(pts_path, write_mode="overwrite")
        print(f"  Wrote {n:,} points → {pts_path}")

        # polygons.parquet — fixed 5 query polygons (unchanged for examples)
        polys_compat = daft.from_pydict({
            "name": [n for n, _ in _QUERY_POLYS],
            "geom": [w for _, w in _QUERY_POLYS],
        })
        poly_path = os.path.join(out_dir, "polygons.parquet")
        polys_compat.write_parquet(poly_path, write_mode="overwrite")
        print(f"  Wrote {len(_QUERY_POLYS)} polygons → {poly_path}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n=== Summary ===")
    pts_files  = sum(
        len([f for f in os.listdir(os.path.join(pts_root, d)) if f.endswith(".parquet")])
        for d in os.listdir(pts_root) if os.path.isdir(os.path.join(pts_root, d))
    )
    poly_files = sum(
        len([f for f in os.listdir(os.path.join(poly_root, d)) if f.endswith(".parquet")])
        for d in os.listdir(poly_root) if os.path.isdir(os.path.join(poly_root, d))
    )
    print(f"  Points   : {N_PTS:>12,} rows across {pts_files:>4} file(s) in {pts_root}/")
    print(f"  Polygons : {N_POLY:>12,} rows across {poly_files:>4} file(s) in {poly_root}/")
    print(f"  Spatial indexes built for all partition directories.")
    print("\nReady!  Run the examples:")
    print(f"  DAFT_RUNNER=native python spatial_pip_geohash_pruning.py")
    print(f"  DAFT_RUNNER=native python spatial_partition_index.py")


if __name__ == "__main__":
    main()
