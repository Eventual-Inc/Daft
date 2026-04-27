"""Spatial index utilities for partition-level pruning.

Overview
--------
A *spatial index* is a small JSON sidecar file (``_spatial_index.json``)
stored alongside a directory of parquet files.  It records the spatial
coverage of each file so the ``SpatialPartitionPruning`` optimizer rule can
skip files that cannot possibly intersect the query geometry.

Index format — H3 hex index  *(requires the ``h3`` Python package)*
    Each file is mapped to the set of `H3 <https://h3geo.org>`_ hexagonal cell
    indices that cover its geometries.  H3 hexagons tile the globe uniformly,
    enabling aggressive partition pruning.

    Install the H3 library::

        pip install h3

    Recommended resolutions:

    ========  ===========  =====================
    Res       Edge length  Typical use-case
    ========  ===========  =====================
    4         ~22 km       Country / region data
    5         ~8.5 km      City clusters (default)
    6         ~3.2 km      Dense urban data
    7         ~1.2 km      Street-level data
    ========  ===========  =====================

Typical workflow
----------------
::

    # 1. Write partitioned data
    df.write_parquet("output/")

    # 2. Build the H3 index (once)
    from daft.functions.spatial_index import build_spatial_index
    build_spatial_index("output/", geom_col="geom")
    # → writes output/_spatial_index.json  (H3)

    # 3. Query — pruning is automatic
    import daft
    from daft.functions.spatial import st_intersects
    df = daft.read_parquet("output/*.parquet")
    result = df.where(st_intersects(col("geom"), lit(query_wkb)))

Index format (v1)
-----------------
::

    {
      "version": 1,
      "geom_col": "geom",
      "h3_resolution": 5,
      "files": {
        "part-0.parquet": ["851fb46ffffffff", "851fb467fffffff"],
        "part-1.parquet": ["851fb46ffffffff"]
      }
    }

Cell strings use H3's standard hex representation (15 hex characters).
"""

from __future__ import annotations

import glob
import json
import os
import struct
from typing import Optional

__all__ = [
    "build_spatial_index",
    "load_spatial_index",
    "spatial_join",
    "SpatialIndex",
]

INDEX_FILENAME = "_spatial_index.json"
INDEX_VERSION_H3 = 1
DEFAULT_H3_RESOLUTION = 10
DEFAULT_MAX_CELLS_PER_FILE = 50


# ── H3 library helpers ────────────────────────────────────────────────────


def _h3_import():
    """Return ``(h3_module, is_v4)`` or ``(None, False)`` if not installed."""
    try:
        import h3 as _h3
        # h3 v4 renamed geo_to_h3 → latlng_to_cell
        is_v4 = hasattr(_h3, "latlng_to_cell")
        return _h3, is_v4
    except ImportError:
        return None, False


def _cell_for_point(h3lib, is_v4: bool, lat: float, lon: float, res: int) -> str:
    if is_v4:
        return h3lib.latlng_to_cell(lat, lon, res)
    return h3lib.geo_to_h3(lat, lon, res)


def _disk(h3lib, is_v4: bool, cell: str, k: int) -> set:
    if is_v4:
        return h3lib.grid_disk(cell, k)
    return h3lib.k_ring(cell, k)


# ── WKB parsing (coordinate extraction) ──────────────────────────────────


def _wkb_mbr(data: bytes) -> Optional[tuple[float, float, float, float]]:
    """Return ``(min_x, min_y, max_x, max_y)`` for any WKB geometry."""
    if not data:
        return None
    try:
        return _parse_wkb(data, 0)[1]
    except Exception:
        return None


def _wkb_rings(data: bytes) -> list[list[tuple[float, float]]]:
    """Return exterior + interior rings as lists of ``(lon, lat)`` pairs."""
    if not data:
        return []
    try:
        return _parse_wkb_rings(data, 0)[1]
    except Exception:
        return []


def _parse_wkb(buf: bytes, offset: int) -> tuple[int, Optional[tuple]]:
    bo = buf[offset]
    e = "<" if bo == 1 else ">"
    geom_type = struct.unpack_from(e + "I", buf, offset + 1)[0] & 0xFFFF
    offset += 5
    return _parse_type(buf, e, geom_type, offset)


def _parse_type(buf: bytes, e: str, geom_type: int, offset: int) -> tuple[int, Optional[tuple]]:
    if geom_type == 1:  # Point
        x, y = struct.unpack_from(e + "dd", buf, offset)
        return offset + 16, (x, y, x, y)
    if geom_type == 2:  # LineString
        n = struct.unpack_from(e + "I", buf, offset)[0]; offset += 4
        coords = struct.unpack_from(e + "d" * (2 * n), buf, offset); offset += 16 * n
        xs = coords[0::2]; ys = coords[1::2]
        return offset, (min(xs), min(ys), max(xs), max(ys))
    if geom_type == 3:  # Polygon
        n_rings = struct.unpack_from(e + "I", buf, offset)[0]; offset += 4
        all_x: list[float] = []; all_y: list[float] = []
        for _ in range(n_rings):
            n = struct.unpack_from(e + "I", buf, offset)[0]; offset += 4
            coords = struct.unpack_from(e + "d" * (2 * n), buf, offset); offset += 16 * n
            all_x.extend(coords[0::2]); all_y.extend(coords[1::2])
        if not all_x:
            return offset, None
        return offset, (min(all_x), min(all_y), max(all_x), max(all_y))
    if geom_type in (4, 5, 6, 7):  # Multi* / GeometryCollection
        n = struct.unpack_from(e + "I", buf, offset)[0]; offset += 4
        mbrs: list[tuple] = []
        for _ in range(n):
            offset, sub = _parse_wkb(buf, offset)
            if sub is not None:
                mbrs.append(sub)
        if not mbrs:
            return offset, None
        return offset, (
            min(m[0] for m in mbrs), min(m[1] for m in mbrs),
            max(m[2] for m in mbrs), max(m[3] for m in mbrs),
        )
    return offset, None


def _parse_wkb_rings(buf: bytes, offset: int) -> tuple[int, list[list[tuple[float, float]]]]:
    """Parse WKB to a flat list of rings (each ring = list of (lon, lat) pairs)."""
    bo = buf[offset]
    e = "<" if bo == 1 else ">"
    geom_type = struct.unpack_from(e + "I", buf, offset + 1)[0] & 0xFFFF
    offset += 5

    if geom_type == 1:  # Point
        x, y = struct.unpack_from(e + "dd", buf, offset)
        return offset + 16, [[(x, y)]]

    if geom_type == 3:  # Polygon
        n_rings = struct.unpack_from(e + "I", buf, offset)[0]; offset += 4
        rings: list[list[tuple[float, float]]] = []
        for _ in range(n_rings):
            n = struct.unpack_from(e + "I", buf, offset)[0]; offset += 4
            coords = struct.unpack_from(e + "d" * (2 * n), buf, offset); offset += 16 * n
            rings.append([(coords[i * 2], coords[i * 2 + 1]) for i in range(n)])
        return offset, rings

    if geom_type in (4, 5, 6, 7):  # Multi* / GeometryCollection
        n = struct.unpack_from(e + "I", buf, offset)[0]; offset += 4
        all_rings: list[list[tuple[float, float]]] = []
        for _ in range(n):
            offset, sub_rings = _parse_wkb_rings(buf, offset)
            all_rings.extend(sub_rings)
        return offset, all_rings

    return offset, []


# Average H3 cell area in m² per resolution level (from h3geo.org)
_H3_CELL_AREA_M2 = [
    4_250_546_840_000, 607_220_000_000, 86_745_854_071, 12_392_264_870,
    1_770_347_654, 252_903_858, 36_129_813, 5_161_293, 737_327, 105_332,
    15_047, 2_149, 307, 44, 6, 1,
]


def _estimate_polyfill_cells(x0: float, y0: float, x1: float, y1: float, resolution: int) -> int:
    """Rough upper-bound estimate of H3 polyfill count from an MBR."""
    area_m2 = (x1 - x0) * (y1 - y0) * (111_000 ** 2)
    cell_area = _H3_CELL_AREA_M2[min(resolution, 15)]
    return max(1, int(area_m2 / cell_area))


# ── H3 cell computation ───────────────────────────────────────────────────


def _h3_cells_for_wkb(
    wkb: bytes,
    h3lib,
    is_v4: bool,
    resolution: int,
    max_polyfill: int = 10_000,
) -> set[str]:
    """Return the set of H3 cell strings covering ``wkb`` at *resolution*.

    Strategy
    --------
    * **Points** — single exact cell.
    * **Small polygons** (estimated polyfill ≤ *max_polyfill*) — full
      ``polygon_to_cells`` / ``polyfill_geojson`` for interior cells, plus a
      k=1 ring around every boundary vertex for edge coverage.
    * **Large polygons** (estimated polyfill > *max_polyfill*) — uniform
      ``n × n`` grid of sample points across the MBR, converted to cells.
      This avoids enumerating millions of cells for coarse geometries at fine
      resolutions while still providing representative spatial coverage.
    """
    rings = _wkb_rings(wkb)
    if not rings:
        return set()

    cells: set[str] = set()

    for ring in rings:
        if len(ring) == 1:
            # Point
            lon, lat = ring[0]
            c = _cell_for_point(h3lib, is_v4, lat, lon, resolution)
            cells.add(c)
            continue

        # Compute ring MBR for area estimate
        xs = [p[0] for p in ring]
        ys = [p[1] for p in ring]
        x0, y0, x1, y1 = min(xs), min(ys), max(xs), max(ys)

        estimated = _estimate_polyfill_cells(x0, y0, x1, y1, resolution)
        if estimated > max_polyfill:
            # Large polygon — sample a uniform grid across the MBR
            n = max(2, int(max_polyfill ** 0.5))
            dx = (x1 - x0) / (n - 1) if n > 1 else 0.0
            dy = (y1 - y0) / (n - 1) if n > 1 else 0.0
            for i in range(n):
                for j in range(n):
                    lon = x0 + i * dx
                    lat = y0 + j * dy
                    cells.add(_cell_for_point(h3lib, is_v4, lat, lon, resolution))
        else:
            # Small polygon: polyfill for interior + vertex expansion for edges.
            outer_latlng = [(lat, lon) for lon, lat in ring]
            try:
                if is_v4:
                    poly = h3lib.LatLngPoly(outer_latlng)
                    interior = set(h3lib.polygon_to_cells(poly, resolution))
                else:
                    geojson = {
                        "type": "Polygon",
                        "coordinates": [[[lon, lat] for lon, lat in ring]],
                    }
                    interior = set(h3lib.polyfill_geojson(geojson, resolution))
            except Exception:
                interior = set()
            cells.update(interior)

            # Boundary vertex cells + k=1 ring (catches edge-straddling cells).
            for lon, lat in ring:
                c = _cell_for_point(h3lib, is_v4, lat, lon, resolution)
                cells.add(c)
                cells.update(_disk(h3lib, is_v4, c, 1))

    return cells


# ── Index building ────────────────────────────────────────────────────────


def build_spatial_index(
    directory: str,
    geom_col: str = "geom",
    output_path: Optional[str] = None,
    glob_pattern: str = "*.parquet",
    h3_resolution: int = DEFAULT_H3_RESOLUTION,
    max_cells_per_file: int = DEFAULT_MAX_CELLS_PER_FILE,
) -> dict:
    """Scan every parquet file in *directory* and write ``_spatial_index.json``.

    Parameters
    ----------
    directory:
        Directory containing the parquet files.
    geom_col:
        Name of the WKB-encoded geometry column.
    output_path:
        Path for the index file.  Defaults to
        ``{directory}/_spatial_index.json``.
    glob_pattern:
        Glob pattern for parquet files within *directory*.
    h3_resolution:
        H3 resolution (0–15).  Default 10 (~65 m cell edge).
        Increase for finer precision at the cost of larger index files.
    max_cells_per_file:
        Maximum number of H3 cells stored per parquet file in the index.
        When the full cell set is larger, a random sample of this size is
        kept, trading recall for a smaller index.  Default 50.

    Returns
    -------
    dict
        ``{filename: [cell, ...]}`` mapping that was written to the index.
    """
    import daft

    parquet_files = sorted(glob.glob(os.path.join(directory, glob_pattern)))
    if not parquet_files:
        raise FileNotFoundError(
            f"No files matching '{glob_pattern}' found in '{directory}'"
        )

    if output_path is None:
        output_path = os.path.join(directory, INDEX_FILENAME)

    h3lib, is_v4 = _h3_import()
    if h3lib is None:
        raise ImportError(
            "The 'h3' package is required to build spatial indexes.\n"
            "Install it with:  pip install h3"
        )
    return _build_h3_index(
        parquet_files, geom_col, h3lib, is_v4, h3_resolution, output_path,
        max_cells_per_file,
    )


def _build_h3_index(
    parquet_files: list[str],
    geom_col: str,
    h3lib,
    is_v4: bool,
    resolution: int,
    output_path: str,
    max_cells_per_file: int = DEFAULT_MAX_CELLS_PER_FILE,
) -> dict[str, list[str]]:
    import random
    import daft

    file_cells: dict[str, list[str]] = {}
    for fpath in parquet_files:
        df = daft.read_parquet(fpath)
        rows = df.select(geom_col).to_pydict()[geom_col]
        cells: set[str] = set()
        for wkb in rows:
            if wkb is not None:
                cells.update(
                    _h3_cells_for_wkb(
                        bytes(wkb), h3lib, is_v4, resolution,
                        max_polyfill=max_cells_per_file * 10,
                    )
                )
        if cells:
            cell_list = sorted(cells)
            if len(cell_list) > max_cells_per_file:
                cell_list = sorted(random.sample(cell_list, max_cells_per_file))
            file_cells[os.path.basename(fpath)] = cell_list

    index_data = {
        "version": INDEX_VERSION_H3,
        "geom_col": geom_col,
        "h3_resolution": resolution,
        "files": file_cells,
    }
    with open(output_path, "w") as f:
        json.dump(index_data, f, indent=2)
    print(
        f"H3 spatial index (v1, res={resolution}, max_cells={max_cells_per_file}) "
        f"written to '{output_path}' ({len(file_cells)} files indexed)."
    )
    return file_cells




# ── Index querying ────────────────────────────────────────────────────────


class SpatialIndex:
    """In-memory representation of a ``_spatial_index.json`` sidecar (H3 format).

    Attributes
    ----------
    geom_col : str
        Geometry column this index was built for.
    h3_resolution : int
        H3 resolution the index was built at.
    file_h3_cells : dict
        ``{basename: [cell_str, ...]}`` — H3 cells covering each file.
    """

    def __init__(
        self,
        geom_col: str,
        *,
        file_h3_cells: Optional[dict[str, list[str]]] = None,
        h3_resolution: Optional[int] = None,
    ):
        self.geom_col = geom_col
        self.file_h3_cells: dict[str, list[str]] = file_h3_cells or {}
        self.h3_resolution = h3_resolution

    @classmethod
    def load(cls, path: str) -> "SpatialIndex":
        """Load a ``_spatial_index.json`` file."""
        with open(path) as f:
            data = json.load(f)

        version = data.get("version")
        if version != INDEX_VERSION_H3:
            raise ValueError(
                f"Unsupported spatial index version: {version!r}. Only H3 (version 1) is supported."
            )
        return cls(
            geom_col=data["geom_col"],
            file_h3_cells=data["files"],
            h3_resolution=data["h3_resolution"],
        )

    def filter_paths(
        self,
        full_paths: list[str],
        query_wkb: bytes,
    ) -> list[str]:
        """Return only those *full_paths* whose coverage overlaps *query_wkb*.

        Files not present in the index are kept (conservative fallback).
        """
        return self._filter_h3(full_paths, query_wkb)

    def _filter_h3(self, full_paths: list[str], query_wkb: bytes) -> list[str]:
        h3lib, is_v4 = _h3_import()
        if h3lib is None or self.h3_resolution is None:
            return full_paths  # conservative: keep all if h3 unavailable

        # Use a coarser resolution for comparison so that the 50-cell sample
        # reliably covers the partition.  At res=5 (~8.5 km) each stored
        # fine-res cell maps to a single parent cell, giving full partition
        # coverage even with a small sample.
        COMPARE_RES = min(self.h3_resolution, 5)
        query_cells = _h3_cells_for_wkb(query_wkb, h3lib, is_v4, COMPARE_RES)
        if not query_cells:
            return full_paths

        def _to_parent(cell: str) -> str:
            if COMPARE_RES < self.h3_resolution:
                return h3lib.cell_to_parent(cell, COMPARE_RES) if is_v4 else h3lib.h3_to_parent(cell, COMPARE_RES)
            return cell

        result = []
        for path in full_paths:
            basename = os.path.basename(path)
            file_cells = self.file_h3_cells.get(basename)
            if file_cells is None:
                result.append(path)
                continue
            coarse = {_to_parent(c) for c in file_cells}
            if bool(query_cells.intersection(coarse)):
                result.append(path)
        return result

    def __repr__(self) -> str:
        return (
            f"SpatialIndex(H3, geom_col={self.geom_col!r}, "
            f"resolution={self.h3_resolution}, files={len(self.file_h3_cells)})"
        )


def load_spatial_index(directory: str) -> Optional[SpatialIndex]:
    """Load the ``_spatial_index.json`` from *directory*, or return ``None``."""
    path = os.path.join(directory, INDEX_FILENAME)
    if not os.path.exists(path):
        return None
    return SpatialIndex.load(path)


def read_parquet_spatial(
    directory: str,
    query_wkb: bytes,
    geom_col: str = "geom",
    glob_pattern: str = "*.parquet",
):
    """Read only the parquet files whose coverage overlaps *query_wkb*.

    Loads the ``_spatial_index.json`` sidecar (if present) and passes only
    the relevant file paths to ``daft.read_parquet``.  Supports both v1
    (MBR) and v2 (H3) index formats transparently.
    """
    import daft

    all_paths = sorted(glob.glob(os.path.join(directory, glob_pattern)))
    if not all_paths:
        raise FileNotFoundError(f"No '{glob_pattern}' files in '{directory}'")

    index = load_spatial_index(directory)
    if index is None or index.geom_col != geom_col:
        return daft.read_parquet(os.path.join(directory, glob_pattern))

    pruned = index.filter_paths(all_paths, query_wkb)
    skipped = len(all_paths) - len(pruned)
    if skipped:
        print(f"Spatial index pruned {skipped}/{len(all_paths)} file(s).")

    if not pruned:
        return daft.read_parquet(all_paths[0]).where(daft.lit(False))

    return daft.read_parquet(pruned)


# ── Dynamic partition pruning join ────────────────────────────────────────

def spatial_join(
    left,
    right,
    *,
    predicate: str = "st_contains",
    left_geom: str = "geom",
    right_geom: str = "geom",
    left_dir: Optional[str] = None,
    right_dir: Optional[str] = None,
    output_cols: Optional[list[str]] = None,
):
    """Spatial join with automatic dynamic partition pruning.

    Implements the conceptual query::

        SELECT <output_cols>
        FROM   left  AS l,
               right AS r
        WHERE  <predicate>(l.<left_geom>, r.<right_geom>);

    Before running the join, the function:

    1. Detects which side has a ``_spatial_index.json`` (the *probe* side).
    2. Materialises the *build* side's geometry column (the other side).
    3. Computes a union bounding geometry from the build side.
    4. Uses the spatial index to skip probe-side partitions that cannot
       possibly overlap the build-side geometries.
    5. Executes the SQL join over the pruned file set.

    Parameters
    ----------
    left, right:
        Either a ``daft.DataFrame`` or a directory path (str) containing
        hive-partitioned parquet files with a ``_spatial_index.json`` sidecar.
    predicate:
        Spatial SQL function name.  One of ``"st_contains"``,
        ``"st_intersects"``, ``"st_within"``.  Applied as
        ``predicate(left.<left_geom>, right.<right_geom>)``.
    left_geom, right_geom:
        Names of the WKB geometry columns on each side.
    left_dir, right_dir:
        Override the partition directory used for index lookup when *left* /
        *right* is already a DataFrame (e.g. if it was built from a subset of
        files).  If not provided and the corresponding argument is a str, that
        path is used automatically.
    output_cols:
        Columns to include in the result.  Defaults to all columns from both
        sides (with left columns prefixed by ``l_`` and right by ``r_`` to
        avoid collisions).

    Returns
    -------
    daft.DataFrame
        Result of the spatial join, with partition pruning applied.

    Example
    -------
    ::

        from daft.functions.spatial_index import spatial_join

        result = spatial_join(
            "path/to/points/",
            "path/to/polygons/",
            predicate="st_contains",
            left_geom="geom",
            right_geom="geom",
        )
        result.show()
    """
    import daft

    # ── Resolve directories and DataFrames ────────────────────────────────
    def _to_df_and_dir(arg, default_dir):
        if isinstance(arg, str):
            glob_path = os.path.join(arg, "**", "*.parquet")
            return daft.read_parquet(glob_path, hive_partitioning=True), arg
        return arg, default_dir

    left_df, l_dir = _to_df_and_dir(left, left_dir)
    right_df, r_dir = _to_df_and_dir(right, right_dir)

    # ── Detect which side has a spatial index ─────────────────────────────
    def _has_index(dirpath, geom_col):
        if dirpath is None:
            return False
        for entry in os.scandir(dirpath):
            if entry.is_dir():
                idx = load_spatial_index(entry.path)
                if idx is not None and idx.geom_col == geom_col:
                    return True
        return False

    def _has_gh_partitions(dirpath):
        """Return True if the directory contains partition_gh=<prefix> subdirs."""
        if dirpath is None:
            return False
        return any(
            e.is_dir() and os.path.basename(e.path).startswith("partition_gh=")
            for e in os.scandir(dirpath)
        )

    left_indexed   = _has_index(l_dir, left_geom)
    right_indexed  = _has_index(r_dir, right_geom)
    left_gh_parts  = _has_gh_partitions(l_dir)
    right_gh_parts = _has_gh_partitions(r_dir)

    # ── Geohash partition pruning (no sidecar needed) ─────────────────────
    def _geohash_prefix_pruning(probe_dir, build_df, build_geom_col):
        """Prune probe partitions using geohash prefix of the build-side union MBR."""
        try:
            import geohash as _gh
        except ImportError:
            return None  # geohash not available

        build_wkbs = [
            bytes(w)
            for w in build_df.select(build_geom_col).to_pydict()[build_geom_col]
            if w is not None
        ]
        union_wkb = _compute_union_wkb(build_wkbs)
        if union_wkb is None:
            return None

        # Determine prefix length from the first partition dir name.
        part_dirs = sorted(
            e.path for e in os.scandir(probe_dir) if e.is_dir()
            and os.path.basename(e.path).startswith("partition_gh=")
        )
        if not part_dirs:
            return None

        # Extract prefix length from first dir.
        first_prefix = os.path.basename(part_dirs[0]).split("=", 1)[1]
        precision = len(first_prefix)

        # Compute covering geohash cells from union MBR.
        mbr = _wkb_mbr(union_wkb)
        if mbr is None:
            return None
        x0, y0, x1, y1 = mbr

        # Sample a grid of points across the MBR and collect their geohash prefixes.
        n = max(3, precision * 2)
        dx = (x1 - x0) / max(n - 1, 1)
        dy = (y1 - y0) / max(n - 1, 1)
        covering: set[str] = set()
        for i in range(n):
            for j in range(n):
                lon = x0 + i * dx
                lat = y0 + j * dy
                try:
                    cell = _gh.encode(lat, lon, precision=precision)
                    covering.add(cell)
                    # Also add k=1 geohash neighbors for edge coverage.
                    for neighbor in _gh.neighbors(cell).values():
                        covering.add(neighbor)
                except Exception:
                    pass

        kept_files, skipped = [], 0
        all_dirs = sorted(e.path for e in os.scandir(probe_dir) if e.is_dir())
        for pdir in all_dirs:
            basename = os.path.basename(pdir)
            if basename.startswith("partition_gh="):
                prefix = basename.split("=", 1)[1]
                if prefix not in covering:
                    skipped += 1
                    continue
            files = sorted(
                os.path.join(pdir, f)
                for f in os.listdir(pdir)
                if f.endswith(".parquet")
            )
            kept_files.extend(files)

        total = len(all_dirs)
        print(
            f"Geohash prefix pruning (precision={precision}): "
            f"skipped {skipped}/{total} partitions"
        )
        return kept_files

    # ── Dynamic partition pruning (H3 sidecar index) ──────────────────────
    def _prune_dir(probe_dir, probe_geom, build_df, build_geom):
        """Materialise build side geom, compute union WKB, prune probe tasks."""
        build_wkbs = [
            bytes(w)
            for w in build_df.select(build_geom).to_pydict()[build_geom]
            if w is not None
        ]
        union_wkb = _compute_union_wkb(build_wkbs)
        if union_wkb is None:
            return None  # can't prune

        part_dirs = sorted(
            e.path for e in os.scandir(probe_dir) if e.is_dir()
        )
        kept_files, skipped = [], 0
        for pdir in part_dirs:
            files = sorted(
                os.path.join(pdir, f)
                for f in os.listdir(pdir)
                if f.endswith(".parquet")
            )
            idx = load_spatial_index(pdir)
            if idx is None or idx.geom_col != probe_geom:
                kept_files.extend(files)
            else:
                kept = idx.filter_paths(files, union_wkb)
                if kept:
                    kept_files.extend(kept)
                else:
                    skipped += 1

        total = len(part_dirs)
        total_files = sum(
            len([f for f in os.listdir(d) if f.endswith(".parquet")])
            for d in part_dirs
        )
        print(
            f"Dynamic spatial pruning: skipped {skipped}/{total} partitions "
            f"({total_files - len(kept_files)}/{total_files} files pruned)"
        )
        return kept_files

    if right_indexed and r_dir is not None:
        # Probe = right; build = left.  Materialise left geom to prune right.
        pruned_files = _prune_dir(r_dir, right_geom, left_df, left_geom)
        if pruned_files is not None:
            right_df = daft.read_parquet(pruned_files, hive_partitioning=True)
    elif left_indexed and l_dir is not None:
        # Probe = left; build = right.  Materialise right geom to prune left.
        pruned_files = _prune_dir(l_dir, left_geom, right_df, right_geom)
        if pruned_files is not None:
            left_df = daft.read_parquet(pruned_files, hive_partitioning=True)
    elif right_gh_parts and r_dir is not None:
        # Fallback: geohash prefix pruning — no sidecar index required.
        pruned_files = _geohash_prefix_pruning(r_dir, left_df, left_geom)
        if pruned_files is not None:
            right_df = daft.read_parquet(pruned_files, hive_partitioning=True)
    elif left_gh_parts and l_dir is not None:
        pruned_files = _geohash_prefix_pruning(l_dir, right_df, right_geom)
        if pruned_files is not None:
            left_df = daft.read_parquet(pruned_files, hive_partitioning=True)

    # ── Build output column list ───────────────────────────────────────────
    l_cols = left_df.schema().column_names()
    r_cols = right_df.schema().column_names()
    if output_cols is None:
        # Prefix all columns to avoid collisions
        l_select = ", ".join(f"l.{c} AS l_{c}" for c in l_cols)
        r_select = ", ".join(f"r.{c} AS r_{c}" for c in r_cols)
        select_clause = f"{l_select}, {r_select}"
    else:
        select_clause = ", ".join(output_cols)

    # ── SQL join ──────────────────────────────────────────────────────────
    # Daft requires an equality key; add a constant _jk column on both sides.
    sql = f"""
        SELECT {select_clause}
        FROM   lhs AS l
        JOIN   rhs AS r  ON l._jk = r._jk
        WHERE  {predicate}(l.{left_geom}, r.{right_geom})
    """
    return daft.sql(
        sql,
        lhs=daft.sql(f"SELECT *, 1 AS _jk FROM t", t=left_df),
        rhs=daft.sql(f"SELECT *, 1 AS _jk FROM t", t=right_df),
    )


def _compute_union_wkb(wkbs: list[bytes]) -> Optional[bytes]:
    """Compute a WKB rectangle covering the MBRs of all input geometries."""
    xs, ys = [], []
    for wkb in wkbs:
        if not wkb or len(wkb) < 5:
            continue
        bo  = wkb[0]
        e   = "<" if bo == 1 else ">"
        gt  = struct.unpack_from(e + "I", wkb, 1)[0] & 0xFFFF
        off = 5
        if gt == 1 and len(wkb) >= 21:   # Point
            x, y = struct.unpack_from(e + "dd", wkb, off)
            xs += [x]; ys += [y]
        elif gt == 3 and len(wkb) >= 13:  # Polygon
            n_rings = struct.unpack_from(e + "I", wkb, off)[0]; off += 4
            for _ in range(n_rings):
                n = struct.unpack_from(e + "I", wkb, off)[0]; off += 4
                cs = struct.unpack_from(e + "d" * (2 * n), wkb, off); off += 16 * n
                xs.extend(cs[0::2]); ys.extend(cs[1::2])
    if not xs:
        return None
    x0, y0, x1, y1 = min(xs), min(ys), max(xs), max(ys)
    return struct.pack(
        "<BIIIdddddddddd",
        1, 3, 1, 5,
        x0, y0, x1, y0, x1, y1, x0, y1, x0, y0,
    )



def load_spatial_index(directory: str) -> Optional[SpatialIndex]:
    """Load the ``_spatial_index.json`` from *directory*, or return ``None``."""
    path = os.path.join(directory, INDEX_FILENAME)
    if not os.path.exists(path):
        return None
    return SpatialIndex.load(path)


def read_parquet_spatial(
    directory: str,
    query_wkb: bytes,
    geom_col: str = "geom",
    glob_pattern: str = "*.parquet",
):
    """Read only the parquet files whose MBR overlaps *query_wkb*.

    This is the **Python-level** spatial partition pruning entry point.  It
    loads the ``_spatial_index.json`` sidecar (if present) and passes only the
    relevant file paths to ``daft.read_parquet``.

    The optimizer-level ``SpatialPartitionPruning`` rule also fires when you
    use ``daft.read_parquet`` + a spatial ``where()`` filter — this function is
    complementary: it prunes **before** Daft even creates scan tasks.

    Parameters
    ----------
    directory:
        Directory that contains the parquet files and the index sidecar.
    query_wkb:
        WKB bytes of the query geometry.
    geom_col:
        Geometry column name (must match what the index was built with).
    glob_pattern:
        File glob pattern within *directory*.

    Returns
    -------
    daft.DataFrame
        A DataFrame reading only the files that may overlap the query.
    """
    import daft

    all_paths = sorted(glob.glob(os.path.join(directory, glob_pattern)))
    if not all_paths:
        raise FileNotFoundError(f"No '{glob_pattern}' files in '{directory}'")

    index = load_spatial_index(directory)
    if index is None or index.geom_col != geom_col:
        # No index or wrong column — read everything.
        return daft.read_parquet(os.path.join(directory, glob_pattern))

    pruned = index.filter_paths(all_paths, query_wkb)
    skipped = len(all_paths) - len(pruned)
    if skipped:
        print(f"Spatial index pruned {skipped}/{len(all_paths)} file(s).")

    if not pruned:
        # All files pruned — return empty DataFrame with correct schema.
        return daft.read_parquet(all_paths[0]).where(daft.lit(False))

    return daft.read_parquet(pruned)
