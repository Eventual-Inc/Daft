"""Spatial index utilities for partition-level pruning.

Overview
--------
A *spatial index* is a small inverted-Parquet sidecar file
(``_spatial_index.idx``) stored alongside a directory of parquet files.  It records the spatial
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
    # → writes output/_spatial_index.idx  (H3 inverted Parquet)

    # 3. Query — pruning is automatic
    import daft
    from daft.functions.spatial import st_intersects
    df = daft.read_parquet("output/*.parquet")
    result = df.where(st_intersects(col("geom"), lit(query_wkb)))

Index format (v1)
-----------------
Stored as an **inverted Parquet file** (``_spatial_index.idx``) with two
columns::

    h3_cell  STRING   — H3 cell hex string (e.g. "8a1fb46ffffffff")
    filename STRING   — basename of the parquet file covering that cell

The file also carries schema-level metadata::

    b"geom_col"      → column name bytes
    b"h3_resolution" → resolution as decimal bytes

Using an inverted layout means query-time pruning can filter to only the
rows whose ``h3_cell`` matches the query — no need to load the full index.
Cell strings use H3's standard hex representation (15 hex characters).
"""

from __future__ import annotations

import glob
import os
import struct
from typing import Optional

__all__ = [
    "build_spatial_index",
    "load_spatial_index",
    "spatial_join",
    "SpatialIndex",
]

INDEX_FILENAME = "_spatial_index.idx"
INDEX_VERSION_H3 = 1
DEFAULT_H3_RESOLUTION = 7


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
    flags = struct.unpack_from(e + "I", buf, offset + 1)[0]
    geom_type = flags & 0xFFFF
    offset += 5
    if flags & 0x20000000:  # EWKB: skip 4-byte SRID
        offset += 4
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
    flags = struct.unpack_from(e + "I", buf, offset + 1)[0]
    geom_type = flags & 0xFFFF
    offset += 5
    if flags & 0x20000000:  # EWKB: skip 4-byte SRID
        offset += 4

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

# Approximate H3 cell edge length in degrees for each resolution (equatorial).
# Used to compute grid density required to have ≤ one cell-width between samples
# (prevents false negatives from sparse grid sampling).
# Derived from: edge_km = [1281,483,182.5,68.8,25.9,9.78,3.69,1.39,0.524,0.198,0.0747,0.0282,0.0107,0.00401,0.00152,0.000572]
# 1 deg lat ≈ 111km, edge_deg = edge_km / 111
_H3_EDGE_DEG = [
    11.54, 4.35, 1.64, 0.620, 0.234, 0.0881, 0.0332, 0.0125,
    0.00472, 0.00178, 0.000673, 0.000254, 0.0000964, 0.0000361, 0.0000137, 0.00000515,
]


def _estimate_polyfill_cells(x0: float, y0: float, x1: float, y1: float, resolution: int) -> int:
    """Rough upper-bound estimate of H3 polyfill count from an MBR."""
    import math

    area_m2 = (x1 - x0) * (y1 - y0) * (111_000 ** 2)
    cell_area = _H3_CELL_AREA_M2[min(resolution, 15)]
    ratio = area_m2 / cell_area
    if not math.isfinite(ratio):
        return 2**63 - 1
    return max(1, int(ratio))


def _batch_point_h3_cells(
    wkbs: list,
    h3lib,
    is_v4: bool,
    resolution: int,
) -> set[str]:
    """Fast batch H3 cell extraction for uniform 21-byte LE point WKBs using numpy.

    Skips per-row WKB parsing entirely; extracts all (lon, lat) coordinates
    in a single numpy buffer read.
    """
    import numpy as np

    valid = [bytes(w) for w in wkbs if w is not None and len(w) == 21]
    if not valid:
        return set()
    buf = np.frombuffer(b"".join(valid), dtype=np.uint8).reshape(-1, 21)
    lons = np.ascontiguousarray(buf[:, 5:13]).view("<f8").flatten()
    lats = np.ascontiguousarray(buf[:, 13:21]).view("<f8").flatten()
    if is_v4:
        return {
            h3lib.latlng_to_cell(float(la), float(lo), resolution)
            for lo, la in zip(lons.tolist(), lats.tolist())
        }
    return {
        h3lib.geo_to_h3(float(la), float(lo), resolution)
        for lo, la in zip(lons.tolist(), lats.tolist())
    }


def _batch_rect_h3_cells(
    wkbs: list,
    h3lib,
    is_v4: bool,
    resolution: int,
    max_polyfill: int = 300,
) -> set[str]:
    """Fast per-row H3 cells for uniform 93-byte LE axis-aligned polygon WKBs.

    Uses numpy to batch-extract all ``(x0, y0, x1, y1)`` corner coordinates,
    then for each row either polyfills (small polygon) or samples a small
    ``n × n`` grid (large polygon).  Avoids re-parsing WKB in Python.

    The *max_polyfill* threshold controls the polyfill/grid switch; keeping it
    low (≤ 500) avoids generating millions of cells for large polygons.
    """
    import numpy as np

    valid = [bytes(w) for w in wkbs if w is not None and len(w) == 93]
    if not valid:
        return set()

    buf = np.frombuffer(b"".join(valid), dtype=np.uint8).reshape(-1, 93)
    x0l = np.ascontiguousarray(buf[:, 13:21]).view("<f8").flatten().tolist()
    y0l = np.ascontiguousarray(buf[:, 21:29]).view("<f8").flatten().tolist()
    x1l = np.ascontiguousarray(buf[:, 45:53]).view("<f8").flatten().tolist()
    y1l = np.ascontiguousarray(buf[:, 53:61]).view("<f8").flatten().tolist()

    cells: set[str] = set()
    disk_fn = h3lib.grid_disk if is_v4 else h3lib.k_ring

    for i in range(len(valid)):
        vx0, vy0, vx1, vy1 = x0l[i], y0l[i], x1l[i], y1l[i]
        estimated = _estimate_polyfill_cells(vx0, vy0, vx1, vy1, resolution)

        if estimated <= max_polyfill:
            outer_latlng = [(vy0, vx0), (vy0, vx1), (vy1, vx1), (vy1, vx0), (vy0, vx0)]
            try:
                if is_v4:
                    interior = set(h3lib.polygon_to_cells(h3lib.LatLngPoly(outer_latlng), resolution))
                else:
                    gj = {"type": "Polygon", "coordinates": [[[lon, lat] for lat, lon in outer_latlng]]}
                    interior = set(h3lib.polyfill_geojson(gj, resolution))
                cells.update(interior)
            except Exception:
                pass
        else:
            # Grid sampling: spacing ≤ one H3 cell edge so no cell is skipped.
            edge_deg = _H3_EDGE_DEG[min(resolution, 15)]
            n_lon = max(3, int((vx1 - vx0) / edge_deg) + 2)
            n_lat = max(3, int((vy1 - vy0) / edge_deg) + 2)
            dx = (vx1 - vx0) / max(n_lon - 1, 1)
            dy = (vy1 - vy0) / max(n_lat - 1, 1)
            for gi in range(n_lon):
                for gj in range(n_lat):
                    c = _cell_for_point(h3lib, is_v4, vy0 + gj * dy, vx0 + gi * dx, resolution)
                    cells.add(c)

        # Always cover corners + center with k=1 disk for edge safety
        cx, cy = (vx0 + vx1) * 0.5, (vy0 + vy1) * 0.5
        for lon, lat in [(vx0, vy0), (vx1, vy0), (vx1, vy1), (vx0, vy1), (cx, cy)]:
            c = _cell_for_point(h3lib, is_v4, lat, lon, resolution)
            cells.update(disk_fn(c, 1))

    return cells


def _file_mbr_from_wkbs(wkbs: list) -> Optional[tuple[float, float, float, float]]:
    """Compute the union MBR of all WKBs in a file.

    Fast path: if all WKBs are uniform 21-byte (points) or 93-byte (LE
    axis-aligned rectangles) use numpy for bulk extraction.  Falls back to
    the row-by-row ``_wkb_mbr`` parser for heterogeneous/complex geometries.
    Returns ``(min_x, min_y, max_x, max_y)`` or ``None`` if nothing valid.
    """
    import numpy as np

    def _to_bytes(w) -> bytes:
        if isinstance(w, (bytes, bytearray, memoryview)):
            return bytes(w)
        # Hex string stored in parquet as utf8/large_string
        return bytes.fromhex(w)

    valid = [_to_bytes(w) for w in wkbs if w is not None]
    if not valid:
        return None

    sizes = {len(w) for w in valid}

    if sizes == {21}:
        # All points (21 bytes LE)
        buf = np.frombuffer(b"".join(valid), dtype=np.uint8).reshape(-1, 21)
        lons = np.ascontiguousarray(buf[:, 5:13]).view("<f8").flatten()
        lats = np.ascontiguousarray(buf[:, 13:21]).view("<f8").flatten()
        return float(lons.min()), float(lats.min()), float(lons.max()), float(lats.max())

    if sizes == {25}:
        # All EWKB points with SRID (25 bytes): 1 endian + 4 flags + 4 SRID + 8 x + 8 y
        buf = np.frombuffer(b"".join(valid), dtype=np.uint8).reshape(-1, 25)
        lons = np.ascontiguousarray(buf[:, 9:17]).view("<f8").flatten()
        lats = np.ascontiguousarray(buf[:, 17:25]).view("<f8").flatten()
        return float(lons.min()), float(lats.min()), float(lons.max()), float(lats.max())

    if sizes == {93}:
        # All LE axis-aligned rectangles (93 bytes: geom hdr + ring hdr + 5 × (x,y))
        buf = np.frombuffer(b"".join(valid), dtype=np.uint8).reshape(-1, 93)
        x0 = np.ascontiguousarray(buf[:, 13:21]).view("<f8").flatten()
        y0 = np.ascontiguousarray(buf[:, 21:29]).view("<f8").flatten()
        x1 = np.ascontiguousarray(buf[:, 45:53]).view("<f8").flatten()
        y1 = np.ascontiguousarray(buf[:, 53:61]).view("<f8").flatten()
        return float(x0.min()), float(y0.min()), float(x1.max()), float(y1.max())

    # Fallback: parse each WKB individually
    mbrs = [_wkb_mbr(w) for w in valid]
    mbrs = [m for m in mbrs if m is not None]
    if not mbrs:
        return None
    return (
        min(m[0] for m in mbrs), min(m[1] for m in mbrs),
        max(m[2] for m in mbrs), max(m[3] for m in mbrs),
    )


# Maximum total H3 calls allowed when building the file-level MBR grid.
# At ~300K H3 calls/sec in Python, 10K = ~33ms per file.
# Resolutions > 8 on large partition files exceed this limit.
_MAX_INDEX_GRID_CELLS = 10_000


def _h3_cells_for_file_mbr(
    h3lib,
    is_v4: bool,
    x0: float, y0: float,
    x1: float, y1: float,
    resolution: int,
) -> set[str]:
    """Return H3 cells covering the bounding box at *resolution*.

    The caller is responsible for ensuring *resolution* is safe for the given
    MBR size (see ``_safe_resolution_for_mbr``).  Uses ``polygon_to_cells``
    when the estimated cell count is ≤ 5 000; otherwise samples a grid with
    spacing = one H3 cell edge (no false negatives, no gaps).
    """
    import numpy as np

    estimated = _estimate_polyfill_cells(x0, y0, x1, y1, resolution)
    cells: set[str] = set()

    if estimated <= 5_000:
        ring_latlng = [(y0, x0), (y0, x1), (y1, x1), (y1, x0), (y0, x0)]
        try:
            if is_v4:
                poly = h3lib.LatLngPoly(ring_latlng)
                cells = set(h3lib.polygon_to_cells(poly, resolution))
            else:
                geojson = {
                    "type": "Polygon",
                    "coordinates": [[[lon, lat] for lat, lon in ring_latlng]],
                }
                cells = set(h3lib.polyfill_geojson(geojson, resolution))
        except Exception:
            pass

    if not cells:
        # Grid sampling: spacing ≤ one H3 cell edge — no gaps.
        edge_deg = _H3_EDGE_DEG[resolution]
        n_lon = max(3, int((x1 - x0) / edge_deg) + 2)
        n_lat = max(3, int((y1 - y0) / edge_deg) + 2)
        lons = np.linspace(x0, x1, n_lon)
        lats = np.linspace(y0, y1, n_lat)
        lon_g, lat_g = np.meshgrid(lons, lats)
        lo_list = lon_g.flatten().tolist()
        la_list = lat_g.flatten().tolist()
        if is_v4:
            cells = {
                h3lib.latlng_to_cell(float(la), float(lo), resolution)
                for lo, la in zip(lo_list, la_list)
            }
        else:
            cells = {
                h3lib.geo_to_h3(float(la), float(lo), resolution)
                for lo, la in zip(lo_list, la_list)
            }

    # Edge expansion: k=1 disk around all corner cells
    corners = [
        (y0, x0), (y0, x1), (y1, x0), (y1, x1),
        ((y0 + y1) * 0.5, (x0 + x1) * 0.5),
    ]
    for lat, lon in corners:
        c = _cell_for_point(h3lib, is_v4, lat, lon, resolution)
        cells.update(_disk(h3lib, is_v4, c, 1))

    return cells


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
    max_cells_per_file: Optional[int] = None,
) -> dict:
    """Scan every parquet file in *directory* and write ``_spatial_index.idx``.

    Parameters
    ----------
    directory:
        Directory containing the parquet files.
    geom_col:
        Name of the WKB-encoded geometry column.
    output_path:
        Path for the index file.  Defaults to
        ``{directory}/_spatial_index.idx``.
    glob_pattern:
        Glob pattern for parquet files within *directory*.
    h3_resolution:
        H3 resolution (0–15).  Default 10 (~65 m cell edge).
        Increase for finer precision at the cost of larger index files.
    max_cells_per_file:
        Maximum number of H3 cells stored per parquet file in the index.
        When the full cell set is larger, a random sample of this size is
        kept, trading recall for a smaller index.  Default ``None`` (no cap).

    Returns
    -------
    dict
        ``{filename: [cell, ...]}`` mapping that was written to the index.
    """
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


def _safe_resolution_for_mbr(
    x0: float, y0: float, x1: float, y1: float, resolution: int
) -> int:
    """Return the highest resolution ≤ *resolution* that keeps the MBR within
    ``_MAX_INDEX_GRID_CELLS`` H3 calls (polyfill or grid)."""
    res = resolution
    while res > 0:
        estimated = _estimate_polyfill_cells(x0, y0, x1, y1, res)
        if estimated <= 5_000:
            break  # polyfill path — always fine
        edge = _H3_EDGE_DEG[res]
        n_lon = max(3, int((x1 - x0) / edge) + 2)
        n_lat = max(3, int((y1 - y0) / edge) + 2)
        if n_lon * n_lat <= _MAX_INDEX_GRID_CELLS:
            break
        res -= 1
    return res


def _build_h3_index(
    parquet_files: list[str],
    geom_col: str,
    h3lib,
    is_v4: bool,
    resolution: int,
    output_path: str,
    max_cells_per_file: Optional[int] = None,
) -> dict[str, list[str]]:
    import warnings
    import pyarrow as pa
    import pyarrow.parquet as pq
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # ── Pass 1: MBR extraction (cheap path first) ─────────────────────────
    # If the file has pre-computed bbox columns we read only those 4 float columns — no WKB
    # parsing, minimal memory. We prefer the canonical ``rtree_*`` set produced by
    # ``df.with_spatial_bbox()`` and fall back to the legacy ``min_*``/``bbox_*`` names.
    # Otherwise we read only the geometry column and parse WKBs.
    # We also detect pure-point files (all 21-byte WKBs) for pass 2.
    _BBOX_COL_SETS = (
        ("rtree_min_x", "rtree_min_y", "rtree_max_x", "rtree_max_y"),
        ("min_x", "min_y", "max_x", "max_y"),
        ("bbox_min_x", "bbox_min_y", "bbox_max_x", "bbox_max_y"),
    )

    def _scan_file(fpath: str):
        """Return (mbr, is_all_points).  Never caches raw WKBs."""
        pf = pq.ParquetFile(fpath)
        schema_names = {f.name for f in pf.schema_arrow}

        bbox_cols = next((s for s in _BBOX_COL_SETS if all(c in schema_names for c in s)), None)
        if bbox_cols is not None:
            # Fast path: read pre-computed bbox floats only
            tbl = pf.read(columns=list(bbox_cols))
            xs0 = [v for v in tbl[bbox_cols[0]].to_pylist() if v is not None]
            ys0 = [v for v in tbl[bbox_cols[1]].to_pylist() if v is not None]
            xs1 = [v for v in tbl[bbox_cols[2]].to_pylist() if v is not None]
            ys1 = [v for v in tbl[bbox_cols[3]].to_pylist() if v is not None]
            if not xs0:
                return None, False
            mbr = (min(xs0), min(ys0), max(xs1), max(ys1))
            # Polygons always have bbox cols; treat as non-point
            return mbr, False

        # Fallback: read geometry column and parse
        tbl = pf.read(columns=[geom_col])
        wkbs = tbl[geom_col].to_pylist()
        mbr = _file_mbr_from_wkbs(wkbs)
        non_null = [w for w in wkbs if w is not None]
        sizes = {len(w) for w in non_null} if non_null else set()
        return mbr, sizes in ({21}, {25})

    # Parallelism is safe: peak memory per worker = 4 float cols (fast path)
    # or one file's WKBs (fallback path) — never all files at once.
    workers = min(len(parquet_files), (os.cpu_count() or 4), 4)
    # scan_results: {fpath: (mbr, is_all_points)}
    scan_results: dict = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_scan_file, fp): fp for fp in parquet_files}
        for fut in as_completed(futs):
            scan_results[futs[fut]] = fut.result()

    per_file_mbr = {fp: r[0] for fp, r in scan_results.items()}

    # ── Determine a single safe resolution for ALL files ──────────────────
    # Resolution capping only matters for polygon/MBR-indexed files (those that
    # go through polyfill or grid-walk).  Pure-point files map each point to
    # exactly one cell at any resolution, so their MBR size is irrelevant.
    all_points = all(r[1] for r in scan_results.values() if r[0] is not None)
    valid_mbrs = [m for m in per_file_mbr.values() if m is not None]
    if valid_mbrs and not all_points:
        ux0 = min(m[0] for m in valid_mbrs)
        uy0 = min(m[1] for m in valid_mbrs)
        ux1 = max(m[2] for m in valid_mbrs)
        uy1 = max(m[3] for m in valid_mbrs)
        effective_resolution = _safe_resolution_for_mbr(ux0, uy0, ux1, uy1, resolution)
    else:
        effective_resolution = resolution

    if effective_resolution != resolution:
        warnings.warn(
            f"Index resolution automatically adjusted from {resolution} to "
            f"{effective_resolution} because the data MBR is too large for fine-"
            f"resolution indexing within the {_MAX_INDEX_GRID_CELLS:,}-cell budget.  "
            f"For this dataset, h3_resolution ≤ {effective_resolution} avoids this "
            f"adjustment.  The index is still complete (no false negatives).",
            UserWarning,
            stacklevel=4,
        )

    # ── Pass 2: compute H3 cells per file, re-reading WKBs for points ──────
    # For polygon/mixed files the MBR from pass 1 is reused (no re-read).
    # For point files we re-read the file: only one file's WKBs live in
    # memory at a time per worker, keeping peak usage bounded.
    def _index_one_file(fpath: str) -> tuple[str, list[str]]:
        mbr, is_pts = scan_results[fpath]
        cells: set[str] = set()
        if is_pts:
            tbl = pq.ParquetFile(fpath).read(columns=[geom_col])
            cells = _batch_point_h3_cells(
                tbl[geom_col].to_pylist(), h3lib, is_v4, effective_resolution
            )
        elif mbr is not None:
            cells = _h3_cells_for_file_mbr(h3lib, is_v4, *mbr, effective_resolution)

        cell_list = sorted(cells)
        if max_cells_per_file is not None and len(cell_list) > max_cells_per_file:
            import random
            cell_list = sorted(random.sample(cell_list, max_cells_per_file))
        return os.path.basename(fpath), cell_list

    file_cells: dict[str, list[str]] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_index_one_file, fp): fp for fp in parquet_files}
        for fut in as_completed(futs):
            basename, cell_list = fut.result()
            if cell_list:
                file_cells[basename] = cell_list

    # ── Write inverted Parquet index ───────────────────────────────────────
    h3_cells_col: list[str] = []
    filenames_col: list[str] = []
    for basename, cell_list in sorted(file_cells.items()):
        h3_cells_col.extend(cell_list)
        filenames_col.extend([basename] * len(cell_list))

    schema = pa.schema(
        [
            pa.field("h3_cell", pa.dictionary(pa.int32(), pa.utf8())),
            pa.field("filename", pa.dictionary(pa.int32(), pa.utf8())),
        ],
        metadata={
            b"geom_col": geom_col.encode(),
            b"h3_resolution": str(effective_resolution).encode(),
        },
    )
    table = pa.table(
        {
            "h3_cell": pa.array(h3_cells_col, type=pa.dictionary(pa.int32(), pa.utf8())),
            "filename": pa.array(filenames_col, type=pa.dictionary(pa.int32(), pa.utf8())),
        },
        schema=schema,
    )
    pq.write_table(table, output_path, compression="zstd")
    total_cells = len(h3_cells_col)
    eff_str = (
        f"res={effective_resolution}"
        + (f" (requested {resolution}, adjusted)" if effective_resolution != resolution else "")
    )
    print(
        f"H3 spatial index (v1, {eff_str}, "
        f"max_cells={'unlimited' if max_cells_per_file is None else max_cells_per_file}) "
        f"written to '{output_path}' ({len(file_cells)} files, {total_cells:,} cell-file pairs)."
    )
    return file_cells




# ── Index querying ────────────────────────────────────────────────────────


class SpatialIndex:
    """In-memory representation of a ``_spatial_index.idx`` sidecar (H3 format).

    Attributes
    ----------
    geom_col : str
        Geometry column this index was built for.
    h3_resolution : int
        H3 resolution the index was built at.
    index_path : str
        Path to the ``.idx`` Parquet file (used for lazy queries).
    file_h3_cells : dict
        ``{basename: [cell_str, ...]}`` — H3 cells covering each file.
        A property: accessing it triggers a full materialization of the
        index sidecar on first access if it hasn't been loaded yet (see
        ``load_full``). Prefer ``filter_paths`` for query-time pruning,
        which reads only the relevant rows instead of the whole sidecar.
    """

    def __init__(
        self,
        geom_col: str,
        *,
        file_h3_cells: Optional[dict[str, list[str]]] = None,
        h3_resolution: Optional[int] = None,
        index_path: Optional[str] = None,
    ):
        self.geom_col = geom_col
        self._file_h3_cells: dict[str, list[str]] = file_h3_cells or {}
        self.h3_resolution = h3_resolution
        self.index_path = index_path

    @classmethod
    def load(cls, path: str) -> "SpatialIndex":
        """Load a ``_spatial_index.idx`` Parquet file (lazy — metadata only)."""
        import pyarrow.parquet as pq
        meta = pq.read_schema(path).metadata or {}
        geom_col = (meta.get(b"geom_col") or b"geom").decode()
        resolution_raw = meta.get(b"h3_resolution")
        h3_resolution = int(resolution_raw.decode()) if resolution_raw else None
        # Don't load all rows yet — filter_paths will query lazily
        return cls(geom_col=geom_col, h3_resolution=h3_resolution, index_path=path)

    @property
    def file_h3_cells(self) -> dict[str, list[str]]:
        """``{basename: [cell_str, ...]}`` — H3 cells covering each file.

        Triggers ``load_full`` on first access to materialise the full
        forward dict from the ``.idx`` sidecar. Prefer ``filter_paths`` for
        query-time pruning, which reads only the relevant rows.
        """
        self.load_full()
        return self._file_h3_cells

    def load_full(self) -> "SpatialIndex":
        """Eagerly materialise the full forward ``{filename: [cell, ...]}`` map.

        Populates ``file_h3_cells`` (and this call becomes a no-op) once the
        sidecar has been loaded, or immediately if ``index_path`` is unset.
        Useful for repr / introspection / debugging, where the whole index is
        needed rather than the lazy, query-scoped reads that ``filter_paths``
        performs.

        Returns
        -------
        SpatialIndex
            ``self``, for chaining, e.g. ``SpatialIndex.load(path).load_full()``.
        """
        if self._file_h3_cells or self.index_path is None:
            return self
        import pyarrow.parquet as pq
        tbl = pq.read_table(self.index_path, columns=["h3_cell", "filename"])
        d: dict[str, list[str]] = {}
        for cell, fname in zip(tbl["h3_cell"].to_pylist(), tbl["filename"].to_pylist()):
            d.setdefault(fname, []).append(cell)
        self._file_h3_cells = d
        return self

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
        if h3lib is None or self.h3_resolution is None or self.index_path is None:
            return full_paths

        query_cells = _h3_cells_for_wkb(query_wkb, h3lib, is_v4, self.h3_resolution)
        if not query_cells:
            return full_paths

        # Query inverted index lazily — read only rows whose h3_cell is in the
        # query set; no need to load the full index into memory.
        import pyarrow.parquet as pq
        import pyarrow.compute as pc
        tbl = pq.read_table(
            self.index_path,
            columns=["h3_cell", "filename"],
            filters=[("h3_cell", "in", list(query_cells))],
        )
        matched_basenames: set[str] = set(tbl["filename"].to_pylist())

        result = []
        for path in full_paths:
            basename = os.path.basename(path)
            # Keep if matched by index, or if not in index at all (conservative)
            if basename in matched_basenames or basename not in self._all_indexed_files():
                result.append(path)
        return result

    def _all_indexed_files(self) -> set[str]:
        """Return set of all filenames present in the index (cached)."""
        if not hasattr(self, "_indexed_files_cache"):
            if self.index_path is None:
                self._indexed_files_cache: set[str] = set()
            else:
                import pyarrow.parquet as pq
                tbl = pq.read_table(self.index_path, columns=["filename"])
                self._indexed_files_cache = set(tbl["filename"].unique().to_pylist())
        return self._indexed_files_cache

    def __repr__(self) -> str:
        return (
            f"SpatialIndex(H3, geom_col={self.geom_col!r}, "
            f"resolution={self.h3_resolution}, path={self.index_path!r})"
        )


def load_spatial_index(directory: str) -> Optional[SpatialIndex]:
    """Load the ``_spatial_index.idx`` from *directory*, or return ``None``."""
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

    Loads the ``_spatial_index.idx`` sidecar (if present) and passes only
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

    1. Detects which side has a ``_spatial_index.idx`` (the *probe* side).
    2. Materialises the *build* side's geometry column (the other side).
    3. Computes a union bounding geometry from the build side.
    4. Uses the spatial index to skip probe-side partitions that cannot
       possibly overlap the build-side geometries.
    5. Executes the SQL join over the pruned file set.

    Parameters
    ----------
    left, right:
        Either a ``daft.DataFrame`` or a directory path (str) containing
        hive-partitioned parquet files with a ``_spatial_index.idx`` sidecar.
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



