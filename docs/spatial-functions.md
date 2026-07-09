# Spatial Functions Reference

Daft ships a full geospatial subsystem: a dedicated Rust crate (`src/daft-geo/`),
a Python namespace (`daft.functions`), a SQL module (`SQLModuleSpatial`), a
WKB-backed `DataType.geometry()`, GeoParquet IO, R-tree spatial joins, and
geohash/H3 partition-pruning optimizer rules.

This document catalogs every spatial **expression function**, its availability
across the Python and SQL surfaces, and the supporting infrastructure.

- **Rust core:** `src/daft-geo/` — one file per `ST_*` function; all registered
  in the `SpatialFunctions` module (`src/daft-geo/src/lib.rs`, `register()`).
- **Python:** `daft/functions/spatial.py`, re-exported from
  `daft/functions/__init__.py`.
- **SQL:** `src/daft-sql/src/modules/spatial.rs` (registered in `functions.rs`).

Function names are identical across Rust, Python, and SQL. Import from Python as
`from daft.functions import st_area, st_contains, ...`.

## Coverage at a glance

| Surface | Count |
|---|---|
| Rust `SpatialFunctions` module | **37** |
| Python bindings (`daft.functions`) | **36** |
| SQL (`ST_*`) | **35** |

### Functions NOT exposed everywhere

| Function | Python | SQL | Notes |
|---|:---:|:---:|---|
| `great_circle_distance` | ✅ | ❌ | Great-circle distance in meters from lat/lon scalars. No SQL binding. |
| `st_geohash_covers` | ❌ | ❌ | **Rust-only.** Internal optimizer helper for geohash partition pruning — not a user-facing function. |

Everything else (35 functions) is available identically in both Python and SQL.

## Function catalog

Legend: **P** = Python, **S** = SQL.

### Measurement & scalar accessors (unary)

| Function | Signature (Python) | P | S | Description |
|---|---|:-:|:-:|---|
| `st_area` | `st_area(geom, use_spheroid=False)` | ✅ | ✅ | 2D area; `use_spheroid=True` for WGS84 geodesic area. |
| `st_length` | `st_length(geom, use_spheroid=False)` | ✅ | ✅ | Length of line geometries (0 for non-lines); optional geodesic. |
| `st_x` | `st_x(geom)` | ✅ | ✅ | X / longitude of a Point. |
| `st_y` | `st_y(geom)` | ✅ | ✅ | Y / latitude of a Point. |
| `st_centroid` | `st_centroid(geom)` | ✅ | ✅ | Centroid as a Point geometry. |
| `st_bbox` | `st_bbox(geom)` | ✅ | ✅ | Bounding box as struct `{min_x, min_y, max_x, max_y}`. |
| `st_geometrytype` | `st_geometrytype(geom)` | ✅ | ✅ | Geometry type name (string). |
| `st_isvalid` | `st_isvalid(geom)` | ✅ | ✅ | OGC topological validity → bool. |

### Binary predicates (return bool)

| Function | Signature (Python) | P | S | Description |
|---|---|:-:|:-:|---|
| `st_contains` | `st_contains(geom_a, geom_b)` | ✅ | ✅ | `a` contains `b`. |
| `st_intersects` | `st_intersects(geom_a, geom_b)` | ✅ | ✅ | `a` and `b` share any point. |
| `st_within` | `st_within(geom_a, geom_b)` | ✅ | ✅ | `a` is within `b`. |
| `st_touches` | `st_touches(geom_a, geom_b)` | ✅ | ✅ | Boundaries touch, interiors don't. |
| `st_crosses` | `st_crosses(geom_a, geom_b)` | ✅ | ✅ | Geometries cross. |
| `st_overlaps` | `st_overlaps(geom_a, geom_b)` | ✅ | ✅ | Geometries overlap (same dimension). |
| `st_disjoint` | `st_disjoint(geom_a, geom_b)` | ✅ | ✅ | No shared points. |
| `st_equals` | `st_equals(geom_a, geom_b)` | ✅ | ✅ | Spatially equal. |
| `st_covers` | `st_covers(geom_a, geom_b)` | ✅ | ✅ | `a` covers `b`. |
| `st_covered_by` | `st_covered_by(geom_a, geom_b)` | ✅ | ✅ | `a` is covered by `b`. |
| `st_dwithin` | `st_dwithin(geom_a, geom_b, distance)` | ✅ | ✅ | True if within `distance` of each other. |

### Distance

| Function | Signature (Python) | P | S | Description |
|---|---|:-:|:-:|---|
| `st_distance` | `st_distance(geom_a, geom_b, use_spheroid=False)` | ✅ | ✅ | Minimum distance; `use_spheroid=True` for WGS84 geodesic (point pairs only). |
| `great_circle_distance` | `great_circle_distance(lat1, lon1, lat2, lon2)` | ✅ | ❌ | Great-circle distance in meters from lat/lon columns. |

### Set / overlay operations (return geometry)

| Function | Signature (Python) | P | S | Description |
|---|---|:-:|:-:|---|
| `st_union` | `st_union(geom_a, geom_b)` | ✅ | ✅ | Union of two geometries. |
| `st_intersection` | `st_intersection(geom_a, geom_b)` | ✅ | ✅ | Intersection. |
| `st_difference` | `st_difference(geom_a, geom_b)` | ✅ | ✅ | Difference (`a` minus `b`). |
| `st_symdifference` | `st_symdifference(geom_a, geom_b)` | ✅ | ✅ | Symmetric difference (XOR). Polygon/MultiPolygon only. |

### Geometry-producing transforms

| Function | Signature (Python) | P | S | Description |
|---|---|:-:|:-:|---|
| `st_envelope` | `st_envelope(geom)` | ✅ | ✅ | Minimum bounding rectangle as a polygon. |
| `st_convexhull` | `st_convexhull(geom)` | ✅ | ✅ | Convex hull polygon. |
| `st_simplify` | `st_simplify(geom, tolerance)` | ✅ | ✅ | Ramer–Douglas–Peucker simplification. |
| `st_buffer` | `st_buffer(geom, distance)` | ✅ | ✅ | Planar buffer by `distance`. |

### Constructors

| Function | Signature (Python) | P | S | Description |
|---|---|:-:|:-:|---|
| `st_point` | `st_point(x, y)` | ✅ | ✅ | Point from x, y columns. |
| `st_makeline` | `st_makeline(geom_a, geom_b)` | ✅ | ✅ | LineString from two Points. |

### Text / format conversion

| Function | Signature (Python) | P | S | Description |
|---|---|:-:|:-:|---|
| `st_geomfromtext` | `st_geomfromtext(wkt)` | ✅ | ✅ | Parse WKT → geometry. |
| `st_astext` | `st_astext(geom)` | ✅ | ✅ | Geometry → WKT. |
| `st_geomfromgeojson` | `st_geomfromgeojson(geojson)` | ✅ | ✅ | Parse GeoJSON → geometry. |
| `st_geojsonfromgeom` | `st_geojsonfromgeom(geom)` | ✅ | ✅ | Geometry → GeoJSON. |

### Geohash

| Function | Signature (Python) | P | S | Description |
|---|---|:-:|:-:|---|
| `st_geohash` | `st_geohash(geom, precision=5)` | ✅ | ✅ | Geohash string of the geometry's centroid. |
| `st_geohash_covers` | _(internal)_ | ❌ | ❌ | Rust-only geohash-covering predicate for partition pruning. |

## Usage examples

### Python

```python
import daft
from daft.functions import st_geomfromtext, st_area, st_contains, st_distance

df = daft.from_pydict({
    "wkt": ["POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))", "POINT (1 1)"],
})
df = df.with_column("geom", st_geomfromtext(df["wkt"]))
df = df.with_column("area", st_area(df["geom"]))

# geodesic distance
df.with_column("dist", st_distance(df["geom"], df["geom"], use_spheroid=True))
```

### SQL

```sql
SELECT
    ST_Area(geom)                     AS area,
    ST_Contains(region, ST_Point(x, y)) AS in_region
FROM shapes
WHERE ST_DWithin(a, b, 100.0)
```

## Supporting infrastructure (not expression functions)

- **Geometry type:** `DataType.geometry()` — WKB-encoded
  (`daft/datatype.py`).
- **DataFrame helper:** `df.with_spatial_bbox(geom_col)` — adds `rtree_*`
  bounding-box columns (`daft/dataframe/dataframe.py`).
- **Spatial joins:** R-tree nested-loop operator
  (`src/daft-recordbatch/src/ops/joins/nested_loop_join.rs`); optimizer rules
  `geohash_pruning.rs`, `spatial_partition_pruning.rs`, `collocated_join.rs`
  under `src/daft-logical-plan/src/optimization/rules/`. See
  [Spatial Joins](optimization/spatial-joins.md).
- **Geohash / H3 partition pruning:** `st_geohash_covers` (Rust) plus H3 helpers
  in `src/daft-geo/src/h3_index.rs`.
- **Spatial index utilities (Python):** `build_spatial_index`,
  `load_spatial_index`, `read_parquet_spatial`, `spatial_join`, and the
  `SpatialIndex` class in `daft/functions/spatial_index.py`.
- **GeoParquet IO:** `daft/io/_geoparquet.py` (`build_geo_metadata`,
  `detect_geo_columns`, `attach_geo_field_metadata`).

## Source-of-truth files

| Concern | File |
|---|---|
| Rust function registration | `src/daft-geo/src/lib.rs` (`register()`) |
| Rust function implementations | `src/daft-geo/src/st_*.rs` |
| Python bindings | `daft/functions/spatial.py` |
| Python re-exports | `daft/functions/__init__.py` |
| SQL registration | `src/daft-sql/src/modules/spatial.rs` |
| Spatial index utilities | `daft/functions/spatial_index.py` |
| Tests | `tests/expressions/test_spatial.py`, `tests/expressions/test_spatial_join.py`, `tests/expressions/test_spatial_geohash_pruning.py` |
