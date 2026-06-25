# Spatial Sub-project A: Geometry Function Completeness & Engine

- **Date:** 2026-06-24
- **Status:** Approved design, ready for implementation planning
- **Scope:** `src/daft-geo`, `src/daft-sql/src/modules/spatial.rs`, `daft/functions/spatial.py`, `daft/functions/__init__.py`, `tests/`
- **Part of:** a 3-sub-project geospatial effort. This is **Sub-project A** (functions & engine). Sub-project B (GeoParquet I/O + Delta) and Sub-project C (R-tree index + native spatial join) are separate spec → plan → build cycles that follow this one. C depends on the predicates hardened here.

## Summary

Daft's geospatial layer (the pure-Rust [`daft-geo`](../../../src/daft-geo) crate) today has correct WKB/WKT/GeoJSON handling, geohash/H3 pruning, and a handful of predicates and measures — but the predicates fall through to `false` for unhandled geometry-type pairs, distance/area/length are planar-only, `st_buffer` is a fake bounding-box envelope, `st_isvalid` always returns `true`, several functions are SQL-only (no Python wrapper), and many standard `ST_*` functions are missing.

This sub-project fills those gaps **on the existing pure-Rust `geo` engine** (no new C dependency): correct DE-9IM predicates for all type pairs, the missing predicates, true WGS84 geodesic distance/length/area, a real planar buffer, real validity, the overlay/processing/constructor functions, and full Python⇄SQL parity (including Python wrappers for the currently SQL-only functions).

## Goals

- Every spatial predicate is correct for **all** geometry-type pairs (via DE-9IM `Relate`), not just the few hand-coded combinations.
- Add the missing predicates: `st_touches`, `st_crosses`, `st_overlaps`, `st_disjoint`, `st_equals`.
- True **geodesic** distance / length / area (WGS84 spheroid), exposed via a non-breaking `use_spheroid` parameter (planar remains the default).
- A real **planar** `st_buffer` (replacing the bbox-envelope stub) and real `st_isvalid`.
- Overlay + processing + constructor functions: `st_union`, `st_intersection`, `st_difference`, `st_symdifference`, `st_envelope`, `st_convexhull`, `st_simplify`, `st_point`, `st_makeline`.
- **Python⇄SQL parity**: every `ST_*` function callable from both the Python API and SQL, including Python wrappers for the currently SQL-only `st_astext` / `st_geomfromtext` / `st_geomfromgeojson` / `st_geojsonfromgeom`.
- No new C/system dependency; Daft stays pure-Rust for clean cross-platform wheels.

## Non-goals

- **Geodesic buffer** — pure-Rust has no robust geodesic buffer; `st_buffer` is planar (distance in coordinate units). Documented limitation.
- **CRS / coordinate transforms** (`st_transform`, `st_setsrid`, reprojection) — no PROJ dependency. Out of scope.
- **GeoParquet / shapefile I/O** — Sub-project B.
- **R-tree index and native spatial-join operator** — Sub-project C.
- **A `col.geo.*` expression namespace** — functions stay top-level in `daft.functions`, consistent with the current code.
- **3D/Z/M-aware predicates** — Z/M coordinates are parsed but predicates operate in 2D.
- **Spatial aggregates** (e.g. `st_union` as an aggregate / `st_collect`) — scalar functions only here.

## Decisions (resolved during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Geometry engine | **Pure-Rust `geo`** (Relate + BooleanOps + Geodesic) | No C dependency; `geo` is richer on geodesic than GEOS (which is planar-only); preserves Daft's pure-Rust wheel packaging. |
| Geodesic API shape | **`use_spheroid` parameter** (default `False`) on `st_distance`/`st_area`/`st_length` | Compact, discoverable, non-breaking; works in SQL like the existing optional `st_geohash(geom, precision)`. |
| Buffer | **Planar** via a pure-Rust offset crate; geodesic buffer out of scope | The one thing pure-Rust can't do robustly; planar is correct for projected data and honest. |
| Scope | **Core + optional extras** (overlay/processing/constructors included) | User opted in; `geo` makes them cheap and uniform. |
| API namespace | **Top-level `daft.functions`** (no `col.geo.*`) | Consistent with current code; YAGNI. |

## Current state (baseline)

- Crate [`src/daft-geo`](../../../src/daft-geo): one file per function, registered via a `FunctionModule` in [lib.rs](../../../src/daft-geo/src/lib.rs); shared helpers (`unary_geom_*`, `binary_geom_*`, WKB/EWKB parsing) in [utils.rs](../../../src/daft-geo/src/utils.rs). Geometry stored as WKB under `DataType::Geometry` (logical over Binary); EWKB SRID stripped.
- Existing functions: `st_area`, `st_length`, `st_centroid`, `st_x`, `st_y`, `st_geometrytype`, `st_isvalid`, `st_intersects`, `st_contains`, `st_within`, `st_distance`, `st_buffer`, `st_geohash`, `st_geohash_covers`, `st_astext`, `st_geomfromtext`, `st_geomfromgeojson`, `st_geojsonfromgeom`, plus `great_circle_distance`.
- Gaps being addressed: predicates fall through to `false`/`NaN` for unhandled type pairs ([st_contains.rs:15](../../../src/daft-geo/src/st_contains.rs), [st_distance.rs:18](../../../src/daft-geo/src/st_distance.rs)); `st_buffer` is a bbox envelope ([st_buffer.rs:17](../../../src/daft-geo/src/st_buffer.rs)); `st_isvalid` always `true` ([st_isvalid.rs:16](../../../src/daft-geo/src/st_isvalid.rs)); four functions SQL-only (no Python wrapper).

## Architecture

All work lands in the existing `daft-geo` crate, one file per new function, reusing the established helper pattern and registration so each function stays small and uniform.

- **Shared `relate` helper:** a single `relate(a: &Geometry, b: &Geometry) -> geo::relate::IntersectionMatrix` wrapper used by all eight predicates (`intersects`/`contains`/`within`/`touches`/`crosses`/`overlaps`/`disjoint`/`equals`). The existing `st_intersects`/`st_contains`/`st_within` are **re-based** onto it (correctness upgrade — full type-pair coverage). Each predicate is then a one-line `matrix.is_*()`.
- **Overlay helper:** `geo::BooleanOps` for `union`/`intersection`/`difference`/`symdifference`, returning new WKB.
- **Geodesic:** `geo`'s `Geodesic` algorithms for spheroid distance/length/area; selected by the `use_spheroid` flag, with the planar path unchanged.
- **Buffer:** a pure-Rust offset crate (candidate: `geo-buffer`; exact crate validated in planning), planar, distance in coordinate units.
- **Validity:** `geo`'s `Validation` (`is_valid`) replacing the always-`true` stub.
- **Dependencies:** `geo` is already present; may bump to the latest for `Relate`/`Validation`; add one pure-Rust buffer crate. No C/system dependencies.

## Function inventory

### Predicates (DE-9IM via `Relate`) — Boolean
- **New:** `st_touches`, `st_crosses`, `st_overlaps`, `st_disjoint`, `st_equals`.
- **Re-based for full type coverage:** `st_intersects`, `st_contains`, `st_within`.

### Measures — Float64, with `use_spheroid` (default `False`)
- `st_distance(a, b, use_spheroid=False)`, `st_area(g, use_spheroid=False)`, `st_length(g, use_spheroid=False)`.
- `use_spheroid=True` → WGS84 geodesic; coordinates interpreted as lon/lat degrees.
- `great_circle_distance(lat1, lon1, lat2, lon2)` retained unchanged.

### Overlay — return Geometry (WKB)
- `st_union`, `st_intersection`, `st_difference`, `st_symdifference`.

### Processing / constructors — return Geometry (WKB)
- `st_envelope` (bounding rectangle), `st_convexhull`, `st_simplify(geom, tolerance)`, `st_point(x, y)`, `st_makeline`.

### Fixes
- `st_buffer(geom, distance)` → real planar offset buffer (replaces bbox envelope).
- `st_isvalid(geom)` → real OGC validity (replaces always-`true`).

### Format I/O — gain Python wrappers (already in SQL)
- `st_astext`, `st_geomfromtext`, `st_geomfromgeojson`, `st_geojsonfromgeom`.

## Public API surface (Python ⇄ SQL parity)

- **Python** ([daft/functions/spatial.py](../../../daft/functions/spatial.py)): wrappers for every new function and the four currently SQL-only ones, all via the existing `_call_builtin_scalar_fn` pattern, exported through [daft/functions/__init__.py](../../../daft/functions/__init__.py). `use_spheroid` exposed as a keyword argument on the three measure functions.
- **SQL** ([modules/spatial.rs](../../../src/daft-sql/src/modules/spatial.rs)): register every new function. End state: **every `ST_*` function callable from both Python and SQL** — closing the current SQL-only gap and adding the new functions to both.
- **Namespace:** top-level functions only (no `col.geo.*`).

## Semantics & error handling

Consistent with existing `daft-geo` behavior:
- Null in → null out; a null predicate operand → null.
- Unparseable WKB / WKT / GeoJSON → null (matches current `st_geomfromtext`).
- Predicates operate in 2D (Z/M parsed but ignored).
- Geodesic ops (`use_spheroid=True`) interpret coordinates as WGS84 lon/lat degrees; using them on projected coordinates is user error (documented).
- Planar buffer distance is in coordinate units; geodesic buffer is out of scope (documented).
- Empty geometries handled per `geo` semantics; documented where behavior could surprise.

## Testing

- **Rust unit tests** per function against known geometries with reference values.
- **Predicate consistency property checks** (cheap DE-9IM wiring guards): `within(a,b) == contains(b,a)`, `disjoint == !intersects`, `equals` symmetric.
- **Geodesic validation** against known references (e.g. great-circle distances between known cities) within a documented tolerance.
- **Python⇄SQL parity tests:** each function callable from both, identical result; extend [tests/expressions/test_spatial.py](../../../tests/expressions/test_spatial.py) and add SQL tests.
- **Edge cases:** null, empty, and mixed geometry-type inputs.

## Risks / open questions

- **`geo` version:** confirm the pinned version exposes `Relate`, `BooleanOps`, geodesic measures, and `Validation`; bump if needed (watch for API churn in `geo`'s measure traits across versions).
- **Buffer crate:** validate `geo-buffer` (or alternative) quality on polygons/lines during planning; if no pure-Rust crate is acceptable, fall back to documenting buffer as best-effort planar.
- **Geodesic API in SQL:** the `use_spheroid` boolean arg must thread cleanly through the SQL function registration alongside the existing optional-arg pattern.
