# Spatial Sub-project A: Geometry Functions & Engine — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete Daft's spatial function library on the pure-Rust `geo` engine — correct DE-9IM predicates for all geometry-type pairs, the missing predicates, true WGS84 geodesic distance/length/area, a real planar buffer + real validity, overlay/processing/constructor functions, and full Python⇄SQL parity.

**Architecture:** Every function lives in the existing [`src/daft-geo`](../../../src/daft-geo) crate, one file per function, reusing the `unary_geom_*`/`binary_geom_*` helpers in [utils.rs](../../../src/daft-geo/src/utils.rs), a `ScalarUDF` impl + `#[typetag::serde]`, registration in [lib.rs](../../../src/daft-geo/src/lib.rs), SQL registration in [modules/spatial.rs](../../../src/daft-sql/src/modules/spatial.rs), and a Python wrapper in [daft/functions/spatial.py](../../../daft/functions/spatial.py) exported via [__init__.py](../../../daft/functions/__init__.py). Predicates share one DE-9IM `relate` helper.

**Tech Stack:** Rust (`daft-geo`, `daft-sql` crates), `geo = 0.33.1` (Relate, BooleanOps, Validation, Geodesic, Area, ConvexHull, Simplify, BoundingRect), one pure-Rust buffer crate, `wkb`, PyO3 builtin-scalar-fn bridge, Python, `pytest`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-24-spatial-functions-engine-design.md`. Every task inherits it.
- **Engine: pure-Rust `geo` only. No GEOS, PROJ, or other C/system dependency.** The only new crate is a pure-Rust buffer crate.
- `geo` is pinned at `0.33.1` in the workspace [Cargo.toml](../../../Cargo.toml#L316). Do NOT bump it unless a task proves a needed trait is absent; if you must, build the whole workspace to check for ripple (geo is used by `daft-geo` mbr/h3 and others).
- **geo API confirmation:** `geo`'s measure/relate trait method names churn across versions. Where a step calls a `geo` trait method (e.g. `relate`, `is_touches`, geodesic length/area), confirm the exact method against the installed `geo 0.33.1` (`cargo doc -p geo --open` or source under `~/.cargo`); the code given is the expected form. This applies only to external-crate calls, never to Daft logic.
- Geometry is WKB under `DataType::Geometry` (logical over Binary). Functions accept `Geometry` or `Binary`; null/unparseable → null. Predicates are 2D.
- Geodesic (`use_spheroid=True`) interprets coordinates as **WGS84 lon/lat degrees**; planar is the default.
- **Parity:** every new function must be callable from both Python and SQL with identical results.
- Build after Rust changes that Python tests exercise: `make build` (slow). Rust-only checks: `cargo build -p daft-geo` / `cargo test -p daft-geo`. Python tests: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial.py"`.
- Commit after every task. Conventional Commits titles.

---

## File map

| File | Responsibility | Tasks |
|---|---|---|
| `src/daft-geo/src/relate.rs` (new) | Shared DE-9IM relate helper | 1 |
| `src/daft-geo/src/st_{intersects,contains,within}.rs` | Re-based on relate | 1 |
| `src/daft-geo/src/st_{touches,crosses,overlaps,disjoint,equals}.rs` (new) | New predicates | 2 |
| `src/daft-geo/src/st_{distance,area,length}.rs` | `use_spheroid` geodesic param | 3 |
| `src/daft-geo/src/st_buffer.rs`, `st_isvalid.rs` | Real buffer + real validity | 4 |
| `src/daft-geo/src/st_{union,intersection,difference,symdifference}.rs` (new) | Overlay ops | 5 |
| `src/daft-geo/src/st_{envelope,convexhull,simplify}.rs` (new) | Processing ops | 6 |
| `src/daft-geo/src/st_{point,makeline}.rs` (new) | Constructors | 7 |
| `src/daft-geo/src/utils.rs` | New helpers: `binary_geom_to_geom`, constructor helpers | 5, 7 |
| `src/daft-geo/src/lib.rs` | Register every new struct | 1–7 |
| `src/daft-sql/src/modules/spatial.rs` | SQL registration for every new fn | 2–8 |
| `daft/functions/spatial.py`, `daft/functions/__init__.py` | Python wrappers + exports | 2–8 |
| `tests/expressions/test_spatial.py`, `tests/sql/` | Tests | all |
| `src/daft-geo/Cargo.toml`, workspace `Cargo.toml` | Buffer crate dep | 4 |

---

## Task 1: Shared DE-9IM `relate` helper + re-base existing predicates

**Files:**
- Create: `src/daft-geo/src/relate.rs`
- Modify: `src/daft-geo/src/st_intersects.rs`, `st_contains.rs`, `st_within.rs`, `src/daft-geo/src/lib.rs`
- Test: `src/daft-geo/src/relate.rs` (`#[cfg(test)]`)

**Interfaces:**
- Produces: `pub(crate) fn relate_pred(a: &geo::Geometry, b: &geo::Geometry, pred: RelatePred) -> bool` and `pub(crate) enum RelatePred { Intersects, Contains, Within, Touches, Crosses, Overlaps, Disjoint, Equals }`.

- [ ] **Step 1: Write the failing test**

Add `src/daft-geo/src/relate.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use geo::{Geometry, Point, Polygon, LineString, Coord};

    fn square() -> Geometry {
        // unit square (0,0)-(2,2)
        let ring = LineString(vec![
            Coord { x: 0.0, y: 0.0 }, Coord { x: 2.0, y: 0.0 },
            Coord { x: 2.0, y: 2.0 }, Coord { x: 0.0, y: 2.0 },
            Coord { x: 0.0, y: 0.0 },
        ]);
        Geometry::Polygon(Polygon::new(ring, vec![]))
    }

    #[test]
    fn test_contains_and_within_are_symmetric() {
        let poly = square();
        let inside = Geometry::Point(Point::new(1.0, 1.0));
        // previously st_contains returned false for Polygon/Point only via hand-coding;
        // relate must handle it and within is the mirror.
        assert!(relate_pred(&poly, &inside, RelatePred::Contains));
        assert!(relate_pred(&inside, &poly, RelatePred::Within));
        assert!(!relate_pred(&inside, &poly, RelatePred::Contains));
    }

    #[test]
    fn test_disjoint_is_not_intersects() {
        let poly = square();
        let far = Geometry::Point(Point::new(100.0, 100.0));
        assert!(relate_pred(&poly, &far, RelatePred::Disjoint));
        assert!(!relate_pred(&poly, &far, RelatePred::Intersects));
    }

    #[test]
    fn test_linestring_polygon_intersects_now_handled() {
        // a type pair the old hand-coded st_intersects fell through to false on
        let poly = square();
        let line = Geometry::LineString(LineString(vec![
            Coord { x: -1.0, y: 1.0 }, Coord { x: 3.0, y: 1.0 },
        ]));
        assert!(relate_pred(&poly, &line, RelatePred::Intersects));
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p daft-geo --lib relate`
Expected: FAIL — `relate_pred`/`RelatePred` not defined.

- [ ] **Step 3: Implement the relate helper**

At the top of `src/daft-geo/src/relate.rs`:
```rust
use geo::Geometry;
use geo::relate::Relate;

/// The DE-9IM spatial predicate to evaluate.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RelatePred {
    Intersects,
    Contains,
    Within,
    Touches,
    Crosses,
    Overlaps,
    Disjoint,
    Equals,
}

/// Evaluate a DE-9IM predicate between two geometries. Correct for all geometry-type pairs.
pub(crate) fn relate_pred(a: &Geometry, b: &Geometry, pred: RelatePred) -> bool {
    let m = a.relate(b); // geo 0.33: Geometry: Relate -> IntersectionMatrix
    match pred {
        RelatePred::Intersects => m.is_intersects(),
        RelatePred::Contains => m.is_contains(),
        RelatePred::Within => m.is_within(),
        RelatePred::Touches => m.is_touches(),
        RelatePred::Crosses => m.is_crosses(),
        RelatePred::Overlaps => m.is_overlaps(),
        RelatePred::Disjoint => m.is_disjoint(),
        RelatePred::Equals => m.is_equal_topo(),
    }
}
```
geo 0.33 API note: confirm `Relate` is `geo::relate::Relate` and the `IntersectionMatrix` accessors (`is_intersects`/`is_contains`/`is_within`/`is_touches`/`is_crosses`/`is_overlaps`/`is_disjoint`/`is_equal_topo`). If `is_equal_topo` is named differently in 0.33 (e.g. `is_equal_topo` vs `is_equal`), use the actual one.

- [ ] **Step 4: Register the module**

In `src/daft-geo/src/lib.rs`, add `pub mod relate;` with the other `pub mod` lines (alphabetical near `st_*`).

- [ ] **Step 5: Re-base the three existing predicates**

Replace the hand-coded match in `src/daft-geo/src/st_contains.rs`'s `geom_contains` with the helper. Change the body of `call` to:
```rust
        binary_geom_to_bool(inputs.required(0)?, inputs.required(1)?, self.name(),
            |a, b| crate::relate::relate_pred(a, b, crate::relate::RelatePred::Contains))
```
and delete the now-unused `geom_contains` fn + the `use geo::Contains` import. Do the equivalent for `st_intersects.rs` (use `RelatePred::Intersects`, delete its hand-coded matcher + `use geo::Intersects`) and `st_within.rs` (use `RelatePred::Within`).

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p daft-geo --lib relate` then `cargo build -p daft-geo`
Expected: PASS; crate compiles (fix any leftover unused imports for pristine build).

- [ ] **Step 7: Commit**

```bash
git add src/daft-geo/src/relate.rs src/daft-geo/src/st_contains.rs src/daft-geo/src/st_intersects.rs src/daft-geo/src/st_within.rs src/daft-geo/src/lib.rs
git commit -m "feat(geo): DE-9IM relate helper; re-base intersects/contains/within for full type coverage"
```

---

## Task 2: New predicates (touches, crosses, overlaps, disjoint, equals)

**Files:**
- Create: `src/daft-geo/src/st_touches.rs`, `st_crosses.rs`, `st_overlaps.rs`, `st_disjoint.rs`, `st_equals.rs`
- Modify: `src/daft-geo/src/lib.rs`, `src/daft-sql/src/modules/spatial.rs`, `daft/functions/spatial.py`, `daft/functions/__init__.py`
- Test: `tests/expressions/test_spatial.py`

**Interfaces:**
- Consumes: `crate::relate::{relate_pred, RelatePred}` (Task 1), `binary_geom_to_bool`, `validate_geometry_field` (utils).
- Produces: structs `StTouches`, `StCrosses`, `StOverlaps`, `StDisjoint`, `StEquals` + `pub fn st_touches`/etc.; Python `st_touches`/etc.

- [ ] **Step 1: Write the failing Python test**

Add to `tests/expressions/test_spatial.py`:
```python
import daft
from daft.functions import st_touches, st_disjoint, st_equals, st_geomfromtext

def _geom(wkt):
    return daft.from_pydict({"w": [wkt]}).select(st_geomfromtext(daft.col("w")).alias("g"))

def test_st_disjoint_and_touches():
    # two unit squares sharing an edge → touch, not disjoint
    a = "POLYGON((0 0,1 0,1 1,0 1,0 0))"
    b = "POLYGON((1 0,2 0,2 1,1 1,1 0))"
    df = daft.from_pydict({"a": [a], "b": [b]}).select(
        st_touches(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b"))).alias("t"),
        st_disjoint(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b"))).alias("d"),
    ).to_pydict()
    assert df["t"] == [True]
    assert df["d"] == [False]

def test_st_equals():
    a = "POINT(1 2)"
    df = daft.from_pydict({"a": [a]}).select(
        st_equals(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("a"))).alias("e"),
    ).to_pydict()
    assert df["e"] == [True]
```

- [ ] **Step 2: Run it to verify it fails**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v -k 'st_disjoint or st_equals' tests/expressions/test_spatial.py"`
Expected: FAIL — `ImportError: cannot import name 'st_touches'`.

- [ ] **Step 3: Implement the five predicate structs**

Create `src/daft-geo/src/st_touches.rs` (full pattern; the other four are identical except struct name, `name()` string, `RelatePred` variant, and docstring):
```rust
use common_error::DaftResult;
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use serde::{Deserialize, Serialize};

use crate::relate::{relate_pred, RelatePred};
use crate::utils::{binary_geom_to_bool, validate_geometry_field};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StTouches;

#[typetag::serde]
impl ScalarUDF for StTouches {
    fn name(&self) -> &'static str { "st_touches" }
    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        binary_geom_to_bool(inputs.required(0)?, inputs.required(1)?, self.name(),
            |a, b| relate_pred(a, b, RelatePred::Touches))
    }
    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom_a", self.name())?;
        validate_geometry_field(&inputs, schema, 1, "geom_b", self.name())?;
        Ok(Field::new(self.name(), DataType::Boolean))
    }
    fn docstring(&self) -> &'static str { "Returns true if A and B share a boundary but their interiors do not intersect." }
}

#[must_use]
pub fn st_touches(geom_a: ExprRef, geom_b: ExprRef) -> ExprRef {
    ScalarFn::builtin(StTouches, vec![geom_a, geom_b]).into()
}
```
Create the other four with these substitutions:
- `st_crosses.rs`: `StCrosses`, `"st_crosses"`, `RelatePred::Crosses`, doc "Returns true if A and B cross (interiors intersect with lower dimension than the inputs)."
- `st_overlaps.rs`: `StOverlaps`, `"st_overlaps"`, `RelatePred::Overlaps`, doc "Returns true if A and B overlap (same dimension, interiors intersect, neither contains the other)."
- `st_disjoint.rs`: `StDisjoint`, `"st_disjoint"`, `RelatePred::Disjoint`, doc "Returns true if A and B share no points."
- `st_equals.rs`: `StEquals`, `"st_equals"`, `RelatePred::Equals`, doc "Returns true if A and B are topologically equal."

- [ ] **Step 4: Register in lib.rs**

In `src/daft-geo/src/lib.rs`: add the five `pub mod st_touches;` … lines, the five `pub use st_touches::StTouches;` … lines, and in `register()` add `parent.add_fn(StTouches); parent.add_fn(StCrosses); parent.add_fn(StOverlaps); parent.add_fn(StDisjoint); parent.add_fn(StEquals);`.

- [ ] **Step 5: Register in SQL**

In `src/daft-sql/src/modules/spatial.rs`: add the five structs to the `use daft_geo::{...}` import, and in `register()` add `parent.add_fn("st_touches", SQLSpatialBinary(Arc::new(StTouches)));` and the same for crosses/overlaps/disjoint/equals.

- [ ] **Step 6: Add Python wrappers + exports**

In `daft/functions/spatial.py` add (mirroring `st_contains`):
```python
def st_touches(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return true where A and B share a boundary but their interiors do not intersect."""
    return Expression._call_builtin_scalar_fn("st_touches", geom_a, geom_b)

def st_crosses(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return true where A and B cross."""
    return Expression._call_builtin_scalar_fn("st_crosses", geom_a, geom_b)

def st_overlaps(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return true where A and B overlap (same dimension, partial intersection)."""
    return Expression._call_builtin_scalar_fn("st_overlaps", geom_a, geom_b)

def st_disjoint(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return true where A and B share no points."""
    return Expression._call_builtin_scalar_fn("st_disjoint", geom_a, geom_b)

def st_equals(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return true where A and B are topologically equal."""
    return Expression._call_builtin_scalar_fn("st_equals", geom_a, geom_b)
```
In `daft/functions/__init__.py`, add `st_touches, st_crosses, st_overlaps, st_disjoint, st_equals` to the `from .spatial import (...)` block AND to the `__all__` list (find where the other `st_*` names appear in `__all__`).

- [ ] **Step 7: Build and run tests**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k 'st_disjoint or st_equals or touches' tests/expressions/test_spatial.py"`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/daft-geo/ src/daft-sql/src/modules/spatial.rs daft/functions/spatial.py daft/functions/__init__.py tests/expressions/test_spatial.py
git commit -m "feat(geo): add st_touches/crosses/overlaps/disjoint/equals predicates (Python+SQL)"
```

---

## Task 3: Geodesic measures via `use_spheroid`

**Files:**
- Modify: `src/daft-geo/src/st_distance.rs`, `st_area.rs`, `st_length.rs`, `src/daft-geo/src/lib.rs`, `src/daft-sql/src/modules/spatial.rs`, `daft/functions/spatial.py`
- Test: `tests/expressions/test_spatial.py`

**Interfaces:**
- Produces: `StDistance { use_spheroid: bool }`, `StArea { use_spheroid: bool }`, `StLength { use_spheroid: bool }`; Python `st_distance(a, b, use_spheroid=False)` etc.; SQL `st_distance(a, b[, use_spheroid])`.

- [ ] **Step 1: Write the failing test**

Add to `tests/expressions/test_spatial.py`:
```python
from daft.functions import st_distance, st_geomfromtext

def test_geodesic_distance_meters():
    # ~ great-circle distance between two lon/lat points; planar vs spheroid differ massively
    a = "POINT(0 0)"
    b = "POINT(0 1)"  # 1 degree of latitude ≈ 111195 m
    df = daft.from_pydict({"a": [a], "b": [b]}).select(
        st_distance(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b"))).alias("planar"),
        st_distance(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b")), use_spheroid=True).alias("geo"),
    ).to_pydict()
    assert abs(df["planar"][0] - 1.0) < 1e-9      # planar = 1.0 (degrees)
    assert abs(df["geo"][0] - 111195.0) < 200.0   # geodesic meters, WGS84
```

- [ ] **Step 2: Run it to verify it fails**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v -k geodesic_distance tests/expressions/test_spatial.py"`
Expected: FAIL — `st_distance() got an unexpected keyword argument 'use_spheroid'`.

- [ ] **Step 3: Implement geodesic distance**

In `src/daft-geo/src/st_distance.rs`: add a geodesic matcher and the struct field. Replace the struct + impl:
```rust
use geo::{Distance, Euclidean, Geodesic, Geometry};
// keep existing geom_distance (planar) and add:
fn geom_distance_geodesic(a: &Geometry, b: &Geometry) -> f64 {
    match (a, b) {
        (Geometry::Point(pa), Geometry::Point(pb)) => Geodesic.distance(*pa, *pb),
        _ => f64::NAN, // geodesic distance for non-point pairs is out of scope; planar covers them
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StDistance { pub use_spheroid: bool }

#[typetag::serde]
impl ScalarUDF for StDistance {
    fn name(&self) -> &'static str { "st_distance" }
    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        let f = if self.use_spheroid { geom_distance_geodesic } else { geom_distance };
        binary_geom_to_f64(inputs.required(0)?, inputs.required(1)?, self.name(), f)
    }
    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom_a", self.name())?;
        validate_geometry_field(&inputs, schema, 1, "geom_b", self.name())?;
        Ok(Field::new(self.name(), DataType::Float64))
    }
    fn docstring(&self) -> &'static str {
        "Minimum distance between A and B. Planar (coordinate units) by default; WGS84 geodesic meters when use_spheroid=true (lon/lat point inputs)."
    }
}

#[must_use]
pub fn st_distance(geom_a: ExprRef, geom_b: ExprRef) -> ExprRef {
    ScalarFn::builtin(StDistance { use_spheroid: false }, vec![geom_a, geom_b]).into()
}
```
geo 0.33 API note: confirm `Geodesic.distance(point_a, point_b)` (metric-space form, same shape as the existing `Euclidean` usage). `Geodesic.distance` returns meters.

- [ ] **Step 4: Implement geodesic area + length**

In `src/daft-geo/src/st_area.rs`: add `pub use_spheroid: bool` to the struct; in `call`, pick `g.unsigned_area()` (planar, `geo::Area`) vs geodesic area. Use geo's `GeodesicArea`:
```rust
use geo::{Area, Geometry};
use geo::algorithm::geodesic_area::GeodesicArea; // confirm path in geo 0.33
fn area(g: &Geometry, spheroid: bool) -> f64 {
    if spheroid { g.geodesic_area_unsigned() } else { g.unsigned_area() }
}
```
In `src/daft-geo/src/st_length.rs`: add `pub use_spheroid: bool`; planar uses `Euclidean.length(g)` (or the existing planar length call already present), geodesic uses `Geodesic.length(g)`:
```rust
use geo::{Euclidean, Geodesic, Length, Geometry}; // confirm Length trait + method in 0.33
fn length(g: &Geometry, spheroid: bool) -> f64 {
    if spheroid { Geodesic.length(g) } else { Euclidean.length(g) }
}
```
geo 0.33 API note: confirm the `Length`/`Area`/`GeodesicArea` trait import paths and method names against 0.33 (the metric-space `Geodesic.length(&geom)` form mirrors `Euclidean.distance`). Match the existing planar implementation already in each file rather than rewriting it.
Update each `#[must_use] pub fn st_area/st_length(geom)` constructor to set `use_spheroid: false`.

- [ ] **Step 5: Update lib.rs registration**

In `src/daft-geo/src/lib.rs` `register()`, the three now need the field: `parent.add_fn(StArea { use_spheroid: false }); parent.add_fn(StLength { use_spheroid: false }); parent.add_fn(StDistance { use_spheroid: false });` (replace the existing zero-field constructions).

- [ ] **Step 6: Update SQL (optional trailing bool arg)**

In `src/daft-sql/src/modules/spatial.rs`: replace the `SQLSpatialUnary(Arc::new(StArea))`/`StLength` and `SQLSpatialBinary(Arc::new(StDistance))` registrations with dedicated wrappers that accept an optional final boolean literal. Add (mirroring `SQLStGeohash`):
```rust
// st_distance(a, b [, use_spheroid]); st_area/st_length(g [, use_spheroid])
pub struct SQLStMeasureBinary(&'static str); // "st_distance"
pub struct SQLStMeasureUnary(&'static str);  // "st_area" | "st_length"
```
For each, parse an optional trailing arg via `.as_literal().and_then(|l| l.as_bool())` (default false), then build the right struct (`StDistance { use_spheroid }` / `StArea { use_spheroid }` / `StLength { use_spheroid }`). Register: `parent.add_fn("st_distance", SQLStMeasureBinary("st_distance")); parent.add_fn("st_area", SQLStMeasureUnary("st_area")); parent.add_fn("st_length", SQLStMeasureUnary("st_length"));`. (Confirm `Literal::as_bool` exists; if not, accept `true`/`false` via `as_i64()` nonzero or a string literal.)

- [ ] **Step 7: Update Python wrappers**

In `daft/functions/spatial.py`, change the three signatures:
```python
def st_distance(geom_a: Expression, geom_b: Expression, use_spheroid: bool = False) -> Expression:
    """Minimum distance between A and B. Planar by default; WGS84 geodesic meters when use_spheroid=True (lon/lat points)."""
    return Expression._call_builtin_scalar_fn("st_distance", geom_a, geom_b, use_spheroid)

def st_area(geom: Expression, use_spheroid: bool = False) -> Expression:
    """2D area. Coordinate units² by default; WGS84 geodesic m² when use_spheroid=True (lon/lat input)."""
    return Expression._call_builtin_scalar_fn("st_area", geom, use_spheroid)

def st_length(geom: Expression, use_spheroid: bool = False) -> Expression:
    """Length/perimeter. Coordinate units by default; WGS84 geodesic meters when use_spheroid=True (lon/lat input)."""
    return Expression._call_builtin_scalar_fn("st_length", geom, use_spheroid)
```
(`use_spheroid` passes through as a boolean literal arg; the Rust `call` reads it from the struct field, which the Python builtin-fn bridge sets via the trailing literal — confirm the builtin-scalar-fn path threads a trailing bool literal into the struct; if the bridge cannot set struct fields from args, instead read an optional 3rd `FunctionArgs` boolean in `call` and drop the struct field. Pick whichever the bridge supports and keep Rust+Python consistent.)

- [ ] **Step 8: Build and run tests**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k 'geodesic or distance or area or length' tests/expressions/test_spatial.py"`
Expected: PASS (planar values unchanged; geodesic ≈ reference).

- [ ] **Step 9: Commit**

```bash
git add src/daft-geo/ src/daft-sql/src/modules/spatial.rs daft/functions/spatial.py tests/expressions/test_spatial.py
git commit -m "feat(geo): geodesic distance/area/length via use_spheroid parameter"
```

---

## Task 4: Real planar `st_buffer` + real `st_isvalid`

**Files:**
- Modify: `src/daft-geo/src/st_buffer.rs`, `st_isvalid.rs`, `src/daft-geo/Cargo.toml`, workspace `Cargo.toml`
- Test: `src/daft-geo/src/st_buffer.rs` and `st_isvalid.rs` (`#[cfg(test)]`)

**Interfaces:** unchanged public signatures (`st_buffer(geom, distance)`, `st_isvalid(geom)`); only implementations change.

- [ ] **Step 1: Add the buffer crate**

In workspace `Cargo.toml` `[workspace.dependencies]`, add `geo-buffer = "0.2"` (validate the exact version's `geo-types` compatibility with `geo 0.33`; if incompatible, see fallback in Step 3). In `src/daft-geo/Cargo.toml` `[dependencies]`, add `geo-buffer = { workspace = true }`.

- [ ] **Step 2: Write the failing buffer test**

In `src/daft-geo/src/st_buffer.rs` `#[cfg(test)]`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use geo::{Geometry, Point, Area};
    #[test]
    fn test_point_buffer_area_approx_pi_r2() {
        let p = Geometry::Point(Point::new(0.0, 0.0));
        let buf = apply_buffer(&p, 1.0).unwrap();
        // area of a radius-1 buffer ≈ π
        let a = buf.unsigned_area();
        assert!((a - std::f64::consts::PI).abs() < 0.2, "got {a}");
    }
}
```

- [ ] **Step 3: Implement a real planar buffer**

Replace `apply_buffer` in `src/daft-geo/src/st_buffer.rs` with a real offset using `geo_buffer::buffer_polygon` / `buffer_point` (confirm the crate's API). The point path can keep the 64-gon circle (it's a correct circle); for polygons/lines use the crate's offset:
```rust
fn apply_buffer(g: &Geometry, distance: f64) -> Option<Geometry> {
    match g {
        Geometry::Point(p) => { /* keep existing 64-vertex circle — it is a correct buffer */ }
        Geometry::Polygon(poly) => Some(Geometry::MultiPolygon(geo_buffer::buffer_polygon(poly, distance))),
        Geometry::MultiPolygon(mp) => Some(Geometry::MultiPolygon(geo_buffer::buffer_multi_polygon(mp, distance))),
        Geometry::LineString(ls) => Some(Geometry::MultiPolygon(geo_buffer::buffer_line_string(ls, distance))),
        _ => { let bbox = g.bounding_rect()?; Some(Geometry::Rect(/* expanded as before */)) }
    }
}
```
geo-buffer API note: confirm the function names/return types (`buffer_polygon` returns `MultiPolygon` in 0.x). **Fallback** if `geo-buffer` is incompatible with `geo 0.33`'s `geo-types`: keep the point-circle path, keep the bbox-envelope for other types, and update the docstring to say buffer is best-effort planar — but DO try the crate first; the spec calls for a real planar buffer.

- [ ] **Step 4: Real validity**

In `src/daft-geo/src/st_isvalid.rs`, replace the always-true body with `geo`'s `Validation`:
```rust
use geo::algorithm::validation::Validation; // confirm path in geo 0.33
fn is_valid(g: &Geometry) -> bool { g.is_valid() }
```
Add a test: an exterior ring that self-intersects (bowtie polygon) → `false`; a simple square → `true`. geo 0.33 API note: confirm the `Validation` trait import path and that `is_valid()` exists; if 0.33 exposes `explain_invalidity()`/`validate()` instead, derive a bool from it.

- [ ] **Step 5: Run tests + build**

Run: `cargo test -p daft-geo --lib 'st_buffer' && cargo test -p daft-geo --lib 'st_isvalid' && cargo build -p daft-geo`
Expected: PASS, compiles.

- [ ] **Step 6: Commit**

```bash
git add src/daft-geo/src/st_buffer.rs src/daft-geo/src/st_isvalid.rs src/daft-geo/Cargo.toml Cargo.toml Cargo.lock
git commit -m "feat(geo): real planar st_buffer (geo-buffer) and real st_isvalid"
```

---

## Task 5: Overlay ops (union, intersection, difference, symdifference)

**Files:**
- Create: `src/daft-geo/src/st_union.rs`, `st_intersection.rs`, `st_difference.rs`, `st_symdifference.rs`
- Modify: `src/daft-geo/src/utils.rs` (add `binary_geom_to_geom`), `lib.rs`, `src/daft-sql/src/modules/spatial.rs`, `daft/functions/spatial.py`, `daft/functions/__init__.py`
- Test: `tests/expressions/test_spatial.py`

**Interfaces:**
- Produces: `pub fn binary_geom_to_geom(lhs, rhs, out_name, f: impl Fn(&Geometry, &Geometry) -> Option<Geometry>) -> DaftResult<Series>`; structs `StUnion`/`StIntersection`/`StDifference`/`StSymDifference`; Python `st_union`/etc.

- [ ] **Step 1: Add `binary_geom_to_geom` helper**

In `src/daft-geo/src/utils.rs`, add (model on `binary_geom_to_bool` for parsing/broadcast + on `unary_geom_to_geom` for WKB output):
```rust
pub fn binary_geom_to_geom(
    lhs: &Series, rhs: &Series, out_name: &str,
    f: impl Fn(&Geometry, &Geometry) -> Option<Geometry>,
) -> DaftResult<Series> {
    use wkb::geom_to_wkb;
    let lhs_bin = get_geometry_binary(lhs)?;
    let rhs_bin = get_geometry_binary(rhs)?;
    let rhs_scalar = rhs_bin.len() == 1;
    let rhs_geom_scalar: Option<Option<Geometry>> = rhs_scalar.then(|| {
        rhs_bin.into_iter().next().flatten().and_then(|b| parse_wkb(b).ok())
    });
    let len = lhs_bin.len();
    let mut wkb_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);
    let compute = |lopt: Option<&[u8]>, rg: Option<&Geometry>| -> Option<Vec<u8>> {
        let lg = lopt.and_then(|b| parse_wkb(b).ok())?;
        let rg = rg?;
        f(&lg, rg).and_then(|g| geom_to_wkb(&g).ok())
    };
    if let Some(rhs_opt) = rhs_geom_scalar {
        for lopt in lhs_bin.into_iter() { wkb_values.push(compute(lopt, rhs_opt.as_ref())); }
    } else {
        for (lopt, ropt) in lhs_bin.into_iter().zip(rhs_bin.into_iter()) {
            let rg = ropt.and_then(|b| parse_wkb(b).ok());
            wkb_values.push(compute(lopt, rg.as_ref()));
        }
    }
    // Wrap as Geometry logical array (identical to unary_geom_to_geom's tail)
    let field = Field::new(out_name, DataType::Geometry);
    let mut builder = arrow_buffer::NullBufferBuilder::new(len);
    let phys: Vec<Option<&[u8]>> = wkb_values.iter().map(|v| {
        if v.is_some() { builder.append_non_null(); } else { builder.append_null(); }
        v.as_deref()
    }).collect();
    let phys_field = Field::new(out_name, DataType::Binary);
    let phys_arr = BinaryArray::from_iter(&phys_field.name, phys.into_iter());
    use daft_core::prelude::{GeometryType, LogicalArray};
    Ok(LogicalArray::<GeometryType>::new(field, phys_arr.with_nulls(builder.finish())?).into_series())
}
```

- [ ] **Step 2: Write the failing test**

Add to `tests/expressions/test_spatial.py`:
```python
from daft.functions import st_union, st_intersection, st_area, st_geomfromtext
def test_union_area():
    a = "POLYGON((0 0,2 0,2 2,0 2,0 0))"
    b = "POLYGON((1 1,3 1,3 3,1 3,1 1))"
    df = daft.from_pydict({"a": [a], "b": [b]}).select(
        st_area(st_union(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b")))).alias("u"),
        st_area(st_intersection(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b")))).alias("i"),
    ).to_pydict()
    # union = 4 + 4 - 1 (overlap) = 7 ; intersection = 1
    assert abs(df["u"][0] - 7.0) < 1e-6
    assert abs(df["i"][0] - 1.0) < 1e-6
```

- [ ] **Step 3: Run it to verify it fails** — `DAFT_RUNNER=native make test EXTRA_ARGS="-v -k union_area tests/expressions/test_spatial.py"` → FAIL (import error).

- [ ] **Step 4: Implement the four overlay structs**

Create `src/daft-geo/src/st_union.rs` (full; siblings differ by name/op/doc). Overlay uses `geo::BooleanOps`, which operates on Polygon/MultiPolygon — handle by converting both operands to MultiPolygon, else return None:
```rust
use geo::{BooleanOps, Geometry, MultiPolygon};
fn as_multipolygon(g: &Geometry) -> Option<MultiPolygon> {
    match g {
        Geometry::Polygon(p) => Some(MultiPolygon(vec![p.clone()])),
        Geometry::MultiPolygon(mp) => Some(mp.clone()),
        _ => None,
    }
}
fn op_union(a: &Geometry, b: &Geometry) -> Option<Geometry> {
    let (a, b) = (as_multipolygon(a)?, as_multipolygon(b)?);
    Some(Geometry::MultiPolygon(a.union(&b)))
}
```
Then the standard `ScalarUDF` impl calling `binary_geom_to_geom(.., op_union)`, returning `DataType::Geometry`. Siblings:
- `st_intersection.rs`: `StIntersection`, `a.intersection(&b)`.
- `st_difference.rs`: `StDifference`, `a.difference(&b)`.
- `st_symdifference.rs`: `StSymDifference`, `a.xor(&b)`.
geo 0.33 API note: confirm `BooleanOps` method names (`union`/`intersection`/`difference`/`xor`) and that they take `&MultiPolygon`.

- [ ] **Step 5: Register (lib.rs + SQL + Python + exports)**

lib.rs: `pub mod`/`pub use`/`add_fn` for the four. SQL: these are binary geom→geom, but `SQLSpatialBinary` only handles geoms and returns whatever the UDF returns (it's generic over the UDF's return field), so register `parent.add_fn("st_union", SQLSpatialBinary(Arc::new(StUnion)));` etc. (SQLSpatialBinary doesn't assume Boolean — it just plumbs two geom args). Python wrappers (mirror `st_contains` shape, 2 args) + `__init__.py` exports + `__all__`.

- [ ] **Step 6: Build + test** — `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k union_area tests/expressions/test_spatial.py"` → PASS.

- [ ] **Step 7: Commit**

```bash
git add src/daft-geo/ src/daft-sql/src/modules/spatial.rs daft/functions/spatial.py daft/functions/__init__.py tests/expressions/test_spatial.py
git commit -m "feat(geo): overlay ops st_union/intersection/difference/symdifference (Python+SQL)"
```

---

## Task 6: Processing ops (envelope, convexhull, simplify)

**Files:**
- Create: `src/daft-geo/src/st_envelope.rs`, `st_convexhull.rs`, `st_simplify.rs`
- Modify: `lib.rs`, `src/daft-sql/src/modules/spatial.rs`, `daft/functions/spatial.py`, `daft/functions/__init__.py`
- Test: `tests/expressions/test_spatial.py`

**Interfaces:** `StEnvelope`, `StConvexHull`, `StSimplify { tolerance: OrderedFloat<f64> }`; Python `st_envelope(g)`, `st_convexhull(g)`, `st_simplify(g, tolerance)`.

- [ ] **Step 1: Failing test**

```python
from daft.functions import st_envelope, st_convexhull, st_simplify, st_astext, st_geomfromtext
def test_envelope_is_bbox():
    g = "LINESTRING(0 0, 2 3, 1 1)"
    df = daft.from_pydict({"g": [g]}).select(
        st_astext(st_envelope(st_geomfromtext(daft.col("g")))).alias("e")
    ).to_pydict()
    # envelope is the bounding rectangle polygon
    assert "POLYGON" in df["e"][0].upper()
```

- [ ] **Step 2: Run → FAIL** (`DAFT_RUNNER=native make test EXTRA_ARGS="-v -k envelope_is_bbox tests/expressions/test_spatial.py"`).

- [ ] **Step 3: Implement**

- `st_envelope.rs`: unary geom→geom via `unary_geom_to_geom`; `g.bounding_rect().map(|r| Geometry::Polygon(r.to_polygon()))` (`geo::BoundingRect`, `Rect::to_polygon`).
- `st_convexhull.rs`: unary geom→geom; `Some(Geometry::Polygon(g.convex_hull()))` (`geo::ConvexHull` — confirm it's implemented for `Geometry`; if only for point collections, convert via `g.coords_iter()`).
- `st_simplify.rs`: struct `StSimplify { tolerance: ordered_float::OrderedFloat<f64> }` (mirror `StBuffer`); unary geom→geom applying `geo::Simplify` (`g.simplify(&tol)` — confirm signature in 0.33; it applies to LineString/Polygon, pass-through others).

- [ ] **Step 4: Register** lib.rs + SQL (`SQLSpatialUnary` for envelope/convexhull; a `SQLStSimplify` wrapper modeled on `SQLStBuffer` for the tolerance arg) + Python wrappers (`st_simplify(g, tolerance)` passes tolerance as literal like `st_buffer`) + exports.

- [ ] **Step 5: Build + test** → PASS.

- [ ] **Step 6: Commit** `feat(geo): processing ops st_envelope/convexhull/simplify (Python+SQL)`.

---

## Task 7: Constructors (st_point, st_makeline)

**Files:**
- Create: `src/daft-geo/src/st_point.rs`, `st_makeline.rs`
- Modify: `src/daft-geo/src/utils.rs` (numeric→geom + geoms→geom helpers), `lib.rs`, SQL, Python, exports
- Test: `tests/expressions/test_spatial.py`

**Interfaces:** `StPoint` (two Float64 args → Geometry point), `StMakeLine` (two geometry point args → LineString). Python `st_point(x, y)`, `st_makeline(a, b)`.

- [ ] **Step 1: Failing test**

```python
from daft.functions import st_point, st_x, st_y
def test_st_point_roundtrip():
    df = daft.from_pydict({"x": [3.0], "y": [4.0]}).select(
        st_x(st_point(daft.col("x"), daft.col("y"))).alias("px"),
        st_y(st_point(daft.col("x"), daft.col("y"))).alias("py"),
    ).to_pydict()
    assert df["px"] == [3.0] and df["py"] == [4.0]
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement**

- `st_point.rs`: `call` reads two Float64 `Series` (`inputs.required(0)?.f64()?`, `.f64()?`), zips, builds `geo::Geometry::Point` per row, encodes to WKB via `wkb::geom_to_wkb`, wraps as Geometry logical array (reuse the tail of `unary_geom_to_geom` — extract a small `wkb_opts_to_geometry_series(name, Vec<Option<Vec<u8>>>)` helper in utils and use it here AND from `unary_geom_to_geom`/`binary_geom_to_geom` to stay DRY). Null if either coord null. Return `DataType::Geometry`. `get_return_field` validates both args are numeric.
- `st_makeline.rs`: binary geom→geom via `binary_geom_to_geom`; build a `LineString` from the two points' coords (`Some(Geometry::LineString(LineString(vec![a_pt.into(), b_pt.into()])))` when both are `Geometry::Point`, else `None`).

- [ ] **Step 4: Register** lib.rs + SQL (a small `SQLStPoint` taking two numeric args; `SQLSpatialBinary` for makeline) + Python wrappers + exports.

- [ ] **Step 5: Build + test** → PASS.

- [ ] **Step 6: Commit** `feat(geo): constructors st_point/st_makeline (Python+SQL)`.

---

## Task 8: Python wrappers for the SQL-only format functions

**Files:**
- Modify: `daft/functions/spatial.py`, `daft/functions/__init__.py`
- Test: `tests/expressions/test_spatial.py`

**Interfaces:** Python `st_astext(g)`, `st_geomfromtext(wkt)`, `st_geomfromgeojson(json)`, `st_geojsonfromgeom(g)` — the Rust UDFs already exist; only Python wrappers are missing.

- [ ] **Step 1: Failing test**

```python
from daft.functions import st_geomfromtext, st_astext
def test_wkt_roundtrip_python():
    df = daft.from_pydict({"w": ["POINT(1 2)"]}).select(
        st_astext(st_geomfromtext(daft.col("w"))).alias("out")
    ).to_pydict()
    assert df["out"][0].upper().startswith("POINT")
```

- [ ] **Step 2: Run → FAIL** (import error for `st_astext`/`st_geomfromtext`).

- [ ] **Step 3: Add the four wrappers**

In `daft/functions/spatial.py`:
```python
def st_astext(geom: Expression) -> Expression:
    """Return the WKT (Well-Known Text) representation of a geometry."""
    return Expression._call_builtin_scalar_fn("st_astext", geom)

def st_geomfromtext(wkt: Expression) -> Expression:
    """Parse a WKT string into a geometry (WKB). Null for unparseable input."""
    return Expression._call_builtin_scalar_fn("st_geomfromtext", wkt)

def st_geomfromgeojson(geojson: Expression) -> Expression:
    """Parse a GeoJSON geometry/feature string into a geometry (WKB). Null for unparseable input."""
    return Expression._call_builtin_scalar_fn("st_geomfromgeojson", geojson)

def st_geojsonfromgeom(geom: Expression) -> Expression:
    """Return the GeoJSON string representation of a geometry."""
    return Expression._call_builtin_scalar_fn("st_geojsonfromgeom", geom)
```
Add the four names to `daft/functions/__init__.py` (`from .spatial import (...)` and `__all__`).

- [ ] **Step 4: Build + test** → PASS.

- [ ] **Step 5: Commit** `feat(geo): Python wrappers for st_astext/geomfromtext/geomfromgeojson/geojsonfromgeom`.

---

## Task 9: Parity sweep, doctests, and full suite

**Files:**
- Modify: `tests/expressions/test_spatial.py` (parity + SQL tests), any docs under `docs/` referencing spatial.

- [ ] **Step 1: Add a Python⇄SQL parity test**

```python
def test_python_sql_parity():
    a = "POLYGON((0 0,2 0,2 2,0 2,0 0))"
    b = "POINT(1 1)"
    base = daft.from_pydict({"a": [a], "b": [b]})
    py = base.select(
        st_contains(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b"))).alias("c")
    ).to_pydict()
    sql = daft.sql("SELECT st_contains(st_geomfromtext(a), st_geomfromtext(b)) AS c FROM base").to_pydict()
    assert py["c"] == sql["c"] == [True]
```
Add analogous parity asserts for one function from each new family (a predicate, an overlay op, a measure with `use_spheroid`, a constructor) to prove every family is reachable from both Python and SQL.

- [ ] **Step 2: Run the full spatial suite**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial.py"`
Expected: all PASS, output pristine.

- [ ] **Step 3: Run doctests for the spatial module**

Run: `make doctests` (or the project's doctest target) — ensure new docstrings' `>>> ` examples are `# doctest: +SKIP` where they need real WKB, matching the existing `st_area` example style.

- [ ] **Step 4: Commit**

```bash
git add tests/expressions/test_spatial.py docs/
git commit -m "test(geo): Python⇄SQL parity sweep for spatial functions"
```

---

## Self-review notes

- **Spec coverage:** missing predicates → Task 2; predicate correctness (Relate) → Task 1; geodesic distance/length/area → Task 3; real buffer + isvalid → Task 4; overlay ops → Task 5; processing ops → Task 6; constructors → Task 7; Python wrappers for SQL-only → Task 8; Python⇄SQL parity → Tasks 2–8 (each) + Task 9 sweep; testing → every task. All spec items mapped.
- **External-API risk is isolated and labeled:** every `geo`/`geo-buffer` call that could differ across crate versions carries a "confirm against 0.33" note; these are external-crate calls, not Daft logic. The buffer crate has a documented fallback (Task 4 Step 3).
- **Bridge risk flagged once:** how `use_spheroid` threads from the Python builtin-fn bridge into the Rust struct field is called out in Task 3 Step 7 with a concrete fallback (read a 3rd `FunctionArgs` bool in `call`); resolve it in Task 3 and the constructor/param tasks follow the same resolution.
- **Type/name consistency:** struct names (`StTouches`/…/`StUnion`/`StSimplify { tolerance }`/`StDistance { use_spheroid }`), the shared `relate_pred`/`RelatePred`, and `binary_geom_to_geom` are used consistently across tasks; SQL wrappers (`SQLSpatialUnary`/`SQLSpatialBinary`/`SQLStBuffer`-style) are reused.
- **DRY:** the WKB-options→Geometry-series tail is extracted into a shared utils helper (Task 7 Step 3) and reused by unary/binary/constructor geom producers.
