# Spatial Sub-project C: Join Surface, Predicates & Index — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface and complete the existing R-tree spatial-join engine — make spatial joins reachable through Python `.join(on=<predicate>)` and SQL `JOIN ON ST_*`, add the missing `st_covers`/`st_covered_by`/`st_dwithin` predicates, and add a `df.with_spatial_bbox()` index helper — all routing to the engine already on `main`.

**Architecture:** The R-tree `NestedLoopJoinOperator`, `mbr.rs`, and geohash pruning already exist (commit `c6480b6be`). This sub-project adds three predicates (modeled on existing `daft-geo` UDFs), exposes the join through the real APIs by (a) routing a spatial `JOIN ON` residual to the existing operator in `translate.rs` and (b) overloading Python `.join(on=)` to accept a boolean predicate, and adds an `st_bbox` UDF + `with_spatial_bbox` helper that materializes the bbox columns the operator's fast-path already detects. No new physical operator.

**Tech Stack:** Rust (`daft-geo`, `daft-local-plan`, `daft-logical-plan`, `daft-local-execution`, `daft-sql`), the `geo` crate 0.33.1 (`Relate` DE-9IM, `Euclidean` distance), `rstar` (already a dep), Python (`daft/functions/spatial.py`, `daft/dataframe/dataframe.py`), `pytest`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-24-spatial-join-surface-design.md`. Every task inherits it.
- **Inner join only.** A spatial/predicate join requested with `how != "inner"` raises a clear error. Outer/semi/anti are out of scope.
- **Only spatial residuals route to the operator.** A non-equi join residual that is NOT a spatial predicate keeps the existing `not_implemented("Execution of non-equality join")` error.
- **Distinct geometry column names.** `resolve_join_on` resolves predicate columns by name against each side and errors on same-name ambiguity ("Ambiguous column reference in join predicate"). The two geometry columns in a predicate join must have distinct names (or be aliased). Tests use distinct names.
- **`st_dwithin` is planar** (Euclidean in coordinate units). `d` is passed as a trailing positional numeric-literal arg (the Sub-project A param pattern via `utils::read_f64_arg`).
- **Reuse `NestedLoopJoinOperator`.** Do not add a new physical join operator.
- **Predicate engine:** geo `Relate` DE-9IM (`is_covers`/`is_coveredby` exist on geo 0.33.1's `IntersectionMatrix`); geo `Euclidean.distance` for `st_dwithin`.
- Build after Rust changes exercised from Python: `make build`. Rust-only: `cargo build/test -p <crate>`. Python tests: `DAFT_RUNNER=native make test EXTRA_ARGS="-v <path>"`.
- Commit after each task. Conventional Commits titles. Commit trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

---

## File map

| File | Responsibility | Tasks |
|---|---|---|
| `src/daft-geo/src/relate.rs` | add `RelatePred::Covers` / `CoveredBy` | 1 |
| `src/daft-geo/src/st_covers.rs` (new), `st_covered_by.rs` (new) | topological predicates | 1 |
| `src/daft-geo/src/st_dwithin.rs` (new), `st_distance.rs` | distance predicate; share `geom_distance` | 2 |
| `src/daft-geo/src/st_bbox.rs` (new) | `st_bbox(geom) -> Struct{min_x,min_y,max_x,max_y}` | 3 |
| `src/daft-geo/src/lib.rs` | module decls, `pub use`, `add_fn` registration | 1,2,3 |
| `src/daft-sql/src/modules/spatial.rs` | SQL registration for new predicates | 1,2 |
| `daft/functions/spatial.py`, `daft/functions/__init__.py` | Python wrappers + exports | 1,2 |
| `src/daft-local-plan/src/translate.rs` | factor shared spatial-NLJ helper; route spatial `JOIN ON` | 4,6 |
| `src/daft-logical-plan/src/builder/mod.rs`, `daft/daft/__init__.pyi` | PyO3 `join(on_predicate=…)` | 5 |
| `daft/dataframe/dataframe.py` | `.join(on=<predicate>)` detection; `with_spatial_bbox` helper | 3,5 |
| `src/daft-local-execution/src/join/nested_loop_join.rs` | `st_dwithin` accel (SPATIAL_FNS + query-box expand by d) | 6 |
| `tests/expressions/test_spatial.py`, `tests/expressions/test_spatial_join.py` (new) | predicate + join tests | 1–7 |
| `docs/connectors/*` or spatial docs | join + predicate + index docs | 7 |

---

## Task 1: `st_covers` / `st_covered_by` predicates

**Files:**
- Modify: `src/daft-geo/src/relate.rs` (add two `RelatePred` variants + matches)
- Create: `src/daft-geo/src/st_covers.rs`, `src/daft-geo/src/st_covered_by.rs`
- Modify: `src/daft-geo/src/lib.rs`, `src/daft-sql/src/modules/spatial.rs`, `daft/functions/spatial.py`, `daft/functions/__init__.py`
- Test: `tests/expressions/test_spatial.py`

**Interfaces:**
- Produces: structs `StCovers`/`StCoveredBy` (impl `ScalarUDF`, name `"st_covers"`/`"st_covered_by"`, return `Boolean`); Rust fns `st_covers(a,b)`/`st_covered_by(a,b) -> ExprRef`; Python `st_covers(geom_a, geom_b)`/`st_covered_by(geom_a, geom_b) -> Expression`. `translate.rs` and `nested_loop_join.rs` already list `"st_covers"`/`"st_covered_by"` in their spatial-name arrays, so these become live join predicates automatically.

- [ ] **Step 1: Write the failing Rust unit test for the DE-9IM mapping**

Add to the `tests` module in `src/daft-geo/src/relate.rs` (it already has `use super::*;` and helpers):
```rust
    #[test]
    fn test_covers_and_covered_by() {
        let poly = square(); // (0,0)-(2,2)
        // Boundary point: contained-by-covers but NOT contains (interior only).
        let boundary = Geometry::Point(Point::new(0.0, 1.0));
        assert!(relate_pred(&poly, &boundary, RelatePred::Covers));
        assert!(relate_pred(&boundary, &poly, RelatePred::CoveredBy));
        assert!(!relate_pred(&poly, &boundary, RelatePred::Contains)); // interior-only
        // A far point is neither covered nor covering.
        let far = Geometry::Point(Point::new(100.0, 100.0));
        assert!(!relate_pred(&poly, &far, RelatePred::Covers));
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p daft-geo relate::tests::test_covers_and_covered_by`
Expected: FAIL — `no variant named Covers`.

- [ ] **Step 3: Add the `RelatePred` variants and matches**

In `src/daft-geo/src/relate.rs`, extend the enum (after `Equals,`):
```rust
    Equals,
    Covers,
    CoveredBy,
```
and add to the `match pred` block (after the `Equals` arm):
```rust
            RelatePred::Equals => m.is_equal_topo(),
            RelatePred::Covers => m.is_covers(),
            RelatePred::CoveredBy => m.is_coveredby(),
```

- [ ] **Step 4: Run the Rust test to verify it passes**

Run: `cargo test -p daft-geo relate::tests::test_covers_and_covered_by`
Expected: PASS.

- [ ] **Step 5: Create `st_covers.rs` and `st_covered_by.rs`**

`src/daft-geo/src/st_covers.rs` (mirror of `st_contains.rs`):
```rust
use common_error::DaftResult;
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use geo::Geometry;
use serde::{Deserialize, Serialize};

use crate::utils::{binary_geom_to_bool, validate_geometry_field};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StCovers;

#[typetag::serde]
impl ScalarUDF for StCovers {
    fn name(&self) -> &'static str { "st_covers" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        binary_geom_to_bool(inputs.required(0)?, inputs.required(1)?, self.name(),
            |a: &Geometry, b: &Geometry| crate::relate::relate_pred(a, b, crate::relate::RelatePred::Covers))
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom_a", self.name())?;
        validate_geometry_field(&inputs, schema, 1, "geom_b", self.name())?;
        Ok(Field::new(self.name(), DataType::Boolean))
    }

    fn docstring(&self) -> &'static str {
        "Returns true if geometry A covers geometry B (no point of B lies outside A; includes boundary)."
    }
}

#[must_use]
pub fn st_covers(geom_a: ExprRef, geom_b: ExprRef) -> ExprRef {
    ScalarFn::builtin(StCovers, vec![geom_a, geom_b]).into()
}
```

`src/daft-geo/src/st_covered_by.rs` — identical but `StCoveredBy`, name `"st_covered_by"`, `RelatePred::CoveredBy`, fn `st_covered_by`, docstring `"Returns true if geometry A is covered by geometry B (no point of A lies outside B; includes boundary)."`.

- [ ] **Step 6: Register in `lib.rs`**

In `src/daft-geo/src/lib.rs`: add module decls (alphabetical, near `st_contains`):
```rust
pub mod st_covered_by;
pub mod st_covers;
```
add `pub use`:
```rust
pub use st_covered_by::StCoveredBy;
pub use st_covers::StCovers;
```
add registration inside `SpatialFunctions::register` (near `StEquals`):
```rust
        parent.add_fn(StCovers);
        parent.add_fn(StCoveredBy);
```

- [ ] **Step 7: Register in SQL**

In `src/daft-sql/src/modules/spatial.rs`: add `StCovers, StCoveredBy` to the `use daft_geo::{…}` import list, and register (near `st_equals`):
```rust
        parent.add_fn("st_covers", SQLSpatialBinary(Arc::new(StCovers)));
        parent.add_fn("st_covered_by", SQLSpatialBinary(Arc::new(StCoveredBy)));
```

- [ ] **Step 8: Add Python wrappers + exports**

In `daft/functions/spatial.py` (after `st_within`/`st_distance`):
```python
def st_covers(geom_a: Expression, geom_b: Expression) -> Expression:
    """Returns true if geometry A covers geometry B (no point of B is outside A; boundary included)."""
    return Expression._call_builtin_scalar_fn("st_covers", geom_a, geom_b)


def st_covered_by(geom_a: Expression, geom_b: Expression) -> Expression:
    """Returns true if geometry A is covered by geometry B (no point of A is outside B; boundary included)."""
    return Expression._call_builtin_scalar_fn("st_covered_by", geom_a, geom_b)
```
In `daft/functions/__init__.py`: add `st_covers`, `st_covered_by` to the `from daft.functions.spatial import (...)` block and to `__all__` (keep `__all__` sorted as the file already is).

- [ ] **Step 9: Write the Python parity test**

Add to `tests/expressions/test_spatial.py`:
```python
def test_st_covers_and_covered_by():
    import daft
    from daft.functions import st_covers, st_covered_by, st_contains, st_geomfromtext

    poly = "POLYGON((0 0,2 0,2 2,0 2,0 0))"
    boundary_pt = "POINT(0 1)"  # on the edge: covered but not contained
    df = daft.from_pydict({"poly": [poly], "pt": [boundary_pt]}).select(
        st_covers(st_geomfromtext(daft.col("poly")), st_geomfromtext(daft.col("pt"))).alias("cov"),
        st_covered_by(st_geomfromtext(daft.col("pt")), st_geomfromtext(daft.col("poly"))).alias("cby"),
        st_contains(st_geomfromtext(daft.col("poly")), st_geomfromtext(daft.col("pt"))).alias("con"),
    ).to_pydict()
    assert df["cov"][0] is True
    assert df["cby"][0] is True
    assert df["con"][0] is False  # boundary point is covered but NOT contained
```

- [ ] **Step 10: Build and run**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k 'covers or covered_by' tests/expressions/test_spatial.py"`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add src/daft-geo/ src/daft-sql/src/modules/spatial.rs daft/functions/ tests/expressions/test_spatial.py
git commit -m "feat(geo): st_covers / st_covered_by topological predicates (Python+SQL)" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `st_dwithin` distance predicate (unaccelerated)

**Files:**
- Modify: `src/daft-geo/src/st_distance.rs` (make `geom_distance` shareable)
- Create: `src/daft-geo/src/st_dwithin.rs`
- Modify: `src/daft-geo/src/lib.rs`, `src/daft-sql/src/modules/spatial.rs`, `daft/functions/spatial.py`, `daft/functions/__init__.py`
- Test: `tests/expressions/test_spatial.py`

**Interfaces:**
- Consumes: `crate::st_distance::geom_distance` (planar Euclidean, made `pub(crate)`); `utils::{read_f64_arg, read_f64_arg_expr, binary_geom_to_bool}`.
- Produces: struct `StDwithin` (name `"st_dwithin"`, return `Boolean`, reads `d` from positional arg 2); Rust `st_dwithin(a, b, d) -> ExprRef`; Python `st_dwithin(geom_a, geom_b, distance) -> Expression`. NOT yet added to any join/accelerator name list — that is Task 6.

- [ ] **Step 1: Make `geom_distance` shareable**

In `src/daft-geo/src/st_distance.rs`, change `fn geom_distance(` to `pub(crate) fn geom_distance(`.

- [ ] **Step 2: Write the failing Rust unit test**

Create `src/daft-geo/src/st_dwithin.rs` with only a test first:
```rust
#[cfg(test)]
mod tests {
    use geo::{Geometry, Point};
    use crate::st_distance::geom_distance;

    #[test]
    fn test_dwithin_boundary() {
        let a = Geometry::Point(Point::new(0.0, 0.0));
        let b = Geometry::Point(Point::new(3.0, 4.0)); // distance = 5.0
        assert!(geom_distance(&a, &b) <= 5.0 + 1e-9);
        assert!(geom_distance(&a, &b) > 4.9);
    }
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cargo test -p daft-geo st_dwithin`
Expected: FAIL — `module st_dwithin not declared` (until lib.rs decl) OR compile error. (If the module isn't declared yet, add `pub mod st_dwithin;` to lib.rs first, then this test compiles and fails appropriately. Declaring the module is part of Step 5.)

- [ ] **Step 4: Implement `StDwithin`**

Replace `src/daft-geo/src/st_dwithin.rs` contents with (keeping the test module):
```rust
use common_error::DaftResult;
use daft_core::{prelude::{DataType, Field, Schema}, series::Series};
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use geo::Geometry;
use serde::{Deserialize, Serialize};

use crate::st_distance::geom_distance;
use crate::utils::{binary_geom_to_bool, read_f64_arg, read_f64_arg_expr, validate_geometry_field};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StDwithin;

#[typetag::serde]
impl ScalarUDF for StDwithin {
    fn name(&self) -> &'static str { "st_dwithin" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        let d = read_f64_arg(&inputs, 2, "distance", self.name())?;
        binary_geom_to_bool(inputs.required(0)?, inputs.required(1)?, self.name(),
            move |a: &Geometry, b: &Geometry| {
                let dist = geom_distance(a, b);
                dist.is_finite() && dist <= d
            })
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom_a", self.name())?;
        validate_geometry_field(&inputs, schema, 1, "geom_b", self.name())?;
        read_f64_arg_expr(&inputs, 2, "distance", self.name())?; // validate numeric literal at plan time
        Ok(Field::new(self.name(), DataType::Boolean))
    }

    fn docstring(&self) -> &'static str {
        "Returns true if the planar distance between A and B is <= distance (coordinate units)."
    }
}

#[must_use]
pub fn st_dwithin(geom_a: ExprRef, geom_b: ExprRef, distance: ExprRef) -> ExprRef {
    ScalarFn::builtin(StDwithin, vec![geom_a, geom_b, distance]).into()
}

#[cfg(test)]
mod tests {
    use geo::{Geometry, Point};
    use crate::st_distance::geom_distance;

    #[test]
    fn test_dwithin_boundary() {
        let a = Geometry::Point(Point::new(0.0, 0.0));
        let b = Geometry::Point(Point::new(3.0, 4.0)); // distance = 5.0
        assert!(geom_distance(&a, &b) <= 5.0 + 1e-9);
        assert!(geom_distance(&a, &b) > 4.9);
    }
}
```

- [ ] **Step 5: Register in `lib.rs`**

In `src/daft-geo/src/lib.rs`: add `pub mod st_dwithin;`, `pub use st_dwithin::StDwithin;`, and in `register`: `parent.add_fn(StDwithin);`.

- [ ] **Step 6: Register in SQL**

In `src/daft-sql/src/modules/spatial.rs`: add `StDwithin` to the `use daft_geo::{…}` list. Add a 3-arg wrapper modeled on `SQLStBuffer` (near the other custom wrappers):
```rust
// ── st_dwithin(geom, geom, distance) ─────────────────────────────────────────
pub struct SQLStDwithin;

impl SQLFunction for SQLStDwithin {
    fn to_expr(
        &self,
        inputs: &[ast::FunctionArg],
        planner: &crate::planner::SQLPlanner,
    ) -> SQLPlannerResult<ExprRef> {
        if inputs.len() != 3 {
            invalid_operation_err!("st_dwithin expects 3 arguments (geom, geom, distance), got {}", inputs.len());
        }
        let a = planner.plan_function_arg(&inputs[0])?.into_inner();
        let b = planner.plan_function_arg(&inputs[1])?.into_inner();
        let distance_expr = planner.plan_function_arg(&inputs[2])?.into_inner();
        let _ = distance_expr
            .as_literal()
            .and_then(|l| l.as_f64().or_else(|| l.as_i64().map(|v| v as f64)))
            .ok_or_else(|| crate::error::PlannerError::invalid_operation(
                "st_dwithin: distance must be a numeric literal",
            ))?;
        Ok(BuiltinScalarFn {
            func: BuiltinScalarFnVariant::Sync(Arc::new(StDwithin)),
            inputs: FunctionArgs::new_unchecked(vec![
                daft_dsl::functions::FunctionArg::unnamed(a),
                daft_dsl::functions::FunctionArg::unnamed(b),
                daft_dsl::functions::FunctionArg::unnamed(distance_expr),
            ]),
        }
        .into())
    }

    fn docstrings(&self, _alias: &str) -> String {
        "Returns true if the planar distance between two geometries is <= distance.".to_string()
    }
}
```
and register it in `SQLModuleSpatial::register`: `parent.add_fn("st_dwithin", SQLStDwithin);`

- [ ] **Step 7: Add Python wrapper + exports**

In `daft/functions/spatial.py`:
```python
def st_dwithin(geom_a: Expression, geom_b: Expression, distance: float) -> Expression:
    """Returns true if the planar distance between two geometries is <= ``distance`` (coordinate units).

    ``distance`` must be a numeric literal.
    """
    return Expression._call_builtin_scalar_fn("st_dwithin", geom_a, geom_b, distance)
```
In `daft/functions/__init__.py`: add `st_dwithin` to the spatial import block and to `__all__`.

- [ ] **Step 8: Write the Python `.where()` correctness test**

Add to `tests/expressions/test_spatial.py`:
```python
def test_st_dwithin_filter():
    import daft
    from daft.functions import st_dwithin, st_point

    df = daft.from_pydict({"id": [1, 2, 3], "x": [0.0, 3.0, 10.0], "y": [0.0, 4.0, 10.0]}).select(
        daft.col("id"),
        st_point(daft.col("x"), daft.col("y")).alias("g"),
    )
    origin = daft.from_pydict({"ox": [0.0], "oy": [0.0]}).select(
        st_point(daft.col("ox"), daft.col("oy")).alias("o")
    )
    # distance from origin: id1=0, id2=5, id3=~14.14
    out = (
        df.join(origin, how="cross")
        .where(st_dwithin(daft.col("g"), daft.col("o"), 5.0))
        .select("id")
        .sort("id")
        .to_pydict()
    )
    assert out["id"] == [1, 2]  # id3 is beyond distance 5
```

- [ ] **Step 9: Build and run**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k dwithin tests/expressions/test_spatial.py"` and `cargo test -p daft-geo st_dwithin`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add src/daft-geo/ src/daft-sql/src/modules/spatial.rs daft/functions/ tests/expressions/test_spatial.py
git commit -m "feat(geo): st_dwithin planar distance predicate (Python+SQL)" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `st_bbox` UDF + `df.with_spatial_bbox()` helper

**Files:**
- Create: `src/daft-geo/src/st_bbox.rs`
- Modify: `src/daft-geo/src/lib.rs`
- Modify: `daft/functions/spatial.py`, `daft/functions/__init__.py` (export `st_bbox`), `daft/dataframe/dataframe.py` (add `with_spatial_bbox`)
- Test: `tests/expressions/test_spatial.py`

**Interfaces:**
- Consumes: `crate::mbr::wkb_to_mbr`, `crate::utils::get_geometry_binary`; `daft_core::prelude::{StructArray, Float64Array, Field, DataType}`.
- Produces: struct `StBbox` (name `"st_bbox"`, return `Struct{min_x,min_y,max_x,max_y: Float64}`); Rust `st_bbox(geom) -> ExprRef`; Python `st_bbox(geom) -> Expression`; `DataFrame.with_spatial_bbox(geom_col: str, *, prefix: str = "") -> DataFrame` adding columns `{prefix}min_x/{prefix}min_y/{prefix}max_x/{prefix}max_y` (the names the join fast-path detects).

- [ ] **Step 1: Write the failing Rust unit test**

Create `src/daft-geo/src/st_bbox.rs` with the test first:
```rust
#[cfg(test)]
mod tests {
    use crate::mbr::wkb_to_mbr;
    #[test]
    fn test_wkb_to_mbr_point() {
        // WKB POINT(1 2) little-endian
        let wkb = hex::decode("0101000000000000000000F03F0000000000000040").unwrap();
        let [min_x, min_y, max_x, max_y] = wkb_to_mbr(&wkb).unwrap();
        assert_eq!((min_x, min_y, max_x, max_y), (1.0, 2.0, 1.0, 2.0));
    }
}
```
(If `hex` is not a dev-dependency of daft-geo, build the bytes inline instead: `let wkb: &[u8] = &[0x01,0x01,0x00,0x00,0x00, 0,0,0,0,0,0,0xF0,0x3F, 0,0,0,0,0,0,0,0x40];`. Use the inline form to avoid adding a dependency.)

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p daft-geo st_bbox` → FAIL (module not declared until Step 4 adds the decl; add `pub mod st_bbox;` to lib.rs first so the test compiles and runs).

- [ ] **Step 3: Implement `StBbox`**

`src/daft-geo/src/st_bbox.rs` (model the struct construction on `daft-functions/src/to_struct.rs`'s `StructArray::new`):
```rust
use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::{ExprRef, functions::{FunctionArgs, ScalarUDF, scalar::ScalarFn}};
use serde::{Deserialize, Serialize};

use crate::mbr::wkb_to_mbr;
use crate::utils::{get_geometry_binary, validate_geometry_field};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StBbox;

fn bbox_struct_fields() -> Vec<Field> {
    vec![
        Field::new("min_x", DataType::Float64),
        Field::new("min_y", DataType::Float64),
        Field::new("max_x", DataType::Float64),
        Field::new("max_y", DataType::Float64),
    ]
}

#[typetag::serde]
impl ScalarUDF for StBbox {
    fn name(&self) -> &'static str { "st_bbox" }

    fn call(&self, inputs: FunctionArgs<Series>, _ctx: &daft_dsl::functions::scalar::EvalContext) -> DaftResult<Series> {
        let series = inputs.required(0)?;
        let binary = get_geometry_binary(series)?;
        let len = binary.len();
        let mut min_x = Vec::with_capacity(len);
        let mut min_y = Vec::with_capacity(len);
        let mut max_x = Vec::with_capacity(len);
        let mut max_y = Vec::with_capacity(len);
        for i in 0..len {
            match binary.get(i).and_then(wkb_to_mbr) {
                Some([mnx, mny, mxx, mxy]) => {
                    min_x.push(Some(mnx)); min_y.push(Some(mny));
                    max_x.push(Some(mxx)); max_y.push(Some(mxy));
                }
                None => { min_x.push(None); min_y.push(None); max_x.push(None); max_y.push(None); }
            }
        }
        let children = vec![
            Float64Array::from_iter(Field::new("min_x", DataType::Float64), min_x.into_iter()).into_series(),
            Float64Array::from_iter(Field::new("min_y", DataType::Float64), min_y.into_iter()).into_series(),
            Float64Array::from_iter(Field::new("max_x", DataType::Float64), max_x.into_iter()).into_series(),
            Float64Array::from_iter(Field::new("max_y", DataType::Float64), max_y.into_iter()).into_series(),
        ];
        let field = Field::new(self.name(), DataType::Struct(bbox_struct_fields()));
        Ok(StructArray::new(field, children, None).into_series())
    }

    fn get_return_field(&self, inputs: FunctionArgs<ExprRef>, schema: &Schema) -> DaftResult<Field> {
        validate_geometry_field(&inputs, schema, 0, "geom", self.name())?;
        Ok(Field::new(self.name(), DataType::Struct(bbox_struct_fields())))
    }

    fn docstring(&self) -> &'static str {
        "Returns the geometry's bounding box as a struct {min_x, min_y, max_x, max_y} (Float64)."
    }
}

#[must_use]
pub fn st_bbox(geom: ExprRef) -> ExprRef {
    ScalarFn::builtin(StBbox, vec![geom]).into()
}
```
**Note on `Float64Array::from_iter` signature:** confirm the exact constructor by matching how an existing `daft-geo` file builds a Float64 series (e.g. how `unary_geom_to_f64` in `utils.rs` constructs its output). If `from_iter` takes `&Field` or a different arg shape, mirror that call exactly. The struct assembly via `StructArray::new(field, children, None)` matches `to_struct.rs`.

- [ ] **Step 4: Register in `lib.rs`**

`pub mod st_bbox;`, `pub use st_bbox::StBbox;`, and `parent.add_fn(StBbox);` in `register`.

- [ ] **Step 5: Run the Rust test**

Run: `cargo test -p daft-geo st_bbox` → PASS.

- [ ] **Step 6: Python wrapper + helper**

In `daft/functions/spatial.py`:
```python
def st_bbox(geom: Expression) -> Expression:
    """Returns the geometry's bounding box as a struct ``{min_x, min_y, max_x, max_y}`` (Float64)."""
    return Expression._call_builtin_scalar_fn("st_bbox", geom)
```
Export `st_bbox` in `daft/functions/__init__.py` (import block + `__all__`).

In `daft/dataframe/dataframe.py`, add a public method on `DataFrame` (place near other projection helpers; use the `@DataframePublicAPI` decorator if neighboring public methods use it):
```python
    @DataframePublicAPI
    def with_spatial_bbox(self, geom_col: str, *, prefix: str = "") -> "DataFrame":
        """Add ``{prefix}min_x``, ``{prefix}min_y``, ``{prefix}max_x``, ``{prefix}max_y`` Float64 columns
        holding the bounding box of ``geom_col``.

        These are the column names the native spatial-join operator detects as a precomputed
        index, letting it skip per-row WKB bounding-box extraction during the join.

        Args:
            geom_col: name of a Geometry (or WKB Binary) column.
            prefix: optional prefix for the four output column names (e.g. ``"bbox_"``).
        """
        from daft.functions import st_bbox

        bbox = st_bbox(col(geom_col))
        return self.with_columns(
            {
                f"{prefix}min_x": bbox.struct.get("min_x"),
                f"{prefix}min_y": bbox.struct.get("min_y"),
                f"{prefix}max_x": bbox.struct.get("max_x"),
                f"{prefix}max_y": bbox.struct.get("max_y"),
            }
        )
```
(`col` is already imported in `dataframe.py`; confirm and use the module's existing column constructor. `bbox.struct.get(...)` is the struct accessor at `expressions.py:2784`.)

- [ ] **Step 7: Write the Python test**

Add to `tests/expressions/test_spatial.py`:
```python
def test_with_spatial_bbox():
    import daft
    from daft.functions import st_point

    df = daft.from_pydict({"x": [1.0, 5.0], "y": [2.0, 6.0]}).select(
        st_point(daft.col("x"), daft.col("y")).alias("g")
    )
    out = df.with_spatial_bbox("g").select("min_x", "min_y", "max_x", "max_y").to_pydict()
    assert out["min_x"] == [1.0, 5.0]
    assert out["min_y"] == [2.0, 6.0]
    assert out["max_x"] == [1.0, 5.0]
    assert out["max_y"] == [2.0, 6.0]
```

- [ ] **Step 8: Build and run**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k spatial_bbox tests/expressions/test_spatial.py"`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/daft-geo/ daft/functions/ daft/dataframe/dataframe.py tests/expressions/test_spatial.py
git commit -m "feat(geo): st_bbox struct UDF + df.with_spatial_bbox index helper" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Route spatial `JOIN ON` to the R-tree operator (translate.rs)

**Files:**
- Modify: `src/daft-local-plan/src/translate.rs`
- Test: `tests/expressions/test_spatial_join.py` (new)

**Interfaces:**
- Consumes: existing `is_spatial_predicate`, `rebind_predicate`, `LocalPhysicalPlan::nested_loop_join`, `JoinPredicate::{inner, is_empty, split_eq_preds}`.
- Produces: a private helper `build_spatial_nested_loop_join(join, predicate_expr, phys_left_raw, phys_right_raw, left_inputs, stats_state) -> DaftResult<(LocalPhysicalPlanRef, Vec<Input>)>` reused by BOTH the Filter-over-join rewrite and the Join arm. After this task, SQL `JOIN ... ON ST_Intersects(a, b)` (and any spatial predicate already in `SPATIAL_PREDICATES`) executes via `NestedLoopJoin` instead of erroring.

**Background:** The Filter-over-join rewrite (`translate.rs` lines ~158–293) already builds the spatial `NestedLoopJoin`: it picks the build side from the spatial function's arg0 column index, extracts an optional equality partition key from `join.on.split_eq_preds()`, rebinds the predicate to `join.output_schema`, and emits `nested_loop_join`. This task factors that block into a reusable helper and calls it from the `Join` arm when the non-equi residual is spatial. The residual from `split_eq_preds` retains `ResolvedColumn::JoinSide` markers, so it must be stripped to plain column names before `rebind_predicate`.

- [ ] **Step 1: Write the failing test (SQL spatial JOIN ON)**

Create `tests/expressions/test_spatial_join.py`:
```python
from __future__ import annotations

import daft
from daft.functions import st_intersects, st_point


def _points_polys():
    # points: id 1 at (1,1) inside; id 2 at (9,9) outside the polygon
    pts = daft.from_pydict({"pid": [1, 2], "px": [1.0, 9.0], "py": [1.0, 9.0]}).select(
        daft.col("pid"), st_point(daft.col("px"), daft.col("py")).alias("pgeom")
    )
    # one polygon (0,0)-(2,2) built from a point's envelope substitute: use a WKT polygon
    from daft.functions import st_geomfromtext

    polys = daft.from_pydict(
        {"qid": [10], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"]}
    ).select(daft.col("qid"), st_geomfromtext(daft.col("wkt")).alias("qgeom"))
    return pts, polys


def test_sql_spatial_join_on():
    pts, polys = _points_polys()
    daft.sql_expr  # ensure sql module import side-effect (no-op)
    result = daft.sql(
        "SELECT pid, qid FROM pts JOIN polys ON ST_Intersects(pgeom, qgeom) ORDER BY pid",
        pts=pts,
        polys=polys,
    ).to_pydict()
    # only point 1 intersects the polygon
    assert result["pid"] == [1]
    assert result["qid"] == [10]
```
(Adjust the `daft.sql(...)` invocation to match the repo's SQL entry point — confirm whether catalog tables are passed as keyword args or registered via `daft.sql_expr`/a session. Use the same form other `tests/` SQL tests use.)

- [ ] **Step 2: Run it to verify it fails**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial_join.py::test_sql_spatial_join_on"`
Expected: FAIL with `not_implemented("Execution of non-equality join")`.

- [ ] **Step 3: Extract the shared helper**

In `src/daft-local-plan/src/translate.rs`, add a helper function (above `translate_helper` or near `rebind_predicate`). Move the body of the Filter-rewrite (the code that currently runs once `join.join_type == JoinType::Inner`, from computing `output_schema`/`filter_expr` through the `return Ok((LocalPhysicalPlan::nested_loop_join(...), left_inputs))`) into it. The helper receives already-translated child plans:
```rust
/// Build the R-tree-backed `NestedLoopJoin` for a spatial-predicate inner join.
/// `predicate_expr` references columns by name resolvable in `join.output_schema`
/// (JoinSide markers already stripped). `phys_left`/`phys_right` are the translated
/// children in logical-join order (left, right).
fn build_spatial_nested_loop_join(
    join: &daft_logical_plan::ops::Join,
    predicate_expr: ExprRef,
    phys_left: LocalPhysicalPlanRef,
    phys_right: LocalPhysicalPlanRef,
    left_inputs: Vec<Input>,
    stats_state: StatsState,
) -> DaftResult<(LocalPhysicalPlanRef, Vec<Input>)> {
    let output_schema = join.output_schema.clone();
    let filter_expr = rebind_predicate(predicate_expr, &output_schema)?;

    let left_schema_len = join.left.schema().len();
    let build_on_left: bool = (|| -> bool {
        fn spatial_arg0_idx(expr: &ExprRef) -> Option<usize> {
            match expr.as_ref() {
                Expr::ScalarFn(daft_dsl::functions::scalar::ScalarFn::Builtin(sf))
                    if SPATIAL_PREDICATES.contains(&sf.func.name()) =>
                {
                    let arg0 = sf.inputs.required(0).ok()?;
                    if let Expr::Column(Column::Bound(bc)) = arg0.as_ref() { Some(bc.index) } else { None }
                }
                Expr::BinaryOp { left, right, .. } => spatial_arg0_idx(left).or_else(|| spatial_arg0_idx(right)),
                Expr::Not(inner) => spatial_arg0_idx(inner),
                _ => None,
            }
        }
        if let Some(idx) = spatial_arg0_idx(filter_expr.inner()) {
            idx < left_schema_len
        } else {
            let left_stats = join.left.materialized_stats();
            let right_stats = join.right.materialized_stats();
            left_stats.approx_stats.num_rows <= right_stats.approx_stats.num_rows
        }
    })();

    let (phys_left, phys_right, build_side) = if build_on_left {
        (phys_right, phys_left, JoinSide::Left)
    } else {
        (phys_left, phys_right, JoinSide::Right)
    };

    let partition_key: Option<[usize; 2]> = (|| -> Option<[usize; 2]> {
        let (_, left_eq_keys, right_eq_keys, _) = join.on.split_eq_preds();
        if left_eq_keys.len() != 1 || right_eq_keys.len() != 1 { return None; }
        let (probe_eq_key, build_eq_key) = if build_on_left {
            (&right_eq_keys[0], &left_eq_keys[0])
        } else {
            (&left_eq_keys[0], &right_eq_keys[0])
        };
        let probe_col_name = match probe_eq_key.as_ref() { Expr::Column(col) => col.name(), _ => return None };
        let build_col_name = match build_eq_key.as_ref() { Expr::Column(col) => col.name(), _ => return None };
        let probe_idx = phys_left.schema().get_index(&probe_col_name).ok()?;
        let build_idx = phys_right.schema().get_index(&build_col_name).ok()?;
        Some([build_idx, probe_idx])
    })();

    Ok((
        LocalPhysicalPlan::nested_loop_join(
            phys_left, phys_right, filter_expr, build_side, partition_key,
            output_schema, stats_state, LocalNodeContext::default(),
        ),
        left_inputs,
    ))
}
```
Then rewrite the Filter arm's spatial branch to translate the children and call the helper:
```rust
                if let Some(join) = maybe_join {
                    if join.join_type == JoinType::Inner {
                        let (left_plan, mut left_inputs) = translate_helper(&join.left, source_counter, psets)?;
                        let (right_plan, right_inputs) = translate_helper(&join.right, source_counter, psets)?;
                        left_inputs.extend(right_inputs);
                        return build_spatial_nested_loop_join(
                            join,
                            filter.predicate.clone(),
                            left_plan,
                            right_plan,
                            left_inputs,
                            filter.stats_state.clone(),
                        );
                    }
                }
```
(`build_spatial_nested_loop_join` calls `rebind_predicate(filter.predicate, output_schema)` internally — identical to the old inline behavior, since the old code also rebound `filter.predicate` to `join.output_schema`.)

- [ ] **Step 4: Add the JoinSide-stripping helper**

Add to `translate.rs` (and add `ResolvedColumn` to the `daft_dsl::expr` imports):
```rust
/// Convert `ResolvedColumn::JoinSide(field, _)` markers in a join residual predicate
/// into plain unresolved column references (by the post-deduplication field name),
/// so the predicate can be re-bound against the join output schema.
fn strip_join_side_cols(expr: ExprRef) -> DaftResult<ExprRef> {
    Ok(expr
        .transform(|e| match e.as_ref() {
            Expr::Column(Column::Resolved(daft_dsl::expr::ResolvedColumn::JoinSide(field, _))) => {
                Ok(Transformed::yes(unresolved_col(field.name.clone())))
            }
            _ => Ok(Transformed::no(e)),
        })?
        .data)
}
```
(Confirm the exact path of `ResolvedColumn` and the `JoinSide(field, side)` variant shape from `daft_dsl`; mirror how `JoinPredicate::replace_join_side_cols` in `src/daft-logical-plan/src/ops/join.rs` matches it.)

- [ ] **Step 5: Route the spatial residual in the Join arm**

In the `LogicalPlan::Join` arm, replace the `if !remaining_on.is_empty() { return Err(...) }` block (lines ~652–654) with:
```rust
            if !remaining_on.is_empty() {
                let resid = remaining_on.inner().expect("non-empty residual has an expr").clone();
                if join.join_type == JoinType::Inner && is_spatial_predicate(&resid) {
                    let predicate = strip_join_side_cols(resid)?;
                    return build_spatial_nested_loop_join(
                        join,
                        predicate,
                        left_plan,
                        right_plan,
                        left_inputs,
                        join.stats_state.clone(),
                    );
                }
                return Err(DaftError::not_implemented("Execution of non-equality join"));
            }
```
(`left_plan`/`right_plan`/`left_inputs` are already in scope from lines 644–648.)

- [ ] **Step 6: Build and run the SQL join test**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial_join.py::test_sql_spatial_join_on"`
Expected: PASS.

- [ ] **Step 7: Run the existing spatial filter/pruning tests (no regression)**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial_geohash_pruning.py"`
Expected: all PASS (the Filter-rewrite refactor preserved behavior).

- [ ] **Step 8: Commit**

```bash
git add src/daft-local-plan/src/translate.rs tests/expressions/test_spatial_join.py
git commit -m "feat(geo): route spatial JOIN ON predicates to the R-tree nested-loop join" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Python `df.join(on=<predicate>)`

**Files:**
- Modify: `src/daft-logical-plan/src/builder/mod.rs` (PyO3 `join` binding: add `on_predicate`)
- Modify: `daft/daft/__init__.pyi` (the `join` stub)
- Modify: `daft/dataframe/dataframe.py` (`.join` predicate detection)
- Test: `tests/expressions/test_spatial_join.py`

**Interfaces:**
- Consumes: the logical `builder.join(on: Option<ExprRef>, …)` predicate path (already exists); `Expression.is_column()`; Task 4's translate routing.
- Produces: `df1.join(df2, on=<boolean Expression>)` executes a spatial join (inner). Guards: predicate `on` with `left_on`/`right_on` → error; `how != "inner"` with predicate `on` → error.

- [ ] **Step 1: Write the failing test**

Add to `tests/expressions/test_spatial_join.py`:
```python
def test_python_join_on_predicate():
    from daft.functions import st_intersects

    pts, polys = _points_polys()
    result = (
        pts.join(polys, on=st_intersects(pts["pgeom"], polys["qgeom"]))
        .select("pid", "qid")
        .sort("pid")
        .to_pydict()
    )
    assert result["pid"] == [1]
    assert result["qid"] == [10]


def test_python_join_on_predicate_rejects_outer():
    import pytest

    from daft.functions import st_intersects

    pts, polys = _points_polys()
    with pytest.raises(ValueError, match="inner"):
        pts.join(polys, on=st_intersects(pts["pgeom"], polys["qgeom"]), how="left")
```
(`pts["pgeom"]` / `polys["qgeom"]` have distinct names, satisfying the distinct-name constraint.)

- [ ] **Step 2: Run it to verify it fails**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial_join.py::test_python_join_on_predicate"`
Expected: FAIL — the predicate is currently routed as an equi-key (wrong results or a resolution error).

- [ ] **Step 3: Add `on_predicate` to the PyO3 binding**

In `src/daft-logical-plan/src/builder/mod.rs`, the `#[pymethods]` `join` (line ~1497): add a parameter `on_predicate: Option<PyExpr>` (place it last with a default via `#[pyo3(signature = (...))]` if the impl uses an explicit signature; otherwise add it as a trailing `Option`). When `on_predicate` is `Some`, take the predicate path; otherwise keep the existing equi-key path. Concretely, after computing `join_type`/`join_strategy`, branch:
```rust
        if let Some(pred) = on_predicate {
            let builder = self.builder.join(
                right.builder.clone(),
                Some(pred.into()),
                vec![],
                join_type,
                join_strategy,
                JoinOptions { prefix, suffix, ..Default::default() },
            )?;
            return Ok(builder.into());
        }
```
(Match the exact field/return types used by the existing equi-key path in this method — `self.builder`, the `JoinOptions`/`ops::join` option struct it already constructs, and how it wraps the result. The non-predicate path is unchanged.)

- [ ] **Step 4: Update the `.pyi` stub**

In `daft/daft/__init__.pyi`, the `LogicalPlanBuilder.join` stub: add `on_predicate: PyExpr | None = None,` before the closing `) -> LogicalPlanBuilder: ...`.

- [ ] **Step 5: Add predicate detection in `.join()`**

In `daft/dataframe/dataframe.py` `.join`, replace the `else:` branch that sets `left_on = on; right_on = on` (lines ~4181–4185) with predicate detection. Add `from daft.expressions import Expression` at the top if not already imported (it is used elsewhere in the file — confirm). New logic:
```python
        on_predicate = None
        if how != "cross" and on is not None and isinstance(on, Expression) and not on.is_column():
            # `on` is a boolean predicate (e.g. st_intersects(...)) → predicate join.
            if left_on is not None or right_on is not None:
                raise ValueError("If `on` is a predicate expression, `left_on`/`right_on` must be None")
            if how != "inner":
                raise ValueError("Predicate (e.g. spatial) joins support how='inner' only")
            on_predicate = on
            left_on = []
            right_on = []
        elif how == "cross":
            ...  # existing cross-join handling
        elif on is None:
            ...  # existing
        else:
            ...  # existing on→left_on/right_on
```
Restructure the existing `if how == "cross" / elif on is None / else` chain so the new predicate branch is checked first (and the cross-join branch still runs its existing validation). Then pass the predicate to the builder:
```python
        builder = self._builder.join(
            other._builder,
            left_on=left_exprs,
            right_on=right_exprs,
            how=join_type,
            strategy=join_strategy,
            prefix=prefix,
            suffix=suffix,
            on_predicate=on_predicate._expr if on_predicate is not None else None,
        )
```
(When `on_predicate` is set, `left_exprs`/`right_exprs` are empty lists. `on_predicate._expr` is the underlying `PyExpr` — confirm the attribute name used elsewhere in this file, e.g. how `_call_builtin_scalar_fn` unwraps `Expression`.)

- [ ] **Step 6: Build and run**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial_join.py"`
Expected: all PASS (SQL test from Task 4 + the two new Python tests).

- [ ] **Step 7: Run the full join suite (no regression)**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/dataframe/test_joins.py"`
Expected: PASS (equi-key joins unaffected — `on_predicate` defaults to `None`).

- [ ] **Step 8: Commit**

```bash
git add src/daft-logical-plan/src/builder/mod.rs daft/daft/__init__.pyi daft/dataframe/dataframe.py tests/expressions/test_spatial_join.py
git commit -m "feat(geo): df.join(on=<predicate>) for spatial joins (inner)" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: `st_dwithin` R-tree acceleration

**Files:**
- Modify: `src/daft-local-plan/src/translate.rs` (`SPATIAL_PREDICATES`: add `"st_dwithin"`)
- Modify: `src/daft-local-execution/src/join/nested_loop_join.rs` (`SPATIAL_FNS` + distance extraction + query-box expansion)
- Test: `tests/expressions/test_spatial_join.py`

**Interfaces:**
- Consumes: the routing from Task 4 (`build_spatial_nested_loop_join`) and the operator from `main`.
- Produces: `st_dwithin` joins routed to and accelerated by the R-tree operator (query AABB expanded by `d`; exact filter refines). Correctness invariant: expanding by `d` is a sound over-approximation (if true distance ≤ d then MBRs are within d), so no false negatives.

**Correctness note:** `st_dwithin` must NOT appear in the operator's `SPATIAL_FNS` without the query-box expansion — querying the R-tree with the un-expanded probe MBR would miss geometries within distance `d` whose MBRs do not overlap, producing wrong (missing) rows. This task adds the name AND the expansion together.

- [ ] **Step 1: Write the failing test**

Add to `tests/expressions/test_spatial_join.py`:
```python
def test_python_join_st_dwithin():
    from daft.functions import st_dwithin, st_point

    left = daft.from_pydict({"lid": [1, 2], "lx": [0.0, 0.0], "ly": [0.0, 0.0]}).select(
        daft.col("lid"), st_point(daft.col("lx"), daft.col("ly")).alias("lg")
    )
    right = daft.from_pydict({"rid": [10, 11], "rx": [3.0, 100.0], "ry": [4.0, 100.0]}).select(
        daft.col("rid"), st_point(daft.col("rx"), daft.col("ry")).alias("rg")
    )
    # distance lid->rid: (1,10)=5, (1,11)=~141, (2,10)=5, (2,11)=~141
    result = (
        left.join(right, on=st_dwithin(left["lg"], right["rg"], 5.0))
        .select("lid", "rid")
        .sort(["lid", "rid"])
        .to_pydict()
    )
    assert list(zip(result["lid"], result["rid"])) == [(1, 10), (2, 10)]
```

- [ ] **Step 2: Run it to verify it fails**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial_join.py::test_python_join_st_dwithin"`
Expected: FAIL with `not_implemented("Execution of non-equality join")` (st_dwithin not yet in `SPATIAL_PREDICATES`).

- [ ] **Step 3: Add `st_dwithin` to the translate routing list**

In `src/daft-local-plan/src/translate.rs`, add `"st_dwithin"` to the `SPATIAL_PREDICATES` array.

- [ ] **Step 4: Add distance extraction + query-box expansion in the operator**

In `src/daft-local-execution/src/join/nested_loop_join.rs`:

(a) Add `"st_dwithin"` to the `SPATIAL_FNS` array (line ~77).

(b) Change `extract_from_expr` / `extract_geom_col_indices` to also return the optional distance. The simplest approach: add a parallel walk `extract_dwithin_distance(expr) -> Option<f64>` that finds an `st_dwithin` builtin and reads its arg2 literal:
```rust
fn extract_dwithin_distance(expr: &ExprRef) -> Option<f64> {
    match expr.as_ref() {
        Expr::ScalarFn(daft_dsl::functions::scalar::ScalarFn::Builtin(sf)) if sf.name() == "st_dwithin" => {
            let d = sf.inputs.required(2).ok()?;
            d.as_literal().and_then(|l| l.as_f64().or_else(|| l.as_i64().map(|v| v as f64)))
        }
        Expr::BinaryOp { left, right, .. } => extract_dwithin_distance(left).or_else(|| extract_dwithin_distance(right)),
        Expr::Not(inner) => extract_dwithin_distance(inner),
        _ => None,
    }
}
```
Store the distance on the operator: add a field `dwithin_distance: Option<f64>` to `NestedLoopJoinOperator`, set in `new()` via `extract_dwithin_distance(filter.inner())`. Thread it into both `RTreeState` and `PartitionedRTreeState` probe paths (add a `query_pad: f64` carried into the probe closures; default `0.0` for topological predicates, `d` for st_dwithin).

(c) In BOTH probe paths, where the query box is built from the probe MBR:
```rust
                            let q = AABB::from_corners(
                                [min_x - pad, min_y - pad],
                                [max_x + pad, max_y + pad],
                            );
```
where `pad = self.dwithin_distance.unwrap_or(0.0)` captured into the spawned task (clone the f64 alongside the other captured fields, like `filter`/`build_side`).

- [ ] **Step 5: Build and run**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial_join.py::test_python_join_st_dwithin"`
Expected: PASS.

- [ ] **Step 6: Re-run the whole spatial-join test file (no regression)**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial_join.py"`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/daft-local-plan/src/translate.rs src/daft-local-execution/src/join/nested_loop_join.rs tests/expressions/test_spatial_join.py
git commit -m "feat(geo): accelerate st_dwithin joins via R-tree query-box expansion" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Cross-cutting join tests, oracle equivalence, index equivalence & docs

**Files:**
- Modify: `tests/expressions/test_spatial_join.py`
- Modify/Create: spatial/IO docs (find the existing spatial docs surface; if none, document on the join API + the `st_*` function docstrings already added)
- Test: full spatial suite

**Interfaces:**
- Consumes: everything from Tasks 1–6.
- Produces: oracle-equivalence tests proving the R-tree join matches a brute-force cross-join+filter; a partitioned-path test (equi-key + spatial predicate); a bbox-helper equivalence test; docs.

- [ ] **Step 1: Oracle-equivalence test (intersects + contains)**

Add to `tests/expressions/test_spatial_join.py`:
```python
import pytest


def _oracle_join(left, right, predicate_fn, lcol, rcol):
    # brute-force: cross join + filter, the ground truth the R-tree path must match
    return (
        left.join(right, how="cross")
        .where(predicate_fn(daft.col(lcol), daft.col(rcol)))
        .select(sorted(set(left.column_names + right.column_names)))
    )


@pytest.mark.parametrize("predicate_name", ["st_intersects", "st_contains"])
def test_spatial_join_matches_oracle(predicate_name):
    from daft import functions as F
    from daft.functions import st_point, st_geomfromtext

    pred = getattr(F, predicate_name)
    pts = daft.from_pydict({"pid": [1, 2, 3], "x": [1.0, 9.0, 0.5], "y": [1.0, 9.0, 0.5]}).select(
        daft.col("pid"), st_point(daft.col("x"), daft.col("y")).alias("pg")
    )
    polys = daft.from_pydict(
        {"qid": [10, 11], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))", "POLYGON((8 8,10 8,10 10,8 10,8 8))"]}
    ).select(daft.col("qid"), st_geomfromtext(daft.col("wkt")).alias("qg"))

    # container (polygon) is arg0 for st_contains; for st_intersects order is symmetric
    got = pts.join(polys, on=pred(polys["qg"], pts["pg"])).select("pid", "qid").sort(["pid", "qid"]).to_pydict()
    oracle = (
        pts.join(polys, how="cross")
        .where(pred(daft.col("qg"), daft.col("pg")))
        .select("pid", "qid")
        .sort(["pid", "qid"])
        .to_pydict()
    )
    assert list(zip(got["pid"], got["qid"])) == list(zip(oracle["pid"], oracle["qid"]))
```

- [ ] **Step 2: Partitioned-path test (equi-key + spatial)**

Add a test that supplies an equality key in the predicate so the operator uses the partitioned R-tree path:
```python
def test_spatial_join_partitioned_by_key():
    from daft.functions import st_intersects, st_point, st_geomfromtext

    pts = daft.from_pydict(
        {"region": ["a", "a", "b"], "pid": [1, 2, 3], "x": [1.0, 9.0, 1.0], "y": [1.0, 9.0, 1.0]}
    ).select("region", "pid", st_point(daft.col("x"), daft.col("y")).alias("pg"))
    polys = daft.from_pydict(
        {"region": ["a", "b"], "qid": [10, 20], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))", "POLYGON((0 0,2 0,2 2,0 2,0 0))"]}
    ).select("region", "qid", st_geomfromtext(daft.col("wkt")).alias("qg"))

    # equality on region + spatial predicate → partitioned R-tree path
    got = (
        pts.join(polys, on=(pts["region"] == polys["region"]) & st_intersects(polys["qg"], pts["pg"]))
        .select("pid", "qid")
        .sort(["pid", "qid"])
        .to_pydict()
    )
    # region a: pid1 (in poly) -> qid10; pid2 outside. region b: pid3 -> qid20
    assert list(zip(got["pid"], got["qid"])) == [(1, 10), (3, 20)]
```
(If `region` collides as a name across both sides, the predicate's `==` is an equi-key extracted by `split_eq_preds`; `pts["region"]`/`polys["region"]` carry distinct sides. Confirm the predicate resolves; if same-name equi-keys are ambiguous in this construction, alias one side's region column and adjust.)

- [ ] **Step 3: Bbox-helper equivalence test**

```python
def test_spatial_join_bbox_index_equivalence():
    from daft.functions import st_intersects, st_point, st_geomfromtext

    pts = daft.from_pydict({"pid": [1, 2, 3], "x": [1.0, 9.0, 0.5], "y": [1.0, 9.0, 0.5]}).select(
        daft.col("pid"), st_point(daft.col("x"), daft.col("y")).alias("pg")
    )
    polys = daft.from_pydict(
        {"qid": [10], "wkt": ["POLYGON((0 0,2 0,2 2,0 2,0 0))"]}
    ).select(daft.col("qid"), st_geomfromtext(daft.col("wkt")).alias("qg"))

    base = pts.join(polys, on=st_intersects(polys["qg"], pts["pg"])).select("pid", "qid").sort("pid").to_pydict()
    # materialize the build-side bbox index columns the fast-path detects
    polys_idx = polys.with_spatial_bbox("qg")
    indexed = pts.join(polys_idx, on=st_intersects(polys_idx["qg"], pts["pg"])).select("pid", "qid").sort("pid").to_pydict()
    assert base == indexed
```

- [ ] **Step 4: Run the new tests**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial_join.py"`
Expected: all PASS.

- [ ] **Step 5: Full spatial suite**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/expressions/test_spatial.py tests/expressions/test_spatial_join.py tests/expressions/test_spatial_geohash_pruning.py"`
Expected: all PASS, pristine output.

- [ ] **Step 6: Docs**

Find the spatial documentation surface (search `docs/` for existing `st_`/spatial content; the geohash-pruning and spatial functions may already have a page). Add a concise "Spatial joins" section documenting: `df1.join(df2, on=st_intersects(a["g"], b["h"]))` (inner only; the two geometry columns must have distinct names); SQL `JOIN ... ON ST_Intersects(...)`; the predicates `st_covers`/`st_covered_by`/`st_dwithin(a, b, d)` (planar); and `df.with_spatial_bbox("geom")` as an optional precompute that the join uses as an index. If there is no spatial docs page, place this content where `read_parquet`/spatial function docstrings live and ensure the new function docstrings (already added in Tasks 1–3) are accurate.

- [ ] **Step 7: Commit**

```bash
git add tests/expressions/test_spatial_join.py docs/
git commit -m "test(geo): spatial-join oracle/partitioned/bbox-index equivalence + docs" -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Spatial join API (Python `on=` predicate) → Task 5. SQL `JOIN ON ST_*` → Task 4. ✓
- Missing predicates: `st_covers`/`st_covered_by` → Task 1; `st_dwithin` → Tasks 2 (predicate) + 6 (acceleration). ✓
- Index helper `with_spatial_bbox` → Task 3. ✓
- End-to-end join tests → Tasks 4–7 (oracle, partitioned, bbox equivalence, inner-only guard). ✓
- Inner-only constraint → Task 5 guard + Task 4 routing only for inner. ✓
- Reuse the existing operator (no new physical op) → Tasks 4/6 route to `nested_loop_join`. ✓
- Non-spatial non-equi keeps erroring → Task 4 (`is_spatial_predicate` gate). ✓
- Distinct-name constraint → Global Constraints + Task 5/7 tests use distinct names. ✓
- Docs → Task 7. ✓

**Placeholder scan:** No "TBD"/"add error handling" placeholders. Confirmation points are explicit and bounded (the exact `Float64Array::from_iter` arg shape; the `ResolvedColumn::JoinSide` variant path; the PyO3 `join` option-struct field names; the `daft.sql(...)` table-passing form) — each names the precise existing code to mirror, which is appropriate for a codebase the implementer must match rather than guess.

**Type consistency:** `RelatePred::Covers`/`CoveredBy` (Task 1) used only in Task 1. `geom_distance` made `pub(crate)` in Task 2 and consumed by Task 2's `st_dwithin`. `build_spatial_nested_loop_join` defined in Task 4, reused conceptually by Task 6's routing (via the same `SPATIAL_PREDICATES` gate). `st_bbox` struct field names `min_x/min_y/max_x/max_y` (Task 3) match the column names the operator fast-path detects and `with_spatial_bbox` produces (Task 3) and the bbox-equivalence test asserts (Task 7). `on_predicate` parameter name consistent across Task 5 (PyO3 binding, `.pyi`, `.join`). ✓
