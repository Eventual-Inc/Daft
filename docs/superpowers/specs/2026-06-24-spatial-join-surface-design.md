# Spatial Sub-project C: Spatial Join Surface, Predicates & Index

- **Date:** 2026-06-24
- **Status:** Approved design, ready for implementation planning
- **Scope:** `src/daft-geo`, `src/daft-local-plan` (translate), `src/daft-logical-plan` (PyO3 join binding), `src/daft-local-execution` (nested-loop join accelerator), `src/daft-sql`, `daft/dataframe/dataframe.py`, `daft/functions/spatial.py`, `tests/`
- **Part of:** the 3-sub-project geospatial effort. Sub-project A (geometry functions & engine) and Sub-project B (GeoParquet I/O + Delta) are complete. This is **Sub-project C** (the final one).

## Summary

The R-tree spatial-join **engine already exists** on `main` (commit `c6480b6be` "feat(geo): spatial join acceleration with R-tree index and geohash partition pruning", + `6e852eb0e`, `87a667a05`): an R-tree-accelerated nested-loop join ([nested_loop_join.rs](../../../src/daft-local-execution/src/join/nested_loop_join.rs)) with a global path, a per-equality-key partitioned path, and a precomputed-bbox fast path; a WKB→bounding-box module ([mbr.rs](../../../src/daft-geo/src/mbr.rs)); and a geohash partition-pruning optimizer rule ([geohash_pruning.rs](../../../src/daft-logical-plan/src/optimization/rules/geohash_pruning.rs)). What is **missing** is everything *around* the engine: it is only reachable via a cross-join + `WHERE` rewrite, not the real join APIs; two predicates the accelerator references (`st_covers`/`st_covered_by`) do not exist; there is no distance predicate; there is no user-facing way to materialize the bbox index columns the fast path detects; and there is no end-to-end spatial-join test.

This sub-project **surfaces and completes** the existing engine. It adds: (1) spatial joins through the real APIs — Python `df.join(other, on=<predicate>)` and SQL `JOIN … ON ST_*(…)` — routed to the existing R-tree operator; (2) the missing predicates `st_covers`, `st_covered_by`, and a distance predicate `st_dwithin`; (3) a `df.with_spatial_bbox()` helper that materializes the fast-path index columns; (4) end-to-end correctness tests. **No new physical operator** is introduced — all paths reuse the existing `NestedLoopJoinOperator`.

## Goals

- `df1.join(df2, on=st_intersects(left["g"], right["g"]))` works in Python (inner join), routing to the existing R-tree operator.
- SQL `… JOIN … ON ST_Intersects(a.geom, b.geom)` works (today it errors with "Execution of non-equality join").
- `st_covers` / `st_covered_by` exist as real topological predicates (Rust + SQL + Python), activating the accelerator's currently-dead function-list entries.
- `st_dwithin(a, b, d)` exists (planar distance ≤ `d`), is R-tree-accelerated (query box expanded by `d`), and is usable as a join predicate.
- `df.with_spatial_bbox("geom")` adds `min_x/min_y/max_x/max_y` columns that the join fast-path auto-detects, giving users an explicit "build the spatial index" step.
- End-to-end tests prove spatial joins (engine + new APIs + new predicates + index helper) produce correct rows.

## Non-goals

- **Outer / semi / anti spatial joins.** Inner only. A spatial predicate join requested with `how != "inner"` raises a clear error. (The R-tree path is `nested_loop_inner_join` only; extending it is a future sub-project.)
- **General non-equi joins.** Only joins whose non-equi residual is a *spatial* predicate are routed to the operator; other non-equi predicates keep their current "not implemented" error.
- **Geodesic `st_dwithin`.** Distance is planar (Euclidean in the geometry's coordinate units), consistent with Sub-project A's planar defaults. A `use_spheroid` variant is a later add.
- **Persistent / on-disk spatial index.** `with_spatial_bbox` materializes columns in the DataFrame; the "index" is those columns plus the R-tree the operator builds per run. No serialized index artifact.
- **New physical join operator or changes to the geohash-pruning rule.** Reuse what exists.
- **Exposing `st_geohash_covers` as a user predicate.** It is an internal UDF used by the geohash-pruning rewrite; it stays internal.

## Decisions (resolved during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Python join API | **Overload `on=` to accept a boolean predicate** | SQL-like, single parameter. A predicate expression (root is a function/comparison, not a column ref) is routed as the join condition; column-name/list `on` keeps its equi-key meaning. |
| Join types | **Inner only** | The existing R-tree path is inner-only; outer/semi/anti are out of scope and error clearly. |
| Distance predicate | **Include `st_dwithin(a, b, d)`** (planar) | A very common spatial-join predicate ("within 500 m"). R-tree query box expands by `d`; exact filter refines. |
| Index API | **`df.with_spatial_bbox()` helper** | One call that adds the exact `min_x/min_y/max_x/max_y` columns the engine fast-path already detects. |
| Operator | **Reuse `NestedLoopJoinOperator`** | The engine is built; C is surface + predicates + tests. |
| Predicate engine | **geo `Relate` DE-9IM** for covers/covered_by; geo Euclidean distance for `st_dwithin` | Consistent with Sub-project A's predicate implementations. |

## Current state (baseline, from exploration)

- **Engine (exists):** `NestedLoopJoinOperator` ([nested_loop_join.rs](../../../src/daft-local-execution/src/join/nested_loop_join.rs)) — global R-tree (`build_rtree`), partitioned R-tree per equality key (`build_partitioned_rtrees`), bbox fast-path (detects columns `min_x/min_y/max_x/max_y` or `bbox_min_x/…`), parallel rayon probe. Its `SPATIAL_FNS` list (line 77) **already names** `st_covers`/`st_covered_by` though those functions don't exist.
- **MBR (exists):** `wkb_to_mbr(&[u8]) -> Option<[f64;4]>`, `Mbr`, `mbrs_intersect` in [mbr.rs](../../../src/daft-geo/src/mbr.rs) (with inline Rust tests).
- **Logical join (supports predicates):** `JoinPredicate` wraps an arbitrary boolean `ExprRef`; `split_eq_preds()` separates equi-keys from a `remaining` predicate ([ops/join.rs](../../../src/daft-logical-plan/src/ops/join.rs)). The logical `builder.join(on: Option<ExprRef>, …)` resolves join-side markers via `resolve_join_on` ([builder/mod.rs:716](../../../src/daft-logical-plan/src/builder/mod.rs)).
- **Translate (the gap):** the `Filter`-over-cross-join spatial rewrite ([translate.rs](../../../src/daft-local-plan/src/translate.rs) ~line 153) routes spatial `WHERE` to `nested_loop_join`; but a `LogicalPlan::Join` with a non-equi `remaining_on` (~line 653) returns `Err("Execution of non-equality join")`.
- **Python `.join()` (the gap):** maps `on` → `left_on = right_on = on` (equi-keys); the PyO3 binding `PyLogicalPlanBuilder::join` ([builder/mod.rs:1497](../../../src/daft-logical-plan/src/builder/mod.rs)) takes `left_on: Vec<PyExpr>, right_on: Vec<PyExpr>` only — no predicate path.
- **SQL (works at parse level):** `JOIN … ON <expr>` already plans to a `LogicalPlan::Join` with the predicate in `on` ([planner.rs](../../../src/daft-sql/src/planner.rs) ~line 952); it fails only downstream in translate.
- **Predicates (exist):** `st_intersects/contains/within/touches/crosses/overlaps/disjoint/equals` in `src/daft-geo/src/`; binary predicates go through `binary_geom_to_bool` + `relate_pred` (DE-9IM). `st_covers/st_covered_by/st_dwithin` do **not** exist.
- **Tests:** [test_spatial.py](../../../tests/expressions/test_spatial.py) (Sub-project A) and [test_spatial_geohash_pruning.py](../../../tests/expressions/test_spatial_geohash_pruning.py) (point-in-polygon via `.where()`, geohash pruning). **No two-frame spatial-join test exists** (`.join(` is unused in spatial tests).

## Architecture

### Component 1 — Spatial join API surface

**Python** ([dataframe.py](../../../daft/dataframe/dataframe.py) `.join`): detect when `on` is a single boolean predicate `Expression` (its root expression is not a plain column reference — e.g. a `ScalarFn` spatial predicate, a comparison, or a boolean op) and route it as a join *predicate* rather than equi-keys. Existing behavior for column-name / `Column` / list-of-keys `on` is unchanged. Guards: a predicate `on` with `left_on`/`right_on` set → error; `how != "inner"` with a predicate `on` → clear error ("spatial/predicate joins support inner only"). The predicate references columns from both frames (e.g. `df1["g"]`, `df2["g"]`); the logical layer's `resolve_join_on` already tags each column with its join side.

**PyO3 binding** ([builder/mod.rs:1497](../../../src/daft-logical-plan/src/builder/mod.rs)): add an optional `on_predicate: Option<PyExpr>` parameter. When set, call the underlying `self.builder.join(Some(pred.into()), vec![], join_type, join_strategy, options)` (the predicate path) instead of constructing equi-key conjunctions from `left_on`/`right_on`. When unset, current behavior is unchanged.

**SQL:** no change — `JOIN … ON ST_*` already produces the correct logical node.

**translate.rs** ([translate.rs](../../../src/daft-local-plan/src/translate.rs), the `LogicalPlan::Join` arm): when the join's non-equi residual (`on.split_eq_preds().remaining`) is a **spatial** predicate (its expression tree contains a `SPATIAL_FNS` call), emit `LocalPhysicalPlan::nested_loop_join` — selecting build side and extracting any equality partition key with the *same* logic the existing `Filter`-over-join rewrite uses (factor that logic into a shared helper so both call sites agree). Equi-keys present alongside the spatial predicate become the partition key (driving the partitioned R-tree path). A non-equi residual that is **not** spatial keeps the existing `not_implemented` error.

### Component 2 — Missing predicates

**st_covers / st_covered_by** (`src/daft-geo/src/st_covers.rs`, `st_covered_by.rs`): topological, via geo `Relate` DE-9IM. Extend `RelatePred` ([relate.rs](../../../src/daft-geo/src/relate.rs)) with `Covers` / `CoveredBy` (DE-9IM: covers = no exterior-of-A intersects interior/boundary-of-B, i.e. matrix pattern `T*****FF*` for covers and its transpose for covered_by; confirm via geo's `IntersectionMatrix` predicates). Each new file mirrors `st_contains.rs` (binary geom→bool through `binary_geom_to_bool`). Register in [lib.rs](../../../src/daft-geo/src/lib.rs), SQL ([spatial.rs](../../../src/daft-sql/src/modules/spatial.rs)), Python ([spatial.py](../../../daft/functions/spatial.py)) + `__all__`. These immediately activate the accelerator's existing `st_covers`/`st_covered_by` list entries.

**st_dwithin** (`src/daft-geo/src/st_dwithin.rs`): `st_dwithin(a, b, d) -> Boolean`, true when planar distance(a, b) ≤ `d`. `d` is a scalar passed as a **trailing positional `FunctionArgs` arg** (Sub-project A's param pattern; read via `utils::read_f64_arg`). Exact distance via geo's `Euclidean`/`EuclideanDistance` on parsed geometries. Register Rust + SQL (named arg) + Python (`st_dwithin(a, b, d)`).

**Accelerator integration for st_dwithin** ([nested_loop_join.rs](../../../src/daft-local-execution/src/join/nested_loop_join.rs)): add `"st_dwithin"` to `SPATIAL_FNS`. The column-index extractor (`extract_from_expr`) must additionally capture the distance literal `d` (3rd arg) for `st_dwithin`. In the R-tree probe, when the predicate is `st_dwithin`, expand each probe geometry's query `AABB` by `d` on all sides before `locate_in_envelope_intersecting` (so all geometries whose MBR is within `d` become candidates); the exact `nested_loop_inner_join_indexed` filter then refines to true distance ≤ `d`. For topological predicates the query box is unchanged (current behavior). This is the only nontrivial engine edit.

### Component 3 — Index helper

**`st_bbox`** (`src/daft-geo/src/st_bbox.rs`): scalar UDF `st_bbox(geom) -> Struct{min_x: f64, min_y: f64, max_x: f64, max_y: f64}`, one `wkb_to_mbr` parse per row; null / empty / non-finite geometry → null struct (or null fields). Register Rust + Python (used by the helper; SQL registration optional).

**`df.with_spatial_bbox(geom_col, *, prefix=None)`** ([dataframe.py](../../../daft/dataframe/dataframe.py) or a small spatial helper): adds top-level Float64 columns `min_x/min_y/max_x/max_y` (the exact names the fast-path detects; `prefix` → `{prefix}min_x` etc. for the `bbox_*` variant) by projecting `st_bbox(col(geom_col))` and unnesting its fields. The join fast-path then uses these instead of re-parsing WKB (10–100× faster MBR extraction per its own comment).

### Testing

New `tests/expressions/test_spatial_join.py` (native runner):
- **Join API:** `df1.join(df2, on=st_intersects(df1["g"], df2["g"]))` → rows equal a brute-force oracle (cross-join + `st_intersects` filter). Same assertion via SQL `JOIN … ON ST_Intersects(...)`.
- **New predicates as join conditions:** `st_covers`, `st_covered_by`, `st_dwithin(…, d)` each in a join → correct rows vs oracle.
- **Index helper equivalence:** `df.with_spatial_bbox("g")` adds the 4 columns; a join run with the bbox columns present yields **identical** rows to the same join without them (fast-path == slow-path).
- **Partitioned path:** join with an equality key *and* a spatial predicate exercises the per-key partitioned R-tree; rows match oracle.
- **Inner-only guard:** `df1.join(df2, on=st_intersects(...), how="left")` raises a clear error.
- **Rust units:** `st_covers`/`st_covered_by` DE-9IM truth tables; `st_dwithin` distance boundary (just inside / just outside `d`); `st_bbox` MBR for point/line/polygon/empty; the dwithin query-box expansion produces the right candidate set.

All Python geometry built with `st_point` / `st_geomfromtext`-fed constructors that return `Geometry`; predicate truth verified independently where practical.

## Error handling

- Predicate `on` combined with `left_on`/`right_on`, or with `how != "inner"` → `ValueError` with a clear message (Python), before planning.
- Non-spatial non-equi join residual → existing `not_implemented` error preserved.
- `st_dwithin` with a non-finite / negative `d` → treat as no-match per row (or error at parse); malformed geometry rows → null (no match), never panic (catch_unwind consistent with Sub-project A where geo ops can panic).
- `with_spatial_bbox` on a non-geometry / non-WKB column → error from `st_bbox` (clear message); null geometries → null bbox columns (fast-path skips non-finite).

## Risks / open questions

- **Predicate-vs-key detection in Python `.join()`.** The rule "root expression is not a plain column reference ⇒ predicate" must not misclassify a legitimate computed equi-key. Mitigation: only a single `Expression` (not a list) whose root is a boolean predicate (spatial `ScalarFn`, comparison, or boolean op) is treated as a predicate; document that computed equi-keys must use `left_on`/`right_on`.
- **`resolve_join_on` from the Python predicate path.** Confirm that a predicate built from `df1["g"]`/`df2["g"]` resolves join-side markers correctly when passed through the new `on_predicate` binding (the SQL path already does this; the Python column objects must carry enough provenance, or the resolver must disambiguate by schema).
- **DE-9IM patterns for covers/covered_by.** Confirm geo's `Relate`/`IntersectionMatrix` exposes covers/coveredby (or hand-write the matrix pattern). geo distinguishes contains (interior) from covers (interior+boundary); pick the standard OGC definitions.
- **st_dwithin candidate completeness.** Expanding the *MBR* by `d` is a sound over-approximation for Euclidean distance between geometries (a necessary condition: if true distance ≤ d then MBRs are within d), so no false negatives; the exact filter removes false positives. Confirm the expansion uses `d` in the same coordinate units as the geometries.
- **Shared build-side/partition-key helper.** Factoring the existing `Filter`-over-join routing logic so the new `Join`-with-predicate path reuses it must not change the existing rewrite's behavior — verify the existing geohash-pruning and spatial `.where()` tests still pass.
