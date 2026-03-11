# Source Pipeline Refactor: Eliminate `ScanTaskLike` Trait

**Branch**: `rchowell/source-cleanup`
**Status**: Phase 1 complete (ScanTaskLike simplified to 7 methods). This document covers the next step: full elimination.

## Problem

`ScanTaskLike` is a trait in `common-scan-info` that exists solely because of a dependency constraint:

```
common-scan-info  (defines ScanOperator, ScanState, Sharder, ScanTaskLike)
       ^
       |
   daft-scan      (defines ScanTask, implements ScanTaskLike for ScanTask)
       |
       v
daft-logical-plan (uses ScanOperator, ScanState — needs common-scan-info)
```

`common-scan-info` cannot reference `ScanTask` (defined in `daft-scan`), so it defines a `ScanTaskLike` trait. Every consumer that needs the concrete `ScanTask` must downcast from `Arc<dyn ScanTaskLike>` at translation boundaries — a runtime cost and indirection that serves no purpose since `ScanTask` is the only real implementor.

## Solution

Reverse the dependency direction between `daft-scan` and `daft-logical-plan`.

Currently `daft-scan` depends on `daft-logical-plan` for scan builder code (`builder.rs`) and one Python binding (`logical_plan_table_scan`). This is a narrow dependency that can be relocated. Once removed, `daft-logical-plan` can depend on `daft-scan`, which allows `common-scan-info` to also depend on `daft-scan`, replacing `ScanTaskLikeRef` with `Arc<ScanTask>` everywhere.

### Future direction (Option B)

The architecturally cleaner long-term goal is to move scan task materialization out of `daft-logical-plan` entirely (Option B from design discussions). In that world, `ScanState::Tasks` and all concrete `ScanTask` usage lives only in physical plan layers (`daft-local-plan`, `daft-distributed`). The logical plan only ever holds `ScanState::Operator`. This document's approach (Option A) is a stepping stone that unblocks Option B by establishing the correct dependency direction.

---

## Plan

### Phase A: Break `daft-scan -> daft-logical-plan` dependency

The `daft-scan` crate depends on `daft-logical-plan` for exactly two things:

1. **`src/daft-scan/src/builder.rs`** — Scan builder structs (`ParquetScanBuilder`, `CsvScanBuilder`, `JsonScanBuilder`) and functions (`delta_scan`, `iceberg_scan`) that create a `GlobScanOperator` then wrap it in `LogicalPlanBuilder::table_scan()`.

2. **`src/daft-scan/src/python.rs`** — The `logical_plan_table_scan` pyfunction (line 653) which calls `LogicalPlanBuilder::table_scan()`.

Both are "bridge" code that combines scan operator construction with logical plan building. They naturally belong in `daft-logical-plan`.

#### Step A1: Move `builder.rs` to `daft-logical-plan`

1. Copy `src/daft-scan/src/builder.rs` to `src/daft-logical-plan/src/scan_builder.rs`.

2. Update imports in the new file:
   - `use crate::{LogicalPlanBuilder, builder::IntoGlobPath};` (was `use daft_logical_plan::...`)
   - `use daft_scan::{glob::GlobScanOperator, storage_config::StorageConfig};` (was `use crate::...`)
   - `#[cfg(feature = "python")] use daft_scan::python::pylib::ScanOperatorHandle;` (same)

3. Register the new module in `src/daft-logical-plan/src/lib.rs`:
   ```rust
   pub mod scan_builder;
   ```

4. Delete `src/daft-scan/src/builder.rs` and remove `pub mod builder;` from `src/daft-scan/src/lib.rs`.

#### Step A2: Move `logical_plan_table_scan` to `daft-logical-plan`

1. Move the pyfunction from `src/daft-scan/src/python.rs` (lines 652-657):
   ```rust
   #[pyfunction]
   pub fn logical_plan_table_scan(
       scan_operator: ScanOperatorHandle,
   ) -> PyResult<PyLogicalPlanBuilder> {
       Ok(LogicalPlanBuilder::table_scan(scan_operator.into(), None)?.into())
   }
   ```
   into `src/daft-logical-plan/src/lib.rs` (or a python submodule). This function needs `ScanOperatorHandle` from `daft-scan`, `LogicalPlanBuilder` and `PyLogicalPlanBuilder` from `daft-logical-plan`.

2. Register it in `daft-logical-plan`'s `register_modules`:
   ```rust
   parent.add_function(wrap_pyfunction!(logical_plan_table_scan, parent)?)?;
   ```

3. Remove it from `daft-scan`'s `register_modules` (line 728 of `python.rs`).

4. Remove `use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};` from `daft-scan/src/python.rs` (line 81). After this, no `daft_logical_plan` imports should remain in `daft-scan`.

#### Step A3: Swap the dependency

1. **Remove** from `src/daft-scan/Cargo.toml`:
   ```toml
   # Remove these lines:
   daft-logical-plan = {path = "../daft-logical-plan", default-features = false}
   # And in [features] python = [...]:
   "daft-logical-plan/python",
   ```

2. **Add** to `src/daft-logical-plan/Cargo.toml`:
   ```toml
   # In [dependencies]:
   daft-scan = {path = "../daft-scan", default-features = false}
   # And in [features] python = [...]:
   "daft-scan/python",
   ```

3. **Verify** no remaining `daft_logical_plan` references exist in `daft-scan`:
   ```bash
   rg 'daft.logical.plan' src/daft-scan/
   ```

#### Step A4: Update downstream consumers

`daft-sql` imports scan builders from `daft-scan::builder`. Update to `daft-logical_plan::scan_builder`:

| File | Old import | New import |
|------|-----------|------------|
| `src/daft-sql/src/table_provider/read_parquet.rs` | `daft_scan::builder::ParquetScanBuilder` | `daft_logical_plan::scan_builder::ParquetScanBuilder` |
| `src/daft-sql/src/table_provider/read_csv.rs` | `daft_scan::builder::CsvScanBuilder` | `daft_logical_plan::scan_builder::CsvScanBuilder` |
| `src/daft-sql/src/table_provider/read_json.rs` | `daft_scan::builder::JsonScanBuilder` | `daft_logical_plan::scan_builder::JsonScanBuilder` |
| `src/daft-sql/src/table_provider/read_deltalake.rs` | `daft_scan::builder::delta_scan` | `daft_logical_plan::scan_builder::delta_scan` |
| `src/daft-sql/src/table_provider/read_iceberg.rs` | `daft_scan::builder::iceberg_scan` | `daft_logical_plan::scan_builder::iceberg_scan` |

Check whether `daft-sql` already depends on `daft-logical-plan` (it likely does). If not, add it.

#### Step A5: Checkpoint — compile and test

At this point the dependency is reversed. The crate graph should be:

```
common-scan-info  <--  daft-scan  <--  daft-logical-plan
```

Run:
```bash
cargo check --workspace
cargo test -p common-scan-info -p daft-scan -p daft-logical-plan -p daft-sql
```

No behavior should change — this is a pure code relocation.

---

### Phase B: Eliminate `ScanTaskLike` trait

With `daft-logical-plan -> daft-scan` established, `common-scan-info` can now also depend on `daft-scan` (since `common-scan-info <- daft-scan <- daft-logical-plan` has no cycle). This lets us replace `ScanTaskLikeRef` with `Arc<ScanTask>` everywhere.

**Important**: Check for a potential cycle through `common-scan-info -> daft-scan -> common-scan-info`. `daft-scan` already depends on `common-scan-info`. Adding the reverse dependency would create a cycle. Instead, we move `ScanTask` into `common-scan-info`, OR we restructure `ScanState` and `ScanOperator::to_scan_tasks` so they don't need `ScanTaskLikeRef`.

#### Approach B1: Move `ScanTask` down to `common-scan-info`

This avoids cycles by putting `ScanTask` where it's needed. However, `ScanTask` depends on types from `daft-scan` (`DataSource`, `storage_config::StorageConfig`) and heavy crates (`daft-parquet` for `DaftParquetMetadata`, `daft-stats`). Moving all of these is impractical.

#### Approach B2: Invert — move `ScanOperator` and `ScanState` up to `daft-scan` (Recommended)

Instead of moving `ScanTask` down, move the types that need it UP from `common-scan-info` to `daft-scan`:

1. **Move to `daft-scan`**: `ScanOperator`, `ScanOperatorRef`, `ScanState`, `PhysicalScanInfo`, `Sharder`
2. **Keep in `common-scan-info`**: `Pushdowns`, `PartitionField`, `PartitionTransform`, `SupportsPushdownFilters` — these are pure data types with no `ScanTask` dependency.
3. **Delete**: `ScanTaskLike` trait, `ScanTaskLikeRef` type alias, `scan_task.rs` in `common-scan-info`.
4. **Delete**: `common-scan-info/src/test/mod.rs` `DummyScanTask` (replace with real `ScanTask` in tests or move tests to `daft-scan`).

After this move:
- `ScanOperator::to_scan_tasks()` returns `DaftResult<Vec<Arc<ScanTask>>>` directly
- `ScanState::Tasks` holds `Arc<Vec<Arc<ScanTask>>>`
- `Sharder` methods take/return `Vec<Arc<ScanTask>>`
- No trait, no downcasts

**Dependency graph after B2**:
```
common-scan-info  (Pushdowns, PartitionField, etc.)
       ^
       |
   daft-scan      (ScanTask, ScanOperator, ScanState, Sharder, GlobScanOperator, etc.)
       ^
       |
daft-logical-plan (uses ScanOperator, ScanState from daft-scan)
```

This means `daft-logical-plan` now imports `ScanOperator`, `ScanState`, etc. from `daft_scan` instead of `common_scan_info`.

#### Step B1: Move types from `common-scan-info` to `daft-scan`

Move these files/items from `common-scan-info` to `daft-scan`:

| Item | Source | Destination |
|------|--------|-------------|
| `ScanOperator` trait, `ScanOperatorRef` | `common-scan-info/src/scan_operator.rs` | `daft-scan/src/scan_operator.rs` |
| `ScanState`, `PhysicalScanInfo` | `common-scan-info/src/lib.rs` | `daft-scan/src/scan_state.rs` (new file) |
| `Sharder`, `ShardingStrategy` | `common-scan-info/src/sharder.rs` | `daft-scan/src/sharder.rs` |

Update `ScanOperator::to_scan_tasks` signature:
```rust
// Before (in common-scan-info)
fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>>;

// After (in daft-scan)
fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<Arc<ScanTask>>>;
```

Update `ScanState::Tasks`:
```rust
// Before
Tasks(Arc<Vec<ScanTaskLikeRef>>),
// After
Tasks(Arc<Vec<Arc<ScanTask>>>),
```

Update `Sharder` methods to use `Arc<ScanTask>` instead of `ScanTaskLikeRef`. The `get_file_paths()` call in sharder becomes direct field access on `ScanTask`.

#### Step B2: Delete `ScanTaskLike`

1. Delete `common-scan-info/src/scan_task.rs`
2. Remove `pub use scan_task::{ScanTaskLike, ScanTaskLikeRef};` from `common-scan-info/src/lib.rs`
3. Remove `mod scan_task;` from `common-scan-info/src/lib.rs`
4. Remove `#[typetag::serde]` from `impl ScanTaskLike for ScanTask` in `daft-scan/src/lib.rs` (delete the entire impl block, lines ~454-488)
5. Remove `typetag` dependency from `common-scan-info/Cargo.toml`

#### Step B3: Remove downcasts from translation layers

**`src/daft-local-plan/src/translate.rs`** (around line 52-75):
```rust
// Before: extract trait objects then downcast
let scan_task_likes = match &info.scan_state { ... };
let scan_tasks: Vec<ScanTaskRef> = scan_task_likes
    .into_iter()
    .map(|task| task.as_any_arc().downcast::<ScanTask>().map_err(...))
    .collect::<Result<Vec<_>, _>>()?;

// After: direct extraction, no downcast needed
let scan_tasks: Vec<Arc<ScanTask>> = match &info.scan_state {
    ScanState::Operator(scan_op) => scan_op.0.to_scan_tasks(info.pushdowns.clone())?,
    ScanState::Tasks(scan_tasks) => (**scan_tasks).clone(),
};
```

**`src/daft-distributed/src/pipeline_node/translate.rs`** (around line 116-140):
Same pattern — remove the `.as_any_arc().downcast::<ScanTask>()` dance.

#### Step B4: Update `common-scan-info` re-exports

After moving `ScanOperator`, `ScanState`, etc. to `daft-scan`, many crates import these from `common_scan_info`. Update all imports:

```bash
# Find all imports to update
rg 'common_scan_info::.*(ScanOperator|ScanState|PhysicalScanInfo|Sharder)' --type rust
```

All such imports change to `daft_scan::...`. Affected crates (at minimum):
- `daft-logical-plan`
- `daft-local-plan`
- `daft-distributed`
- `daft-local-execution`

**Alternative**: To minimize import churn, `common-scan-info` could re-export from `daft-scan`:
```rust
// common-scan-info/src/lib.rs
pub use daft_scan::{ScanOperator, ScanOperatorRef, ScanState, PhysicalScanInfo};
```
But this creates a `common-scan-info -> daft-scan` dependency (which is fine since `daft-scan -> common-scan-info` already exists... wait, that IS a cycle). So re-exporting is NOT possible. All imports must be updated directly.

#### Step B5: Update test code

`common-scan-info/src/test/mod.rs` has `DummyScanTask` implementing `ScanTaskLike` and `DummyScanOperator` implementing `ScanOperator`. After the move:

- `DummyScanOperator` must move to `daft-scan` (since `ScanOperator` is now in `daft-scan`)
- `DummyScanTask` is deleted (no trait to implement)
- `DummyScanOperator::to_scan_tasks` returns `Vec<Arc<ScanTask>>` — it needs to construct real `ScanTask` values. Create a test helper in `daft-scan` for building minimal `ScanTask` instances.

The sharder tests that use `DummyScanOperator` also move to `daft-scan`.

#### Step B6: Checkpoint — compile and test

```bash
cargo check --workspace
cargo test -p common-scan-info -p daft-scan -p daft-logical-plan -p daft-local-plan -p daft-distributed
```

---

## Testing

### Compilation

The refactor is primarily type-level. If it compiles, most behavior is preserved. But verify:

```bash
# Full workspace check (catches all import/dependency issues)
cargo check --workspace

# Full workspace check with python feature
cargo check --workspace --features python
```

### Unit tests

```bash
# Core crates affected by the refactor
cargo test -p common-scan-info
cargo test -p daft-scan
cargo test -p daft-logical-plan
cargo test -p daft-local-plan
cargo test -p daft-distributed
cargo test -p daft-sql

# Run all unit tests
cargo test --workspace --lib
```

### Integration tests (Python)

These exercise the full pipeline from Python through scan operators to execution:

```bash
# Activate venv and rebuild Rust
source .venv/bin/activate
make build

# Core scan tests — these exercise ScanOperator -> ScanTask -> execution
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/dataframe/test_scan.py"
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/test_parquet.py"
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/test_csv.py"
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/test_json.py"

# SQL scan tests — exercise the moved builders
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/sql/test_table_functions.py"

# Delta/Iceberg scans (Python feature, exercises moved builder functions)
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/"
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/iceberg/"

# Python scan operator bridge (exercises PythonScanOperatorBridge -> ScanTask)
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/test_scan_operator.py"

# Full test suite (takes longer but catches edge cases)
DAFT_RUNNER=native make test
```

### Serialization

`ScanTaskLike` uses `#[typetag::serde]` for polymorphic serialization. After elimination, `ScanTask` is serialized directly (it already derives `Serialize`/`Deserialize`). Verify:

```bash
# Tests that exercise plan serialization
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/plan_serialization/"

# Or if there are specific serde tests:
cargo test -p daft-scan -- serde
cargo test -p common-scan-info -- serde
```

### Distributed (Ray) tests

The distributed translation layer is affected. If Ray is available:

```bash
DAFT_RUNNER=ray make test EXTRA_ARGS="-v tests/dataframe/test_scan.py"
DAFT_RUNNER=ray make test EXTRA_ARGS="-v tests/io/test_parquet.py"
```

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Import cycle missed during Phase A | Low | `cargo check --workspace` catches all cycles at compile time |
| Python module registration order matters | Low | Both `daft_scan` and `daft_logical_plan` register independently; moving one pyfunction between them is safe |
| `typetag` removal breaks deserialization of persisted plans | Medium | Check if any code persists `ScanState::Tasks` to disk. `ScanState::Operator` already panics on serde, so `Tasks` may be the only serialized variant. If plans are persisted, this is a breaking change. |
| `daft-sql` has transitive dependency issues | Low | `daft-sql` already depends on both `daft-scan` and `daft-logical-plan`; just update imports |
| Moving `ScanOperator` to `daft-scan` causes widespread import changes | Medium | Mechanical but tedious. Use `sed`/`fastmod` for bulk replacement. |

## Files Changed (Summary)

### Phase A (dependency reversal)
| File | Action |
|------|--------|
| `src/daft-scan/src/builder.rs` | Delete |
| `src/daft-scan/src/lib.rs` | Remove `pub mod builder;` |
| `src/daft-scan/src/python.rs` | Remove `logical_plan_table_scan`, remove `daft_logical_plan` imports |
| `src/daft-scan/Cargo.toml` | Remove `daft-logical-plan` dep |
| `src/daft-logical-plan/src/scan_builder.rs` | Create (moved from daft-scan) |
| `src/daft-logical-plan/src/lib.rs` | Add `pub mod scan_builder;`, add `logical_plan_table_scan` to python registration |
| `src/daft-logical-plan/Cargo.toml` | Add `daft-scan` dep |
| `src/daft-sql/src/table_provider/read_*.rs` | Update imports |

### Phase B (trait elimination)
| File | Action |
|------|--------|
| `src/common/scan-info/src/scan_task.rs` | Delete |
| `src/common/scan-info/src/scan_operator.rs` | Delete (moved to daft-scan) |
| `src/common/scan-info/src/sharder.rs` | Delete (moved to daft-scan) |
| `src/common/scan-info/src/lib.rs` | Remove `ScanState`, `PhysicalScanInfo`, `ScanOperator`, `Sharder` exports; remove moved modules |
| `src/common/scan-info/src/test/mod.rs` | Rewrite (no more DummyScanTask; move DummyScanOperator to daft-scan) |
| `src/common/scan-info/Cargo.toml` | Remove `typetag` dep |
| `src/daft-scan/src/lib.rs` | Delete `impl ScanTaskLike for ScanTask`; add moved modules |
| `src/daft-scan/src/scan_operator.rs` | Create (moved from common-scan-info) |
| `src/daft-scan/src/scan_state.rs` | Create (ScanState, PhysicalScanInfo from common-scan-info) |
| `src/daft-scan/src/sharder.rs` | Create (moved from common-scan-info) |
| `src/daft-scan/src/glob.rs` | Update `to_scan_tasks` return type |
| `src/daft-scan/src/anonymous.rs` | Update `to_scan_tasks` return type |
| `src/daft-scan/src/python.rs` | Update `to_scan_tasks` return type in `PythonScanOperatorBridge` |
| `src/daft-local-plan/src/translate.rs` | Remove downcast code |
| `src/daft-distributed/src/pipeline_node/translate.rs` | Remove downcast code |
| `src/daft-logical-plan/src/ops/source.rs` | Update `ScanState`/`ScanTaskLikeRef` references |
| `src/daft-logical-plan/src/optimization/rules/shard_scans.rs` | Update imports |
| All crates importing from `common_scan_info` | Update imports for moved types |
