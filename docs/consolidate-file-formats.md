# Consolidate common-file-formats into daft-scan

> **Goal**: Move `FileFormatConfig`, all `*SourceConfig` structs, `DatabaseSourceConfig`,
> and `PyFileFormatConfig` from `common-file-formats` into `daft-scan`. Slim
> `common-file-formats` down to just `FileFormat` + `WriteMode` — the only types
> genuinely shared across many crates.

**Branch**: `rchowell/source-cleanup-fileformat` (builds on SourceConfig work)

---

## Motivation

After introducing `SourceConfig` in `daft-scan`, the `common-file-formats` crate is a
misplaced grab-bag. The read-config types (`FileFormatConfig`, `ParquetSourceConfig`, etc.)
are only consumed by `daft-scan` (which owns `SourceConfig`) and `daft-local-execution`
(which reads scan tasks). They aren't "common" — they're scan domain types.

`FileFormat` and `WriteMode` are the only truly shared types (used by `daft-io`,
`daft-writers`, `daft-distributed`, `daft-logical-plan`, `daft-local-execution`).

---

## What moves where

| Type | From | To | Reason |
|------|------|----|--------|
| `FileFormatConfig` | common-file-formats | daft-scan | Only used by scan pipeline |
| `ParquetSourceConfig` | common-file-formats | daft-scan | Only used by scan pipeline |
| `CsvSourceConfig` | common-file-formats | daft-scan | Only used by scan pipeline |
| `JsonSourceConfig` | common-file-formats | daft-scan | Only used by scan pipeline |
| `WarcSourceConfig` | common-file-formats | daft-scan | Only used by scan pipeline |
| `TextSourceConfig` | common-file-formats | daft-scan | Only used by scan pipeline |
| `DatabaseSourceConfig` | common-file-formats | daft-scan | Already used only via SourceConfig |
| `PyFileFormatConfig` | common-file-formats | daft-scan | Wraps FileFormatConfig |
| `FileFormat` | common-file-formats | **stays** | Used by 6+ crates (io, writers, etc.) |
| `WriteMode` | common-file-formats | **stays** | Used by logical-plan, local-execution |

---

## Dependency analysis

**No new dependencies needed.** `daft-scan` already has every dependency that
`common-file-formats` uses for the config types: `daft-schema`, `common-py-serde`,
`pyo3`, `serde`, `serde_json`.

**No circular dependencies.** `common-file-formats` has no dependency on `daft-scan`.
Crates that currently import config types from `common-file-formats` already depend on
`daft-scan`.

---

## Steps

### 1. Move file_format_config.rs into daft-scan

**Move** `src/common/file-formats/src/file_format_config.rs` →
`src/daft-scan/src/file_format_config.rs`

Update `src/daft-scan/src/lib.rs`:
```rust
mod file_format_config;
pub use file_format_config::*;
```

Remove `file_format_config` module from `src/common/file-formats/src/lib.rs`.

Remove the re-exports from `common-file-formats/src/lib.rs`:
```diff
-mod file_format_config;
-pub use file_format_config::{
-    CsvSourceConfig, FileFormatConfig, JsonSourceConfig, ParquetSourceConfig, TextSourceConfig,
-    WarcSourceConfig,
-};
-#[cfg(feature = "python")]
-pub use file_format_config::DatabaseSourceConfig;
```

Remove the `From<&FileFormatConfig> for FileFormat` impl from `common-file-formats/src/lib.rs`
and move it into `daft-scan/src/file_format_config.rs` (it can reference `FileFormat` since
`daft-scan` already depends on `common-file-formats` for `FileFormat`).

### 2. Move PyFileFormatConfig into daft-scan

**Move** `src/common/file-formats/src/python.rs` →
`src/daft-scan/src/python_file_format.rs` (or merge into existing `daft-scan/src/python.rs`)

The `PyFileFormatConfig` wrapper, its `From` impls, and the `register_modules` for
`FileFormat`/`WriteMode` need splitting:
- `PyFileFormatConfig` + `From` impls → move to `daft-scan`
- `FileFormat`/`WriteMode` pymethods + `register_modules` → stay in `common-file-formats`

Update `common-file-formats/src/python.rs` to keep only:
```rust
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_class::<FileFormat>()?;
    parent.add_class::<WriteMode>()?;
    Ok(())
}
```

### 3. Update common-file-formats/src/lib.rs

After the move, `common-file-formats` contains only:
```
src/
  file_format.rs    — FileFormat enum + WriteMode
  lib.rs            — re-exports FileFormat, WriteMode
  python.rs         — pymethods for FileFormat/WriteMode + register_modules
```

Remove dependencies that are no longer needed from `common-file-formats/Cargo.toml`:
- `daft-schema` (was only needed for `ParquetSourceConfig`)

### 4. Update imports across the codebase

Crates that imported config types from `common_file_formats` now import from `daft_scan`:

| Crate | Files | Change |
|-------|-------|--------|
| daft-local-execution | sources/scan_task.rs, sources/scan_task_reader.rs | `common_file_formats::*` → `daft_scan::*` |
| daft-logical-plan | scan_builder.rs, lib.rs | `common_file_formats::{FileFormatConfig, ...}` → `daft_scan::{FileFormatConfig, ...}` |
| daft-scan (internal) | glob.rs, anonymous.rs, scan_task_iters/, lib.rs | `common_file_formats::*` → `crate::*` |

Crates that only use `FileFormat`/`WriteMode` stay unchanged:
- daft-io, daft-writers, daft-distributed (sink.rs), daft-logical-plan (builder/, sink_info.rs),
  daft-local-execution (pipeline.rs, sinks/)

### 5. Update pyclass registration

**`daft-logical-plan/src/lib.rs` register_modules:**
```diff
-use common_file_formats::{
-    CsvSourceConfig, DatabaseSourceConfig, JsonSourceConfig, ParquetSourceConfig,
-    TextSourceConfig, WarcSourceConfig, python::PyFileFormatConfig,
-};
+use daft_scan::{
+    CsvSourceConfig, DatabaseSourceConfig, JsonSourceConfig, ParquetSourceConfig,
+    TextSourceConfig, WarcSourceConfig, python::PyFileFormatConfig,
+};
```

The pyclass registrations stay in `daft-logical-plan` — they're just importing from a
different path.

### 6. Remove common-file-formats dependency where no longer needed

After the move, these crates can drop `common-file-formats` from Cargo.toml:
- `daft-local-execution` — already depends on `daft-scan` which now re-exports everything
- `daft-logical-plan` — already depends on `daft-scan`

Keep `common-file-formats` in:
- `daft-io` (uses `FileFormat`)
- `daft-writers` (uses `FileFormat`)
- `daft-distributed` (uses `FileFormat`)
- `daft-scan` (uses `FileFormat` from it)

### 7. Run cargo machete + tests

- `cargo machete` to verify no unused deps remain
- `make build` to verify Python bindings
- `cargo test -p daft-scan -p daft-local-execution -p daft-logical-plan`
- `DAFT_RUNNER=native make test EXTRA_ARGS="-x tests/dataframe/"`

---

## Files changed (manifest)

| # | File | Change |
|---|------|--------|
| 1 | `src/daft-scan/src/file_format_config.rs` | **New** — moved from common-file-formats |
| 2 | `src/daft-scan/src/lib.rs` | Add module + re-exports |
| 3 | `src/daft-scan/src/python.rs` | Add PyFileFormatConfig |
| 4 | `src/common/file-formats/src/file_format_config.rs` | **Deleted** |
| 5 | `src/common/file-formats/src/python.rs` | Remove PyFileFormatConfig, keep FileFormat/WriteMode |
| 6 | `src/common/file-formats/src/lib.rs` | Remove config re-exports, keep FileFormat/WriteMode |
| 7 | `src/common/file-formats/Cargo.toml` | Remove daft-schema dependency |
| 8 | `src/daft-local-execution/src/sources/scan_task.rs` | Import from daft_scan |
| 9 | `src/daft-local-execution/src/sources/scan_task_reader.rs` | Import from daft_scan |
| 10 | `src/daft-local-execution/Cargo.toml` | Remove common-file-formats dep |
| 11 | `src/daft-logical-plan/src/lib.rs` | Import from daft_scan |
| 12 | `src/daft-logical-plan/src/scan_builder.rs` | Import from daft_scan |
| 13 | `src/daft-logical-plan/Cargo.toml` | Remove common-file-formats dep |
| 14 | `src/daft-scan/src/glob.rs` | `common_file_formats::` → `crate::` |
| 15 | `src/daft-scan/src/anonymous.rs` | `common_file_formats::` → `crate::` |
| 16 | `src/daft-scan/src/scan_task_iters/mod.rs` | `common_file_formats::` → `crate::` |
| 17 | `src/daft-scan/src/scan_task_iters/split_jsonl/mod.rs` | `common_file_formats::` → `crate::` |
| 18 | `src/daft-scan/src/test_utils.rs` | `common_file_formats::` → `crate::` |
| 19 | `src/daft-scan/src/source_config.rs` | `common_file_formats::` → `crate::` |

---

## Risk assessment

**Low risk.** This is purely a code-movement refactor:
- No behavioral changes — same types, same impls, different module paths
- All consuming crates already depend on `daft-scan`
- No new dependencies introduced
- `common-file-formats` still exists for `FileFormat`/`WriteMode`
