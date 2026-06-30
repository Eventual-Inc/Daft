# Scan Result Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cache MicroPartition results by scan identity so the same source file is only read from disk once per query, eliminating the N-fold redundant reads caused by N joins on the same lookup table.

**Architecture:** Add a `ScanCacheKey` type to `daft-scan` that uniquely identifies a scan by (source paths + pushdowns). Wire a per-query `DashMap<ScanCacheKey, Arc<Vec<MicroPartition>>>` through `BuilderContext` → `ScanTaskSource` → `stream_scan_task`. On a cache miss, collect the full stream, store it, and return a stream over the vec; on a hit, return a stream over the cached vec immediately.

**Tech Stack:** Rust, `dashmap` (already in workspace), `std::sync::Arc`, `futures::StreamExt`

## Global Constraints

- `dashmap` is already listed in `daft-local-execution/Cargo.toml` — do not add it again.
- `Pushdowns` already derives `Hash + PartialEq + Eq` — use it directly as a key component.
- Cache is per-query: created in `BuilderContext::new_with_context()`, dropped when the `BuilderContext` is dropped. No cross-query persistence.
- "Last writer wins" on a race is acceptable — concurrent writes to the same key produce identical data.
- Do not change any public Python-facing API or any optimizer rule.

---

## File Map

| File | Change |
|---|---|
| `src/daft-scan/src/lib.rs` | Add `ScanCacheKey` struct + `scan_cache_key()` fn |
| `src/daft-local-execution/src/sources/scan_task.rs` | Add `ScanCache` type alias; add `scan_cache` field to `ScanTaskSource`; thread through `spawn_scan_task_processor` → `forward_scan_task_stream` → `stream_scan_task`; implement cache logic in `stream_scan_task` |
| `src/daft-local-execution/src/pipeline.rs` | Add `scan_cache: Arc<ScanCache>` to `BuilderContext`; initialize in `new_with_context`; pass to `ScanTaskSource::new()` at line 506 |

---

### Task 1: ScanCacheKey type in daft-scan

**Files:**
- Modify: `src/daft-scan/src/lib.rs` (after the `ScanTask` struct, around line 370)
- Test: `src/daft-scan/src/lib.rs` (in the existing `#[cfg(test)]` block, or add one)

**Interfaces:**
- Produces: `pub struct ScanCacheKey { ... }` and `pub fn scan_cache_key(task: &ScanTask) -> ScanCacheKey`
- Consumed by Task 2 and Task 3.

- [ ] **Step 1: Write the failing test**

Add to `src/daft-scan/src/lib.rs` inside `#[cfg(test)] mod tests { ... }` (add the module if it doesn't exist):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_scan_task_with_path(path: &str) -> Arc<ScanTask> {
        // ScanTask with one File source, no pushdowns
        Arc::new(ScanTask {
            sources: vec![ScanSource {
                size_bytes: None,
                metadata: None,
                statistics: None,
                partition_spec: None,
                kind: ScanSourceKind::File {
                    path: path.to_string(),
                    chunk_spec: None,
                    iceberg_delete_files: None,
                    parquet_metadata: None,
                },
            }],
            schema: Arc::new(daft_schema::schema::Schema::empty()),
            source_config: Arc::new(SourceConfig::default()),  // adjust if SourceConfig has no Default
            storage_config: Arc::new(StorageConfig::default()),
            pushdowns: Pushdowns::default(),
            size_bytes_on_disk: None,
            metadata: None,
            statistics: None,
            generated_fields: None,
        })
    }

    #[test]
    fn test_same_path_same_key() {
        let t1 = make_scan_task_with_path("/data/foo.parquet");
        let t2 = make_scan_task_with_path("/data/foo.parquet");
        assert_eq!(scan_cache_key(&t1), scan_cache_key(&t2));
    }

    #[test]
    fn test_different_path_different_key() {
        let t1 = make_scan_task_with_path("/data/foo.parquet");
        let t2 = make_scan_task_with_path("/data/bar.parquet");
        assert_ne!(scan_cache_key(&t1), scan_cache_key(&t2));
    }
}
```

- [ ] **Step 2: Run to confirm it does not compile (type missing)**

```bash
cd /Users/anshulgoel/Work/Code/daft_code
cargo test -p daft-scan 2>&1 | head -30
```

Expected: compile error — `scan_cache_key` not found.

- [ ] **Step 3: Add ScanCacheKey and scan_cache_key to src/daft-scan/src/lib.rs**

Find the end of the `ScanTask` struct definition (around line 370) and add immediately after it:

```rust
/// Uniquely identifies a scan result: same key ↔ same bytes would be read from disk.
/// Used as the key for the per-query scan result cache.
#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub struct ScanCacheKey {
    /// One entry per ScanSource, encoding path + chunk selection + delete files.
    pub source_paths: Vec<String>,
    /// Column projection + filter pushdowns determine which data is read.
    pub pushdowns: Pushdowns,
}

/// Derive a cache key from a ScanTask.
///
/// Two tasks produce the same key iff they would read identical bytes from disk.
/// Source order within the task is preserved (not sorted) since it affects read order.
pub fn scan_cache_key(task: &ScanTask) -> ScanCacheKey {
    let source_paths = task
        .sources
        .iter()
        .map(|s| match &s.kind {
            ScanSourceKind::File {
                path,
                chunk_spec,
                iceberg_delete_files,
                ..
            } => format!("file:{}|chunk={:?}|deletes={:?}", path, chunk_spec, iceberg_delete_files),
            ScanSourceKind::Database { path } => format!("db:{}", path),
            #[cfg(feature = "python")]
            ScanSourceKind::PythonFactoryFunction {
                module, func_name, ..
            } => format!("python:{}:{}", module, func_name),
        })
        .collect();

    ScanCacheKey {
        source_paths,
        pushdowns: task.pushdowns.clone(),
    }
}
```

- [ ] **Step 4: Run the tests**

```bash
cd /Users/anshulgoel/Work/Code/daft_code
cargo test -p daft-scan -- tests 2>&1
```

Expected output includes:
```
test tests::test_same_path_same_key ... ok
test tests::test_different_path_different_key ... ok
```

If `SourceConfig` or `StorageConfig` lacks `Default`, adjust the test helper to use whatever constructor those types expose — the goal is just a minimal `ScanTask`; the exact config values don't matter for the key.

- [ ] **Step 5: Commit**

```bash
git add src/daft-scan/src/lib.rs
git commit -m "feat(scan): add ScanCacheKey type and scan_cache_key() fn"
```

---

### Task 2: Wire ScanCache into BuilderContext and ScanTaskSource::new()

**Files:**
- Modify: `src/daft-local-execution/src/sources/scan_task.rs` (top — add type alias and struct field)
- Modify: `src/daft-local-execution/src/pipeline.rs` (BuilderContext struct + constructor + call site at line 506)

**Interfaces:**
- Consumes: `ScanCacheKey` and `scan_cache_key` from Task 1 (`daft_scan::ScanCacheKey`, `daft_scan::scan_cache_key`)
- Produces:
  - `pub type ScanCache = DashMap<ScanCacheKey, Arc<Vec<MicroPartition>>>`
  - `BuilderContext.scan_cache: Arc<ScanCache>`
  - `ScanTaskSource::new(..., scan_cache: Arc<ScanCache>)` — extended signature

- [ ] **Step 1: Add ScanCache type alias and field to ScanTaskSource**

In `src/daft-local-execution/src/sources/scan_task.rs`, add near the top with the other `use` statements:

```rust
use dashmap::DashMap;
use daft_scan::{scan_cache_key, ScanCacheKey};

pub type ScanCache = DashMap<ScanCacheKey, Arc<Vec<MicroPartition>>>;
```

Then find the `ScanTaskSource` struct definition and add the new field:

```rust
pub struct ScanTaskSource {
    receiver: UnboundedReceiver<(InputId, Vec<ScanTaskRef>)>,
    source_config: Option<Arc<SourceConfig>>,
    pushdowns: Pushdowns,
    schema: SchemaRef,
    num_parallel_tasks: usize,
    skipped_corrupt_files: SkippedCorruptFilesCollector,
    scan_cache: Arc<ScanCache>,   // ← add this line
}
```

Update `ScanTaskSource::new()` to accept and store the cache:

```rust
pub fn new(
    receiver: UnboundedReceiver<(InputId, Vec<ScanTaskRef>)>,
    source_config: Option<Arc<SourceConfig>>,
    pushdowns: Pushdowns,
    schema: SchemaRef,
    cfg: &DaftExecutionConfig,
    skipped_corrupt_files: SkippedCorruptFilesCollector,
    scan_cache: Arc<ScanCache>,   // ← add this parameter
) -> Self {
    Self {
        receiver,
        source_config,
        pushdowns,
        schema,
        num_parallel_tasks: cfg
            .scantask_max_parallel
            .unwrap_or_else(num_cpus::get),
        skipped_corrupt_files,
        scan_cache,   // ← add this line
    }
}
```

- [ ] **Step 2: Add scan_cache to BuilderContext in pipeline.rs**

In `src/daft-local-execution/src/pipeline.rs`, add the import near the top:

```rust
use crate::sources::scan_task::ScanCache;
```

Add the field to the `BuilderContext` struct (after `skipped_corrupt_files`):

```rust
pub struct BuilderContext {
    index_counter: std::cell::RefCell<usize>,
    pub meter: Meter,
    context: HashMap<String, String>,
    shuffle_server: Option<(Arc<ShuffleFlightServer>, String)>,
    checkpoint: std::cell::RefCell<Option<(...)>>,
    pub skipped_corrupt_files: std::sync::Arc<std::sync::Mutex<Vec<(String, String, bool)>>>,
    pub scan_cache: Arc<ScanCache>,   // ← add this line
}
```

Update `new_with_context()` to initialize the cache:

```rust
pub fn new_with_context(
    query_id: QueryID,
    context: HashMap<String, String>,
    shuffle_server: Option<(Arc<ShuffleFlightServer>, String)>,
) -> Self {
    let meter = Meter::query_scope(query_id, "daft.execution.local");

    Self {
        index_counter: std::cell::RefCell::new(0),
        meter,
        context,
        shuffle_server,
        checkpoint: std::cell::RefCell::new(None),
        skipped_corrupt_files: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        scan_cache: Arc::new(ScanCache::new()),   // ← add this line
    }
}
```

- [ ] **Step 3: Pass scan_cache to ScanTaskSource::new() at the call site**

In `src/daft-local-execution/src/pipeline.rs` around line 506, find the existing call:

```rust
let scan_task_source = ScanTaskSource::new(
    rx,
    source_config.clone(),
    pushdowns.clone(),
    schema.clone(),
    cfg,
    Some(ctx.skipped_corrupt_files.clone()),
);
```

Update it to:

```rust
let scan_task_source = ScanTaskSource::new(
    rx,
    source_config.clone(),
    pushdowns.clone(),
    schema.clone(),
    cfg,
    Some(ctx.skipped_corrupt_files.clone()),
    Arc::clone(&ctx.scan_cache),   // ← add this line
);
```

- [ ] **Step 4: Verify it compiles**

```bash
cd /Users/anshulgoel/Work/Code/daft_code
cargo build -p daft-local-execution 2>&1 | head -40
```

Expected: compile errors only about `spawn_scan_task_processor` and `stream_scan_task` not yet accepting the cache — those are fixed in Task 3. No other errors.

- [ ] **Step 5: Commit**

```bash
git add src/daft-local-execution/src/sources/scan_task.rs \
        src/daft-local-execution/src/pipeline.rs
git commit -m "feat(execution): add ScanCache to BuilderContext and ScanTaskSource"
```

---

### Task 3: Implement cache logic in the execution chain

**Files:**
- Modify: `src/daft-local-execution/src/sources/scan_task.rs`
  - `spawn_scan_task_processor` (add parameter, pass through)
  - `forward_scan_task_stream` (add parameter, pass through)
  - `stream_scan_task` (add parameter, change return type, implement cache)
- Test: `src/daft-local-execution/src/sources/scan_task.rs` (add `#[cfg(test)]` block)

**Interfaces:**
- Consumes: `ScanCache` (Task 2), `scan_cache_key` (Task 1)
- Produces: `stream_scan_task` with cache hit/miss behaviour verified by test

- [ ] **Step 1: Write the failing test**

Add at the bottom of `src/daft-local-execution/src/sources/scan_task.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_scan_cache_hit_returns_same_arc() {
        // Build a cache and manually insert an entry
        let cache: Arc<ScanCache> = Arc::new(ScanCache::new());

        use daft_scan::{ScanCacheKey, Pushdowns};
        let key = ScanCacheKey {
            source_paths: vec!["file:/tmp/test.parquet|chunk=None|deletes=None".to_string()],
            pushdowns: Pushdowns::default(),
        };

        // Populate the cache with a sentinel Arc
        let sentinel: Arc<Vec<MicroPartition>> = Arc::new(vec![]);
        cache.insert(key.clone(), Arc::clone(&sentinel));

        // Verify cache hit returns the exact same Arc
        let hit = cache.get(&key).expect("cache should contain the key");
        assert!(Arc::ptr_eq(&sentinel, &*hit), "cache hit should return the same Arc");
    }

    #[test]
    fn test_scan_cache_miss_key_not_present() {
        let cache: Arc<ScanCache> = Arc::new(ScanCache::new());
        use daft_scan::{ScanCacheKey, Pushdowns};
        let key = ScanCacheKey {
            source_paths: vec!["file:/tmp/missing.parquet|chunk=None|deletes=None".to_string()],
            pushdowns: Pushdowns::default(),
        };
        assert!(cache.get(&key).is_none(), "empty cache should have no entry");
    }
}
```

- [ ] **Step 2: Run to confirm tests pass (these are pure unit tests, no I/O needed)**

```bash
cd /Users/anshulgoel/Work/Code/daft_code
cargo test -p daft-local-execution sources::scan_task::tests 2>&1
```

Expected: both tests pass. These validate the `DashMap` usage pattern before wiring it into the async path.

- [ ] **Step 3: Thread scan_cache through spawn_scan_task_processor**

Find `spawn_scan_task_processor` (around line 72) and add the parameter:

```rust
#[allow(clippy::too_many_arguments)]
fn spawn_scan_task_processor(
    num_parallel_tasks: usize,
    mut receiver: UnboundedReceiver<(InputId, Vec<ScanTaskRef>)>,
    output_sender: Sender<PipelineMessage>,
    stats_provider: StatsProvider,
    chunk_size: usize,
    schema: SchemaRef,
    maintain_order: bool,
    skipped_corrupt_files: SkippedCorruptFilesCollector,
    scan_cache: Arc<ScanCache>,   // ← add this parameter
) -> common_runtime::RuntimeTask<DaftResult<()>> {
```

Inside the function body, find every call to `forward_scan_task_stream(...)` and add `Arc::clone(&scan_cache)` as the last argument. There will typically be one or two call sites inside the task-spawning closure.

Find where `spawn_scan_task_processor` is called (inside `ScanTaskSource`'s `get_data` or `Source::get_data` impl) and add `Arc::clone(&self.scan_cache)` as the last argument.

- [ ] **Step 4: Thread scan_cache through forward_scan_task_stream**

Find `forward_scan_task_stream` (around line 482) and add the parameter:

```rust
async fn forward_scan_task_stream(
    scan_task: Arc<ScanTask>,
    io_stats: IOStatsRef,
    delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
    maintain_order: bool,
    chunk_size: usize,
    sender: ScanTaskOutputSender,
    input_id: InputId,
    skipped_corrupt_files: SkippedCorruptFilesCollector,
    scan_cache: Arc<ScanCache>,   // ← add this parameter
) -> DaftResult<InputId> {
```

Inside, find the call to `stream_scan_task(...)` and add `Arc::clone(&scan_cache)` as the last argument.

- [ ] **Step 5: Implement cache logic in stream_scan_task**

Find `stream_scan_task` (around line 547). Make three targeted changes:

**5a — Change the function signature** (add `scan_cache` parameter and change return type):

Old signature:
```rust
async fn stream_scan_task(
    scan_task: Arc<ScanTask>,
    io_stats: IOStatsRef,
    delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
    maintain_order: bool,
    chunk_size: usize,
    skipped_corrupt_files: SkippedCorruptFilesCollector,
) -> DaftResult<impl Stream<Item = DaftResult<MicroPartition>> + Send>
```

New signature:
```rust
async fn stream_scan_task(
    scan_task: Arc<ScanTask>,
    io_stats: IOStatsRef,
    delete_map: Option<Arc<HashMap<String, Vec<i64>>>>,
    maintain_order: bool,
    chunk_size: usize,
    skipped_corrupt_files: SkippedCorruptFilesCollector,
    scan_cache: Arc<ScanCache>,   // ← add
) -> DaftResult<futures::stream::BoxStream<'static, DaftResult<MicroPartition>>>  // ← change return type
```

**5b — Add cache-hit early return** at the very top of the function body (before any existing code):

```rust
use futures::TryStreamExt; // for .try_collect()

let key = scan_cache_key(&scan_task);

// Cache hit: return cached MicroPartitions as a stream without any I/O.
if let Some(cached) = scan_cache.get(&key) {
    let parts: Vec<MicroPartition> = cached.as_ref().clone();
    return Ok(futures::stream::iter(parts.into_iter().map(Ok)).boxed());
}
```

**5c — Replace the final return** (the existing `Ok(mp_stream)` or equivalent last line) with cache-populate + boxed return:

The existing function ends with something like:
```rust
    let mp_stream = table_stream.map(move |table| {
        let table = table?;
        let casted_table = table.cast_to_schema_with_fill(
            scan_task.materialized_schema().as_ref(),
            scan_task.partition_spec().as_ref().map(|pspec| pspec.to_fill_map()).as_ref(),
        )?;
        let stats = scan_task.statistics.as_ref()
            .map(|stats| stats.cast_to_schema(&scan_task.materialized_schema()))
            .transpose()?;
        let mp = MicroPartition::new_loaded(
            scan_task.materialized_schema(),
            Arc::new(vec![casted_table]),
            stats,
        );
        Ok(mp)
    });
    Ok(mp_stream)  // ← replace only this last line
```

Replace only the final `Ok(mp_stream)` with:

```rust
    // Cache miss: collect entire result, store, then stream from the Vec.
    // Eager collection is safe for the small lookup tables this targets (<10 MB).
    // Last-writer-wins on concurrent race: both writes produce identical data.
    let collected: Vec<MicroPartition> = mp_stream.try_collect().await?;
    let arc_collected = Arc::new(collected);
    scan_cache.insert(key, Arc::clone(&arc_collected));
    Ok(futures::stream::iter(arc_collected.as_ref().clone().into_iter().map(Ok)).boxed())
```

Also add `use futures::StreamExt;` near the top of the file if `.boxed()` is not already imported.
```

**Important:** The return type changes from `impl Stream<...>` to `BoxStream<'static, ...>`. Update the caller (`forward_scan_task_stream`) if it relies on the concrete `impl` type — it typically just calls `.await?` and iterates, so `BoxStream` is a drop-in.

- [ ] **Step 6: Build to verify it compiles**

```bash
cd /Users/anshulgoel/Work/Code/daft_code
cargo build -p daft-local-execution 2>&1
```

Expected: clean build. Fix any type mismatches (e.g., missing `use futures::StreamExt` for `.boxed()`).

- [ ] **Step 7: Run the existing scan tests**

```bash
cd /Users/anshulgoel/Work/Code/daft_code
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/dataframe/test_scan.py" 2>&1
```

If `tests/dataframe/test_scan.py` does not exist, run the general scan-related tests:

```bash
DAFT_RUNNER=native make test EXTRA_ARGS="-v -k scan" 2>&1
```

Expected: all existing tests pass. The cache is transparent when no duplicates exist (each unique key is inserted once, subsequent reads return the cached value — same data, no correctness change).

- [ ] **Step 8: Run the end-to-end pipeline to verify performance**

```bash
cd /Users/anshulgoel/Work/Code/daft_code
PYTHONPATH='/Users/anshulgoel/Work/Code/daft' \
DATA_DIR='/Users/anshulgoel/Work/Code/daft/warehouse' \
SOURCE_NAMESPACE='ttmnr 1' \
TARGET_NAMESPACE='ttmnr 1' \
WRITE_INTERMEDIATES=0 \
rtk .venv/bin/python -u /Users/anshulgoel/Downloads/ttmnr_address_entity_creation.py 2>&1
```

Expected: Phase 1 Step 2 (37 LEFT joins on mnr_name) completes in seconds, not minutes. The full pipeline should complete rather than timing out. Verify the `[_build_tomtom_address_id_to_mnr_name] elapsed` time is under 60 seconds.

- [ ] **Step 9: Commit**

```bash
git add src/daft-local-execution/src/sources/scan_task.rs
git commit -m "feat(execution): cache scan results per query to eliminate redundant file reads

When the same source file appears N times in a query plan (e.g., 37 left
joins on a lookup table), the file was previously read N times from disk.
The ScanCache deduplicates reads within a single query execution: first
access reads and caches, subsequent accesses return the cached
Vec<MicroPartition> directly.

Cache lifetime: per-query (held in BuilderContext, dropped after collect()).
Cache key: (source paths + chunk spec + pushdowns).
Race semantics: last-writer-wins — safe because concurrent writes produce
identical data."
```
