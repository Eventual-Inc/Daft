# Scan Result Cache — Design Spec

**Date:** 2026-06-30  
**Status:** Approved  
**Scope:** Narrow fix — eliminate redundant disk reads when the same source file appears multiple times in a single query plan.

---

## Problem

When the same DataFrame is referenced multiple times in a plan (e.g., 37 chained left joins each selecting different columns from the same lookup table), Daft creates a separate `Source` node per reference. After `MaterializeScans`, each node holds an independent `Vec<ScanTaskRef>` that reads the same file from disk. For `mnr_name.parquet` (5.6 MB, 99K rows) this means 37 separate Parquet reads during execution.

The two compounding effects:
1. Repeated I/O: same file read N times from disk.
2. Deep plan: a 37-level join chain the optimizer cannot reorder (hard cap at 12 relations in DP-CCP).

This spec covers fix #1. Fix #2 (plan-level CSE) is a separate, broader effort.

---

## Non-Goals

- Cross-query caching (no persistence between `collect()` calls).
- Large-table caching with eviction policy (unbounded cache is acceptable for small lookup tables).
- Plan-level CSE for computed subplans (e.g., `joined_base_df` shared by two downstream steps).
- Changes to the logical plan, optimizer rules, or physical plan nodes.

---

## Architecture

The cache is a per-query, execution-layer construct. No plan changes are required.

```
df.collect()
  └─ QueryContext (created fresh, dropped at query end)
       └─ ScanCache: Arc<DashMap<ScanCacheKey, Arc<OnceCell<CachedScan>>>>
            └─ threaded into every ScanTaskSource node at execution startup
```

### Files changed

| File | Change |
|---|---|
| `src/daft-scan/src/scan_task.rs` | Add `ScanCacheKey` struct + `scan_task_cache_key()` fn |
| `src/daft-local-execution/src/` (execution runtime context) | Add `Option<Arc<ScanCache>>` field |
| `src/daft-local-execution/src/sources/scan_task.rs` | Check cache before read; populate after read |

All changes are additive. No existing call sites break.

---

## Cache Key

Two scan tasks are equivalent — safe to share a cached result — when they read the same bytes from disk:

```rust
#[derive(Hash, Eq, PartialEq, Clone)]
pub struct ScanCacheKey {
    pub source_path: String,          // file URL or object store path
    pub columns: Vec<String>,         // sorted projected column names
    pub filters: Option<String>,      // Display of pushdown ExprRef, None if no filter
    pub row_groups: Option<Vec<i64>>, // Parquet row group indices, None if full file
}
```

`derive(Hash, Eq, PartialEq)` — no custom hashing needed.

**Why this is correct for the 37-join case:**  
`PushDownProjection` pushes column selection to the scan node; aliasing (e.g., `NAME_ID → COUNTRY_NAME_ID`) stays in a `Project` node above the scan. All 37 `mnr_name` scans therefore have `columns = ["NAME_ID", "NAME"]`, no filter, same path → same key → one read, 36 cache hits.

---

## Data Flow

```
ScanTaskSource::execute():
  for each ScanTask:
    key = scan_task_cache_key(&scan_task)

    match cache.entry(key):
      Occupied(cell) →
        wait on OnceCell (blocks until populated by whichever thread got here first)
        yield Arc::clone of cached Vec<MicroPartition>

      Vacant →
        insert new Arc<OnceCell<...>>
        read MicroPartitions from disk  ← actual I/O, happens once per unique key
        initialize OnceCell with result
        yield Arc::clone of result
```

`DashMap<ScanCacheKey, Arc<OnceCell<CachedScan>>>` provides:
- **Concurrent reads**: multiple threads can read from the same cache entry simultaneously once populated.
- **Single write**: `OnceCell` ensures only one thread executes the disk read; all others block until it completes.
- **No duplicate reads**: the `Vacant` branch is atomic in `DashMap` — two threads racing on the same key will produce exactly one `Vacant` winner.

`CachedScan` is a type alias for `Arc<Vec<MicroPartition>>`.

---

## Scope & Lifetime

- Cache is constructed at the start of `collect()` (or the equivalent local execution entry point).
- Passed as `Arc<ScanCache>` into the execution runtime context.
- Dropped automatically when the query finishes — no cross-query memory accumulation.
- Cache is opt-in: if `ScanCache` is `None` in the context (e.g., for Ray runner or streaming execution where this doesn't apply yet), `ScanTaskSource` falls through to the existing read path unchanged.

---

## Testing Plan

1. **Unit test — deduplication**: Build a physical plan with two `ScanTaskSource` nodes referencing the same file. Assert the file is read exactly once (mock/spy the underlying read function).
2. **Unit test — concurrent safety**: Spawn N threads each executing the same scan key simultaneously; assert no panic, result consistent, disk read count = 1.
3. **Integration test — 37-join pipeline**: Run the `ttmnr_address_entity_creation.py` pipeline end-to-end. Assert wall time drops significantly (target: Phase 1 Step 2 completes in seconds, not minutes).
4. **Regression**: All existing scan-related tests pass unchanged. Cache is transparent when no duplicates exist.

---

## Open Questions (resolved)

- **Cache key for multi-file tables**: Each `ScanTask` covers one file chunk. Multi-file tables produce one cache entry per file — correct behavior, no special handling needed.
- **Pushdown filter serialization**: Use `ExprRef`'s `Display` impl as the filter key. This is deterministic for the same expression tree.
- **Ray runner**: Out of scope. Cache is only wired into the local execution path for now.
