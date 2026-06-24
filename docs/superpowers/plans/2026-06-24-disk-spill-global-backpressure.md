# Disk Spill: Global Backpressure + Hash-Join Default + Dedup Spill — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Daft's per-operator static spill thresholds with one shared, pressure-driven spill pool; turn on hash-join spill by default; add spilling to Dedup; and fix repartition's finalize so spilling actually lowers peak memory.

**Architecture:** A single `MemoryManager`-owned spill pool (`≈0.3 × engine memory`) is charged through a new resident `MemoryReservation` handle held in each blocking sink's per-worker state. A shared `reconcile_reservation` loop replaces every per-operator threshold check: on each `sink()` the operator charges its new in-memory bytes; if the pool denies, it spills its largest bucket (sticky) and retries; if it cannot spill, it proceeds with a bounded one-morsel overshoot (never blocks → no deadlock). Five operators migrate onto this (sort, grouped-agg, window, hash-join, repartition); Dedup gains grace-style spill; repartition's finalize is restructured to stream partition-by-partition.

**Tech Stack:** Rust (`daft-local-execution`, `common-daft-config`, `daft-shuffles` crates), PyO3 config bindings, Python (`daft/context.py`), Arrow IPC spill files, `tokio`, `pytest`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-24-disk-spill-global-backpressure-design.md`. Every task implicitly inherits it.
- Spill pool default: `max(64 MiB, 0.3 × (DAFT_MEMORY_LIMIT or system RAM))`. Env override: `DAFT_SPILL_POOL_BYTES`.
- Per-operator `{sort,agg,window,hash_join,repartition}_spill_threshold_bytes`: `Some(0)` disables that operator's spilling (pure in-memory); `Some(n>0)` is honored as a per-operator **cap** and logs a one-time deprecation note; `None` = enabled, pool-governed.
- Deadlock-safety invariant: a spill-capable operator **never blocks** on memory. Denied + has resident data → spill own bucket; denied + nothing left → proceed (bounded one-morsel overshoot).
- Spill is a **native-engine-only** feature; Python spill tests skip the Ray runner (existing convention in `tests/test_spill_to_disk.py`).
- Spill files use Arrow IPC via the existing `src/daft-local-execution/src/spill/mod.rs` primitives. Spill-file prefixes follow the `daft_<op>_spill_` convention.
- Build after Rust changes: `make build` (slow; only when a Python-visible or cross-crate change must be exercised from Python). Rust unit tests run directly with `cargo test` and do **not** need `make build`.
- Commit after every task (frequent commits). Conventional Commits titles.

---

## File map

| File | Responsibility | Tasks |
|---|---|---|
| `src/daft-local-execution/src/resource_manager.rs` | Spill pool accounting; `MemoryReservation`; `reconcile_reservation` + `SpillableBuckets` trait | 1, 11 |
| `src/common/daft-config/src/lib.rs` | `spill_pool_bytes` field, default, env | 2 |
| `src/common/daft-config/src/python.rs` | PyO3 binding + getter for `spill_pool_bytes` | 2 |
| `daft/context.py` | Python `set_execution_config` param + docstring | 2 |
| `src/daft-local-execution/src/spill/mod.rs` | `SpillConfig` carries `pool_bytes`/`cap_bytes`; `partition_count` from pool | 3, 11 |
| `src/daft-local-execution/src/pipeline.rs` | `build_spill_config` helper; set pool size on manager; call sites | 3 |
| `src/daft-local-execution/src/sinks/sort.rs` | Sort → reservation trigger | 4 |
| `src/daft-local-execution/src/sinks/grouped_aggregate.rs` | Grouped-agg → reservation trigger | 5 |
| `src/daft-local-execution/src/sinks/window_base.rs` | Window → reservation trigger | 6 |
| `src/daft-local-execution/src/join/hash_join.rs` | Hash-join → reservation; on by default | 7 |
| `src/daft-shuffles/src/oneshot_writer.rs` | Streaming per-partition writer | 8 |
| `src/daft-local-execution/src/sinks/repartition.rs` | Streaming finalize call + reservation; prefix cleanup | 8, 9 |
| `src/daft-local-execution/src/sinks/dedup.rs` | Dedup grace spill | 10 |
| `tests/test_spill_to_disk.py` | End-to-end spill tests | 12 |

---

## Task 1: Spill pool + `MemoryReservation` + reconcile loop

**Files:**
- Modify: `src/daft-local-execution/src/resource_manager.rs`
- Test: same file (`#[cfg(test)] mod tests`, already present)

**Interfaces:**
- Produces:
  - `MemoryManager::spill_pool_bytes(&self) -> u64`
  - `MemoryManager::set_spill_pool_bytes(&self, n: u64)`
  - `MemoryManager::reservation(self: &Arc<Self>) -> MemoryReservation`
  - `MemoryReservation::try_grow(&mut self, bytes: u64) -> bool`
  - `MemoryReservation::shrink(&mut self, bytes: u64)`
  - `MemoryReservation::held(&self) -> u64`
  - `trait SpillableBuckets { fn resident_bytes(&self) -> u64; fn spill_largest_bucket(&mut self) -> DaftResult<bool>; }`
  - `fn reconcile_reservation<S: SpillableBuckets>(state: &mut S, reservation: &mut MemoryReservation, cap: Option<u64>) -> DaftResult<()>`

- [ ] **Step 1: Write failing tests**

Add to the existing `mod tests` in `resource_manager.rs`:

```rust
    #[test]
    fn test_spill_pool_default_is_fraction_of_total() {
        let manager = MemoryManager::new();
        // Default pool is 0.3 of total, floored at 64 MiB.
        let expected =
            ((manager.total_bytes as f64 * 0.3) as u64).max(64 * 1024 * 1024);
        assert_eq!(manager.spill_pool_bytes(), expected);
    }

    #[test]
    fn test_reservation_grow_shrink_and_drop_release() {
        let manager = Arc::new(MemoryManager::new());
        manager.set_spill_pool_bytes(1000);
        let mut r = manager.reservation();
        assert!(r.try_grow(600));
        assert_eq!(r.held(), 600);
        assert!(r.try_grow(400)); // exactly fills pool
        assert!(!r.try_grow(1)); // pool exhausted
        r.shrink(500);
        assert_eq!(r.held(), 500);
        assert!(r.try_grow(500)); // room again after shrink
        drop(r);
        // After drop, a fresh reservation can take the whole pool back.
        let mut r2 = manager.reservation();
        assert!(r2.try_grow(1000));
    }

    #[test]
    fn test_reconcile_spills_largest_until_it_fits() {
        struct Fake {
            buckets: Vec<u64>,
            spilled: Vec<usize>,
        }
        impl SpillableBuckets for Fake {
            fn resident_bytes(&self) -> u64 {
                self.buckets.iter().sum()
            }
            fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
                let Some((idx, _)) = self
                    .buckets
                    .iter()
                    .enumerate()
                    .filter(|(_, b)| **b > 0)
                    .max_by_key(|(_, b)| **b)
                else {
                    return Ok(false);
                };
                self.spilled.push(idx);
                self.buckets[idx] = 0;
                Ok(true)
            }
        }
        let manager = Arc::new(MemoryManager::new());
        manager.set_spill_pool_bytes(100);
        let mut r = manager.reservation();
        // Three buckets totalling 150 > pool 100 → must spill the largest (80).
        let mut f = Fake {
            buckets: vec![80, 40, 30],
            spilled: vec![],
        };
        reconcile_reservation(&mut f, &mut r, None).unwrap();
        assert_eq!(f.spilled, vec![0]); // largest spilled first
        assert_eq!(f.resident_bytes(), 70); // 40 + 30 remain
        assert_eq!(r.held(), 70); // exactly charged
    }

    #[test]
    fn test_reconcile_overshoot_when_nothing_to_spill() {
        struct Empty;
        impl SpillableBuckets for Empty {
            fn resident_bytes(&self) -> u64 {
                500
            }
            fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
                Ok(false) // nothing spillable
            }
        }
        let manager = Arc::new(MemoryManager::new());
        manager.set_spill_pool_bytes(100);
        let mut r = manager.reservation();
        // resident 500 > pool 100, nothing to spill → returns Ok, bounded overshoot.
        reconcile_reservation(&mut Empty, &mut r, None).unwrap();
        assert!(r.held() <= 100);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p daft-local-execution --lib resource_manager`
Expected: FAIL — `spill_pool_bytes`, `reservation`, `reconcile_reservation`, `SpillableBuckets` not found.

- [ ] **Step 3: Add spill-pool state to `MemoryManager`**

Replace the `MemoryState` struct and add a pool field. Change:

```rust
struct MemoryState {
    available_bytes: u64,
}
```
to:
```rust
struct MemoryState {
    /// Transient working-memory budget (UDF reservations via `request_bytes`).
    available_bytes: u64,
    /// Total size of the resident spill pool (configurable).
    spill_pool_bytes: u64,
    /// Bytes of the spill pool currently held by live `MemoryReservation`s.
    spill_used_bytes: u64,
}
```

In `Default::default()` and `MemoryManager::new()`, initialize the two new fields. Add a helper for the default pool size and use it in both constructors:

```rust
/// Default spill pool: 30% of the engine budget, floored at 64 MiB, overridable via env.
fn default_spill_pool_bytes(total: u64) -> u64 {
    const SPILL_FRACTION: f64 = 0.3;
    const MIN_POOL_BYTES: u64 = 64 * 1024 * 1024;
    if let Ok(val) = std::env::var("DAFT_SPILL_POOL_BYTES")
        && let Ok(parsed) = val.trim().parse::<u64>()
        && parsed > 0
    {
        return parsed;
    }
    (((total as f64) * SPILL_FRACTION) as u64).max(MIN_POOL_BYTES)
}
```

In `impl Default` body, set the state to:
```rust
            state: Mutex::new(MemoryState {
                available_bytes: total_mem,
                spill_pool_bytes: default_spill_pool_bytes(total_mem),
                spill_used_bytes: 0,
            }),
```
In `MemoryManager::new()`'s custom-limit branch, mirror it with `custom_limit` in place of `total_mem` for both `available_bytes` and the pool seed.

- [ ] **Step 4: Add pool getters/setter and reservation factory**

Add these methods inside `impl MemoryManager` (after `total_bytes`):

```rust
    /// Current configured size of the resident spill pool (bytes).
    pub fn spill_pool_bytes(&self) -> u64 {
        self.state.lock().unwrap().spill_pool_bytes
    }

    /// Override the spill pool size (last-writer-wins; called at pipeline build from config).
    pub fn set_spill_pool_bytes(&self, n: u64) {
        let mut state = self.state.lock().unwrap();
        state.spill_pool_bytes = n;
        drop(state);
        self.notify.notify_waiters();
    }

    /// Create an empty resident reservation against the spill pool.
    pub fn reservation(self: &Arc<Self>) -> MemoryReservation {
        MemoryReservation {
            held: 0,
            manager: self.clone(),
        }
    }

    /// Try to charge `bytes` to the spill pool. Non-blocking; returns false if the pool is full.
    fn try_take_spill(&self, bytes: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.spill_used_bytes + bytes <= state.spill_pool_bytes {
            state.spill_used_bytes += bytes;
            true
        } else {
            false
        }
    }

    /// Return `bytes` to the spill pool.
    fn give_back_spill(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        {
            let mut state = self.state.lock().unwrap();
            state.spill_used_bytes = state.spill_used_bytes.saturating_sub(bytes);
        }
        self.notify.notify_waiters();
    }
```

- [ ] **Step 5: Add `MemoryReservation`, `SpillableBuckets`, `reconcile_reservation`**

Add near the top of the file (after `MemoryPermit`):

```rust
/// A resident reservation against the spill pool, held for as long as an operator keeps data in
/// memory. Unlike `MemoryPermit` (released at task end), this is held across many `sink()` calls and
/// released at finalize or on drop. Bytes are returned to the pool on `Drop` on every path.
pub(crate) struct MemoryReservation {
    held: u64,
    manager: Arc<MemoryManager>,
}

impl MemoryReservation {
    /// Try to charge `bytes` more. Returns false (no error) if the pool is exhausted.
    pub fn try_grow(&mut self, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        if self.manager.try_take_spill(bytes) {
            self.held += bytes;
            true
        } else {
            false
        }
    }

    /// Return up to `bytes` to the pool (clamped to what is held).
    pub fn shrink(&mut self, bytes: u64) {
        let b = bytes.min(self.held);
        self.held -= b;
        self.manager.give_back_spill(b);
    }

    pub fn held(&self) -> u64 {
        self.held
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.manager.give_back_spill(self.held);
    }
}

/// Implemented by blocking-sink states that hold spillable hash buckets so the shared
/// `reconcile_reservation` loop can drive spill decisions.
pub(crate) trait SpillableBuckets {
    /// Total in-memory bytes currently held across all buckets.
    fn resident_bytes(&self) -> u64;
    /// Spill the single largest in-memory bucket to disk, clearing it from memory. Returns false if
    /// there is nothing left to spill.
    fn spill_largest_bucket(&mut self) -> DaftResult<bool>;
}

/// Charge `state`'s resident bytes against the spill pool, spilling its largest buckets until the
/// pool admits the total. `cap` (per-operator deprecated cap) forces spilling once `held` would
/// exceed it even if the pool has room. Never blocks: if nothing can be spilled, returns with a
/// bounded (one-morsel) overshoot.
pub(crate) fn reconcile_reservation<S: SpillableBuckets>(
    state: &mut S,
    reservation: &mut MemoryReservation,
    cap: Option<u64>,
) -> DaftResult<()> {
    loop {
        let resident = state.resident_bytes();
        let held = reservation.held();
        if held > resident {
            reservation.shrink(held - resident);
            continue;
        }
        let need = resident - held;
        if need == 0 {
            return Ok(());
        }
        let over_cap = cap.is_some_and(|c| held + need > c);
        if !over_cap && reservation.try_grow(need) {
            return Ok(());
        }
        if !state.spill_largest_bucket()? {
            return Ok(()); // nothing left to spill → bounded overshoot
        }
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p daft-local-execution --lib resource_manager`
Expected: PASS (all existing tests plus the four new ones).

- [ ] **Step 7: Commit**

```bash
git add src/daft-local-execution/src/resource_manager.rs
git commit -m "feat(swordfish): add shared spill pool, MemoryReservation, reconcile loop"
```

---

## Task 2: `spill_pool_bytes` config knob

**Files:**
- Modify: `src/common/daft-config/src/lib.rs`
- Modify: `src/common/daft-config/src/python.rs`
- Modify: `daft/context.py`
- Test: `src/common/daft-config/src/lib.rs` (`#[cfg(test)]`) — add if no module exists

**Interfaces:**
- Produces: `DaftExecutionConfig.spill_pool_bytes: Option<usize>` (default `None`); env `DAFT_SPILL_POOL_BYTES`; Python kwarg `spill_pool_bytes` on `set_execution_config`.

- [ ] **Step 1: Add the field to `DaftExecutionConfig`**

In `src/common/daft-config/src/lib.rs`, after `repartition_spill_threshold_bytes` in the struct:

```rust
    /// Total size (bytes) of the shared spill pool that all spill-capable operators draw from.
    /// `None` derives `max(64 MiB, 0.3 × (DAFT_MEMORY_LIMIT or system RAM))`. Env override:
    /// `DAFT_SPILL_POOL_BYTES`.
    pub spill_pool_bytes: Option<usize>,
```

In `impl Default`, after `repartition_spill_threshold_bytes: None,`:

```rust
            spill_pool_bytes: None,
```

Add the env constant next to the other `ENV_DAFT_*_SPILL_THRESHOLD` consts:

```rust
    const ENV_DAFT_SPILL_POOL_BYTES: &'static str = "DAFT_SPILL_POOL_BYTES";
```

In `from_env`, after the repartition block:

```rust
        if let Ok(val) = std::env::var(Self::ENV_DAFT_SPILL_POOL_BYTES) {
            if let Ok(parsed) = val.trim().parse::<usize>() {
                cfg.spill_pool_bytes = Some(parsed);
            } else {
                eprintln!("Invalid DAFT_SPILL_POOL_BYTES value: {val}, ignoring");
            }
        }
```

- [ ] **Step 2: Add the PyO3 binding**

In `src/common/daft-config/src/python.rs`, add `spill_pool_bytes=None,` to the `#[pyo3(signature = (...))]` list and `spill_pool_bytes: Option<usize>,` to the `with_config_values` params (both directly after the `repartition_spill_threshold_bytes` entries). Then in the body, after the repartition assignment:

```rust
        if let Some(spill_pool_bytes) = spill_pool_bytes {
            config.spill_pool_bytes = Some(spill_pool_bytes);
        }
```

And add a getter near the other spill getters:

```rust
    #[getter]
    fn spill_pool_bytes(&self) -> PyResult<Option<usize>> {
        Ok(self.config.spill_pool_bytes)
    }
```

- [ ] **Step 3: Add the Python kwarg + docstring**

In `daft/context.py`, locate `set_execution_config` and its `repartition_spill_threshold_bytes` parameter; add `spill_pool_bytes: int | None = None,` to the signature, thread it into the underlying `_config.with_config_values(...)` call, and add a docstring line:

```
            spill_pool_bytes: Total size in bytes of the shared spill pool used by all spill-capable
                operators (sort, grouped aggregation, window, hash join, repartition, dedup). When
                unset, defaults to ~30% of the engine memory budget. Per-operator thresholds set to 0
                disable spilling for that operator.
```

(Match the exact plumbing pattern used by the adjacent `repartition_spill_threshold_bytes` kwarg in the same function.)

- [ ] **Step 4: Add a Rust default test**

Add (or create) a `#[cfg(test)] mod tests` in `lib.rs`:

```rust
    #[test]
    fn test_spill_pool_bytes_defaults_none() {
        let cfg = DaftExecutionConfig::default();
        assert_eq!(cfg.spill_pool_bytes, None);
    }
```

- [ ] **Step 5: Run the Rust test**

Run: `cargo test -p common-daft-config spill_pool_bytes`
Expected: PASS.

- [ ] **Step 6: Build the extension and smoke-test the Python binding**

Run: `make build`
Then: `DAFT_RUNNER=native uv run python -c "import daft; daft.context.set_execution_config(spill_pool_bytes=12345); print(daft.context.get_context().daft_execution_config.spill_pool_bytes)"`
Expected: prints `12345`.

- [ ] **Step 7: Commit**

```bash
git add src/common/daft-config/src/lib.rs src/common/daft-config/src/python.rs daft/context.py
git commit -m "feat(config): add spill_pool_bytes shared-pool config knob"
```

---

## Task 3: `SpillConfig` carries pool/cap; pipeline sets pool + builds configs

**Files:**
- Modify: `src/daft-local-execution/src/spill/mod.rs`
- Modify: `src/daft-local-execution/src/pipeline.rs`
- Test: `src/daft-local-execution/src/spill/mod.rs` (`#[cfg(test)]`)

This task is **additive**: it keeps `threshold_bytes` so the codebase stays green while operators migrate one at a time (Tasks 4–10). `threshold_bytes` is removed in Task 11.

**Interfaces:**
- Produces:
  - `SpillConfig { threshold_bytes: usize, spill_dirs: Vec<String>, pool_bytes: usize, cap_bytes: Option<usize> }`
  - `SpillConfig::cap(&self) -> Option<u64>`
  - `pipeline::build_spill_config(opt_out: Option<usize>, cfg: &DaftExecutionConfig) -> Option<SpillConfig>`

- [ ] **Step 1: Write a failing test for `partition_count` from `pool_bytes`**

Add to `src/daft-local-execution/src/spill/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_count_from_pool_bytes() {
        // 1 GiB pool / 256 MiB target = 4 partitions.
        let sc = SpillConfig {
            threshold_bytes: 0,
            spill_dirs: vec!["/tmp".to_string()],
            pool_bytes: 1024 * 1024 * 1024,
            cap_bytes: None,
        };
        assert_eq!(sc.partition_count(), 4);
        // Tiny pool floors at 2.
        let sc2 = SpillConfig {
            threshold_bytes: 0,
            spill_dirs: vec!["/tmp".to_string()],
            pool_bytes: 1,
            cap_bytes: None,
        };
        assert_eq!(sc2.partition_count(), 2);
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p daft-local-execution --lib spill::tests::test_partition_count_from_pool_bytes`
Expected: FAIL — struct fields `pool_bytes`/`cap_bytes` don't exist.

- [ ] **Step 3: Extend `SpillConfig`**

In `spill/mod.rs`, change the struct and `new`, and switch `partition_count` to `pool_bytes`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct SpillConfig {
    /// DEPRECATED (removed once all operators use the pool): legacy per-operator budget.
    pub threshold_bytes: usize,
    /// Directories to create spill files in (round-robined per bucket/run).
    pub spill_dirs: Vec<String>,
    /// Shared spill pool size; used only to size `partition_count` and finalize recursion budgets.
    pub pool_bytes: usize,
    /// Optional deprecated per-operator cap (bytes) on this operator's resident reservation.
    pub cap_bytes: Option<usize>,
}

impl SpillConfig {
    pub fn new(pool_bytes: usize, spill_dirs: Vec<String>) -> Self {
        Self {
            threshold_bytes: pool_bytes,
            spill_dirs,
            pool_bytes,
            cap_bytes: None,
        }
    }

    /// The per-operator cap as `u64`, if any.
    pub fn cap(&self) -> Option<u64> {
        self.cap_bytes.map(|c| c as u64)
    }

    /// Number of hash partitions for the join build side: `max(2, ceil(pool / 256 MiB))`, capped 256.
    pub fn partition_count(&self) -> usize {
        const TARGET_BYTES_PER_PARTITION: usize = 256 * 1024 * 1024; // 256 MiB
        let n = self.pool_bytes.div_ceil(TARGET_BYTES_PER_PARTITION);
        n.max(2).min(256)
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p daft-local-execution --lib spill::tests::test_partition_count_from_pool_bytes`
Expected: PASS.

- [ ] **Step 5: Add `build_spill_config` and replace `auto_spill_threshold` call sites**

In `src/daft-local-execution/src/pipeline.rs`, add below `auto_spill_threshold`:

```rust
/// Build the optional `SpillConfig` for one operator. Applies the global pool size from config to
/// the shared memory manager, honours the per-operator opt-out (`Some(0)` → disabled) and the
/// deprecated positive-value cap.
fn build_spill_config(
    opt_out: Option<usize>,
    cfg: &DaftExecutionConfig,
) -> Option<crate::spill::SpillConfig> {
    if matches!(opt_out, Some(0)) {
        return None; // operator opted out
    }
    let manager = crate::resource_manager::get_or_init_memory_manager();
    if let Some(n) = cfg.spill_pool_bytes
        && n > 0
    {
        manager.set_spill_pool_bytes(n as u64);
    }
    let pool = manager.spill_pool_bytes() as usize;
    let mut sc = crate::spill::SpillConfig::new(pool, cfg.flight_shuffle_dirs.clone());
    if let Some(n) = opt_out
        && n > 0
    {
        tracing::warn!(
            "Per-operator spill threshold ({n} bytes) is deprecated; using shared spill pool with \
             this value as a cap. Prefer `spill_pool_bytes`."
        );
        sc.cap_bytes = Some(n);
    }
    Some(sc)
}
```

Replace each existing spill-config construction with `build_spill_config`:

- Window (three sites, lines ~548, ~582, ~621):
  ```rust
  let window_spill = build_spill_config(cfg.window_spill_threshold_bytes, cfg);
  ```
- Grouped agg (line ~985):
  ```rust
  let spill_config = build_spill_config(cfg.agg_spill_threshold_bytes, cfg);
  ```
- Sort (line ~1087):
  ```rust
  let spill_config = build_spill_config(cfg.sort_spill_threshold_bytes, cfg);
  ```
- Repartition (line ~1762): replace the `auto_spill_threshold(...)` line with
  ```rust
  let spill_config = build_spill_config(cfg.repartition_spill_threshold_bytes, cfg);
  ```
  and (temporarily) pass `spill_config.as_ref().map(|sc| sc.threshold_bytes)` to the existing `try_new_flight(..., spill_threshold)` argument so this task compiles without touching repartition logic yet:
  ```rust
  let spill_threshold = spill_config.as_ref().map(|sc| sc.threshold_bytes);
  ```
  (Repartition is fully migrated in Task 9.)
- Hash join: locate its `SpillConfig` construction (search `hash_join_spill_threshold_bytes`). It currently maps the threshold to `Some` only when set. Replace with:
  ```rust
  let spill_config = build_spill_config(cfg.hash_join_spill_threshold_bytes, cfg);
  ```
  (This already flips hash-join **on by default**, since `None` now yields `Some(SpillConfig)`. Behavior is finished in Task 7.)

Leave `auto_spill_threshold` in place for now (only repartition's old call used it; it is removed in Task 11 if unused).

- [ ] **Step 6: Build to verify the whole crate still compiles**

Run: `cargo build -p daft-local-execution`
Expected: compiles. Spill behavior is still threshold-driven inside each operator (migrations follow).

- [ ] **Step 7: Commit**

```bash
git add src/daft-local-execution/src/spill/mod.rs src/daft-local-execution/src/pipeline.rs
git commit -m "feat(swordfish): SpillConfig carries pool/cap; build_spill_config wires the pool"
```

---

## Task 4: Migrate Sort to the reservation trigger

**Files:**
- Modify: `src/daft-local-execution/src/sinks/sort.rs`
- Test: `tests/test_spill_to_disk.py` exercises this end-to-end in Task 12; for unit confidence, rely on `cargo build` + existing Python sort spill test.

**Interfaces:**
- Consumes: `MemoryManager::reservation`, `MemoryReservation` (Task 1).
- Sort has `max_concurrency() == 1`, so a single state owns one reservation; on denial it spills the whole buffer as one sorted run.

- [ ] **Step 1: Add a reservation to `SortState`**

In `sort.rs`, extend the struct and imports:

```rust
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    resource_manager::{MemoryReservation, get_or_init_memory_manager},
    spill::{SpillConfig, SpillRunReader, SpillRunWriter, SpilledRun},
};
```
```rust
pub(crate) struct SortState {
    buffer: Vec<MicroPartition>,
    buffer_bytes: usize,
    runs: Vec<SpilledRun>,
    spill_rr: usize,
    /// Resident reservation against the shared spill pool (only used when spilling is enabled).
    reservation: MemoryReservation,
}
```

In `make_state`:

```rust
    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(SortState {
            buffer: Vec::new(),
            buffer_bytes: 0,
            runs: Vec::new(),
            spill_rr: 0,
            reservation: get_or_init_memory_manager().reservation(),
        })
    }
```

- [ ] **Step 2: Replace the threshold trigger with a reservation trigger**

In `sink`, replace the spill block (the `if let Some(sc) = &params.spill_config && *buffer_bytes >= sc.threshold_bytes { ... }`) with a reservation-driven version that spills the whole buffer as one run on denial:

```rust
                    *buffer_bytes += input.size_bytes();
                    let added = input.size_bytes() as u64;
                    buffer.push(input);

                    if let Some(sc) = &params.spill_config {
                        let cap = sc.cap();
                        let over_cap =
                            cap.is_some_and(|c| state.reservation.held() + added > c);
                        if over_cap || !state.reservation.try_grow(added) {
                            // Spill the entire in-memory buffer as one sorted run.
                            let SortState {
                                buffer,
                                buffer_bytes,
                                runs,
                                spill_rr,
                                reservation,
                            } = &mut state;
                            let to_spill = std::mem::take(buffer);
                            *buffer_bytes = 0;
                            let sorted = MicroPartition::concat(to_spill)?.sort(
                                &params.sort_by,
                                &params.descending,
                                &params.nulls_first,
                            )?;
                            if sorted.len() > 0 {
                                let dir = &sc.spill_dirs[*spill_rr % sc.spill_dirs.len()];
                                *spill_rr += 1;
                                let mut writer =
                                    SpillRunWriter::open(dir, "daft_sort_run_", &params.schema)?;
                                for rb in sorted.record_batches() {
                                    let n = rb.len();
                                    let mut start = 0;
                                    while start < n {
                                        let end = (start + MERGE_RUN_BATCH_ROWS).min(n);
                                        writer.write_batch(&rb.slice(start, end)?)?;
                                        start = end;
                                    }
                                }
                                runs.push(writer.finish()?);
                            }
                            // Everything in memory was spilled; release the reservation.
                            let held = reservation.held();
                            reservation.shrink(held);
                        }
                    }
                    Ok(state)
```

Note: the destructuring `let SortState { .. } = &mut state;` at the top of the async block (lines ~137-142) is no longer needed once this block owns the fields; remove the now-unused earlier destructure if the compiler flags it, keeping a single `let mut state = state;`.

- [ ] **Step 3: Drop the `threshold_bytes > 0` filter in `SortSink::new`**

In `SortSink::new`, the `spill_config.filter(|sc| sc.threshold_bytes > 0 && ...)` should no longer gate on `threshold_bytes` (the opt-out is handled at build time). Change it to only check key-type support:

```rust
        let spill_config = spill_config.filter(|_sc| {
            build_sort_fields(&sort_by, &descending, &nulls_first, &schema)
                .map(|fields| RowConverter::supports_fields(&fields))
                .unwrap_or(false)
        });
```

- [ ] **Step 4: Build**

Run: `cargo build -p daft-local-execution`
Expected: compiles.

- [ ] **Step 5: Run the existing Python sort spill test**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k sort tests/test_spill_to_disk.py"`
Expected: PASS (sort spill still works, now pool-driven).

- [ ] **Step 6: Commit**

```bash
git add src/daft-local-execution/src/sinks/sort.rs
git commit -m "feat(swordfish): drive Sort spill from the shared reservation"
```

---

## Task 5: Migrate Grouped Aggregate to the reservation trigger

**Files:**
- Modify: `src/daft-local-execution/src/sinks/grouped_aggregate.rs`
- Test: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k agg tests/test_spill_to_disk.py"`

**Interfaces:**
- Consumes: `reconcile_reservation`, `SpillableBuckets`, `MemoryReservation` (Task 1).
- The existing `maybe_spill(budget)` becomes "spill the single largest bucket"; the `push` calls `reconcile_reservation` instead of `maybe_spill`.

- [ ] **Step 1: Add reservation + cap to `GroupedAggregateState::Accumulating`**

Add imports:

```rust
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    resource_manager::{MemoryReservation, SpillableBuckets, get_or_init_memory_manager,
        reconcile_reservation},
    spill::{SpillConfig, SpillStore, SpillWriter},
};
```

Add fields to the `Accumulating` variant: `reservation: MemoryReservation` and `cap: Option<u64>`. Update `GroupedAggregateState::new` to accept and store them, and `make_state` to pass `get_or_init_memory_manager().reservation()` and `self.spill_config.as_ref().and_then(|sc| sc.cap())`.

- [ ] **Step 2: Replace the `maybe_spill(budget)` call in `push` with reconcile**

In `push`, replace the trailing spill block:

```rust
        // Reconcile resident bytes against the shared spill pool, spilling largest buckets as needed.
        if spill_dirs.is_some() {
            // Borrow split: reconcile needs &mut self, so run it after the borrow above ends.
        }
```

Because `push` currently destructures `self`, restructure so the reconcile runs on `self` after `execute_strategy`. Concretely, change `push` to:

```rust
    fn push(
        &mut self,
        input: MicroPartition,
        params: &GroupedAggregateParams,
        global_strategy_lock: &Arc<Mutex<Option<AggStrategy>>>,
    ) -> DaftResult<()> {
        let spill_enabled = matches!(
            self,
            Self::Accumulating { spill_dirs: Some(_), .. }
        );
        {
            let Self::Accumulating {
                inner_states,
                strategy,
                partial_agg_threshold,
                high_cardinality_threshold_ratio,
                ..
            } = self;
            if let Some(strategy) = strategy {
                strategy.execute_strategy(inner_states, input, params)?;
            } else {
                let decided = Self::determine_agg_strategy(
                    &input,
                    params,
                    *high_cardinality_threshold_ratio,
                    *partial_agg_threshold,
                    strategy,
                    global_strategy_lock,
                )?;
                decided.execute_strategy(inner_states, input, params)?;
            }
        }
        if spill_enabled {
            let cap = match self {
                Self::Accumulating { cap, .. } => *cap,
            };
            // SpillableBuckets is implemented on the params-bound view below.
            let mut view = AggSpillView { state: self, params };
            let reservation = match self {
                Self::Accumulating { reservation, .. } => reservation,
            };
            // reconcile borrows view (state) and reservation separately — split via raw helper.
            reconcile_agg(&mut view, reservation, cap)?;
        }
        Ok(())
    }
```

Because `view` and `reservation` both borrow `self`, introduce a small concrete reconcile that takes the `Accumulating` fields by explicit mutable refs to avoid aliasing. Replace the `if spill_enabled { ... }` body with a direct destructure:

```rust
        if spill_enabled {
            let Self::Accumulating {
                inner_states,
                spill_dirs,
                spill_writer,
                reservation,
                cap,
                ..
            } = self
            else {
                unreachable!()
            };
            let dirs = spill_dirs.as_ref().unwrap().clone();
            let decomposable = !params.partial_agg_exprs.is_empty();
            let mut buckets = AggBuckets {
                inner_states,
                spill_writer,
                spill_dirs: dirs,
                decomposable,
                params,
            };
            reconcile_reservation(&mut buckets, reservation, *cap)?;
        }
        Ok(())
```

- [ ] **Step 3: Add the `AggBuckets` adapter implementing `SpillableBuckets`**

Add to the file (this folds the existing `maybe_spill` per-bucket spill logic into "spill the largest bucket"):

```rust
/// Adapter that lets `reconcile_reservation` drive grace-aggregation spilling: resident bytes are
/// summed across buckets; `spill_largest_bucket` spills the heaviest bucket (compacting decomposable
/// aggregates first, exactly as the previous `maybe_spill` did).
struct AggBuckets<'a> {
    inner_states: &'a mut [Option<SinglePartitionAggregateState>],
    spill_writer: &'a mut Option<SpillWriter>,
    spill_dirs: Vec<String>,
    decomposable: bool,
    params: &'a GroupedAggregateParams,
}

impl SpillableBuckets for AggBuckets<'_> {
    fn resident_bytes(&self) -> u64 {
        self.inner_states
            .iter()
            .flatten()
            .map(|st| (st.partial_bytes + st.unagg_bytes) as u64)
            .sum()
    }

    fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
        let num_buckets = self.inner_states.len();
        let Some(p) = (0..num_buckets)
            .filter(|&p| {
                self.inner_states[p]
                    .as_ref()
                    .is_some_and(|st| st.partial_bytes + st.unagg_bytes > 0)
            })
            .max_by_key(|&p| {
                let st = self.inner_states[p].as_ref().unwrap();
                st.partial_bytes + st.unagg_bytes
            })
        else {
            return Ok(false);
        };
        let st = self.inner_states[p].as_mut().unwrap();

        let to_spill: MicroPartition = if self.decomposable {
            let mut partials = std::mem::take(&mut st.partially_aggregated);
            let unagg = std::mem::take(&mut st.unaggregated);
            st.unaggregated_size = 0;
            st.partial_bytes = 0;
            st.unagg_bytes = 0;
            if !unagg.is_empty() {
                partials.push(
                    MicroPartition::concat(unagg)?
                        .agg(&self.params.partial_agg_exprs, &self.params.group_by)?,
                );
            }
            if partials.is_empty() {
                return Ok(true);
            }
            MicroPartition::concat(partials)?
                .agg(&self.params.final_agg_exprs, &self.params.final_group_by)?
        } else {
            let unagg = std::mem::take(&mut st.unaggregated);
            st.unaggregated_size = 0;
            st.unagg_bytes = 0;
            if unagg.is_empty() {
                return Ok(true);
            }
            MicroPartition::concat(unagg)?
        };

        if to_spill.len() == 0 {
            return Ok(true);
        }

        let writer = match self.spill_writer {
            Some(w) => w,
            None => {
                let schema = to_spill.schema();
                let w = SpillWriter::new(
                    num_buckets,
                    &schema,
                    self.spill_dirs.clone(),
                    "daft_agg_spill_",
                )?;
                *self.spill_writer = Some(w);
                self.spill_writer.as_mut().unwrap()
            }
        };
        for rb in to_spill.record_batches() {
            writer.write_batch(p, rb)?;
        }
        Ok(true)
    }
}
```

Delete the old `maybe_spill` method (now replaced).

- [ ] **Step 4: Update `recursion_budget` source in `finalize`**

`finalize` sets `recursion_budget = self.budget_per_bucket` when spilling. Replace `self.budget_per_bucket` with a per-bucket share of the pool:

```rust
        let recursion_budget = match &self.spill_config {
            Some(sc) => (sc.pool_bytes / self.num_partitions().max(1)).max(1),
            None => usize::MAX,
        };
```

Remove the now-unused `budget_per_bucket` field from `GroupedAggregateSink` and its computation in `new` (the block at lines ~421-427), and drop it from `make_state`.

- [ ] **Step 5: Build**

Run: `cargo build -p daft-local-execution`
Expected: compiles (fix any leftover references to `budget_per_bucket`).

- [ ] **Step 6: Run the Python agg spill test**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k agg tests/test_spill_to_disk.py"`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/daft-local-execution/src/sinks/grouped_aggregate.rs
git commit -m "feat(swordfish): drive grouped-aggregate spill from the shared reservation"
```

---

## Task 6: Migrate Window to the reservation trigger

**Files:**
- Modify: `src/daft-local-execution/src/sinks/window_base.rs`
- Test: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k window tests/test_spill_to_disk.py"`

**Interfaces:**
- Consumes: `reconcile_reservation`, `SpillableBuckets`, `MemoryReservation` (Task 1).
- `WindowBaseState` gains a reservation + cap; `push` calls `reconcile_reservation`; `maybe_spill` becomes "spill the largest bucket."

- [ ] **Step 1: Add reservation + cap to `WindowBaseState`**

Imports + fields:

```rust
use crate::{
    resource_manager::{MemoryReservation, SpillableBuckets, get_or_init_memory_manager,
        reconcile_reservation},
    spill::{SpillConfig, SpillStore, SpillWriter},
};
```
```rust
pub struct WindowBaseState {
    inner_states: Vec<Option<SinglePartitionWindowState>>,
    spill_dirs: Option<Vec<String>>,
    spill_writer: Option<SpillWriter>,
    reservation: MemoryReservation,
    cap: Option<u64>,
}
```

Update `make_base_state` to take `cap: Option<u64>` (drop `budget_per_bucket`) and seed `reservation: get_or_init_memory_manager().reservation()`.

- [ ] **Step 2: Replace `maybe_spill` call in `push` with reconcile**

In `push`, after appending to buckets, replace the `if let Some(dirs) = spill_dirs.as_ref() { Self::maybe_spill(...) }` block with:

```rust
        if let Some(dirs) = spill_dirs.as_ref() {
            let mut buckets = WindowBuckets {
                inner_states,
                spill_writer,
                spill_dirs: dirs.clone(),
            };
            reconcile_reservation(&mut buckets, reservation, *cap)?;
        }
        Ok(())
```
(Adjust the `let Self { .. } = self;` destructure at the top of `push` to also bind `reservation` and `cap`.)

- [ ] **Step 3: Add the `WindowBuckets` adapter and delete `maybe_spill`**

```rust
/// Adapter so `reconcile_reservation` can spill window buckets (raw rows; window fns aren't
/// decomposable). Spills the single heaviest bucket per call.
struct WindowBuckets<'a> {
    inner_states: &'a mut [Option<SinglePartitionWindowState>],
    spill_writer: &'a mut Option<SpillWriter>,
    spill_dirs: Vec<String>,
}

impl SpillableBuckets for WindowBuckets<'_> {
    fn resident_bytes(&self) -> u64 {
        self.inner_states
            .iter()
            .flatten()
            .map(|st| st.bytes as u64)
            .sum()
    }

    fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
        let num_buckets = self.inner_states.len();
        let Some(p) = (0..num_buckets)
            .filter(|&p| self.inner_states[p].as_ref().is_some_and(|st| st.bytes > 0))
            .max_by_key(|&p| self.inner_states[p].as_ref().unwrap().bytes)
        else {
            return Ok(false);
        };
        let st = self.inner_states[p].as_mut().unwrap();
        let batches = std::mem::take(&mut st.partitions);
        st.bytes = 0;
        if batches.is_empty() {
            return Ok(true);
        }
        let writer = match self.spill_writer {
            Some(w) => w,
            None => {
                let schema = batches[0].schema.clone();
                let w = SpillWriter::new(
                    num_buckets,
                    &schema,
                    self.spill_dirs.clone(),
                    "daft_window_spill_",
                )?;
                *self.spill_writer = Some(w);
                self.spill_writer.as_mut().unwrap()
            }
        };
        for b in &batches {
            writer.write_batch(p, b)?;
        }
        Ok(true)
    }
}
```

- [ ] **Step 4: Replace `window_bucket_budget` with a cap accessor**

Delete `window_bucket_budget`. Update the three window sinks (`window_partition_only.rs`, `window_partition_and_order_by.rs`, `window_partition_and_dynamic_frame.rs`) where they call `make_base_state(..., window_bucket_budget(&spill_config))`: change to pass the cap:

```rust
WindowBaseState::make_base_state(
    num_partitions,
    window_spill_dirs(&self.spill_config),
    self.spill_config.as_ref().and_then(|sc| sc.cap()),
)
```
(Search each window sink's `make_state` for the `make_base_state(` call and update it; the signature change in Step 1 forces all three.)

- [ ] **Step 5: Build**

Run: `cargo build -p daft-local-execution`
Expected: compiles.

- [ ] **Step 6: Run the Python window spill test**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k window tests/test_spill_to_disk.py"`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/daft-local-execution/src/sinks/window_base.rs src/daft-local-execution/src/sinks/window_partition_only.rs src/daft-local-execution/src/sinks/window_partition_and_order_by.rs src/daft-local-execution/src/sinks/window_partition_and_dynamic_frame.rs
git commit -m "feat(swordfish): drive window spill from the shared reservation"
```

---

## Task 7: Hash join on by default + reservation trigger (#1)

**Files:**
- Modify: `src/daft-local-execution/src/join/hash_join.rs`
- Test: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k join tests/test_spill_to_disk.py"`

**Interfaces:**
- Consumes: `reconcile_reservation`, `SpillableBuckets`, `MemoryReservation` (Task 1).
- Note: hash-join is already flipped on by default by Task 3 (Step 5). This task makes its build side spill via the shared reservation instead of the per-partition `threshold_per_partition`.

- [ ] **Step 1: Add reservation to `PartitionedBuildData`; drop `threshold_per_partition`**

Imports:

```rust
use crate::{
    ExecutionTaskSpawner,
    join::{ /* unchanged */ },
    pipeline::NodeName,
    resource_manager::{MemoryReservation, SpillableBuckets, get_or_init_memory_manager,
        reconcile_reservation},
    spill::{SpillConfig, SpillStore, SpillWriter},
};
```

Change the struct:

```rust
struct PartitionedBuildData {
    partition_count: usize,
    per_partition_tables: Vec<Vec<RecordBatch>>,
    per_partition_size_bytes: Vec<usize>,
    spill_writer: SpillWriter,
    reservation: MemoryReservation,
    cap: Option<u64>,
}
```

In `new_partitioned`, drop the `threshold_per_partition` computation and seed the reservation + cap:

```rust
        let partition_count = config.partition_count();
        let spill_writer = SpillWriter::new(
            partition_count,
            build_schema,
            config.spill_dirs.clone(),
            "daft_join_spill_",
        )?;
        Ok(Self {
            probe_table_builder: make_probeable_builder(
                key_schema.clone(),
                nulls_equal_aware,
                track_indices,
            )?,
            tables: Vec::new(),
            partitioned: Some(PartitionedBuildData {
                partition_count,
                per_partition_tables: vec![Vec::new(); partition_count],
                per_partition_size_bytes: vec![0; partition_count],
                spill_writer,
                reservation: get_or_init_memory_manager().reservation(),
                cap: config.cap(),
            }),
        })
```

- [ ] **Step 2: Replace the per-partition threshold spill in `add_tables` with reconcile**

In `add_tables`, the partitioned branch currently spills a partition inline when `per_partition_size_bytes[p] > threshold_per_partition`. Replace the whole partitioned branch body with accumulation followed by one reconcile:

```rust
        if let Some(ref mut pd) = self.partitioned {
            for table in input.record_batches() {
                let sub_batches = table.partition_by_hash(&params.build_on, pd.partition_count)?;
                for (p, sub_batch) in sub_batches.into_iter().enumerate() {
                    if sub_batch.is_empty() {
                        continue;
                    }
                    pd.per_partition_size_bytes[p] += sub_batch.size_bytes();
                    pd.per_partition_tables[p].push(sub_batch);
                }
            }
            let cap = pd.cap;
            let PartitionedBuildData {
                per_partition_tables,
                per_partition_size_bytes,
                spill_writer,
                reservation,
                ..
            } = pd;
            let mut buckets = JoinBuildBuckets {
                per_partition_tables,
                per_partition_size_bytes,
                spill_writer,
            };
            reconcile_reservation(&mut buckets, reservation, cap)?;
        } else {
            // ... unchanged in-memory branch ...
        }
```

- [ ] **Step 3: Add the `JoinBuildBuckets` adapter**

```rust
/// Adapter so `reconcile_reservation` can spill hash-join build partitions: spills the heaviest
/// in-memory partition's batches to its bucket file.
struct JoinBuildBuckets<'a> {
    per_partition_tables: &'a mut Vec<Vec<RecordBatch>>,
    per_partition_size_bytes: &'a mut Vec<usize>,
    spill_writer: &'a mut SpillWriter,
}

impl SpillableBuckets for JoinBuildBuckets<'_> {
    fn resident_bytes(&self) -> u64 {
        self.per_partition_size_bytes.iter().map(|b| *b as u64).sum()
    }

    fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
        let Some((p, _)) = self
            .per_partition_size_bytes
            .iter()
            .enumerate()
            .filter(|(_, b)| **b > 0)
            .max_by_key(|(_, b)| **b)
        else {
            return Ok(false);
        };
        let batches = std::mem::take(&mut self.per_partition_tables[p]);
        self.per_partition_size_bytes[p] = 0;
        for b in &batches {
            self.spill_writer.write_batch(p, b)?;
        }
        Ok(true)
    }
}
```

- [ ] **Step 4: Update `multiline_display`**

Replace the `sc.threshold_bytes` display (lines ~712-718) with:

```rust
        if let Some(sc) = &self.params.spill_config {
            display.push(format!(
                "Spill: pool={}B, partitions={}",
                sc.pool_bytes,
                sc.partition_count()
            ));
        }
```

- [ ] **Step 5: Build**

Run: `cargo build -p daft-local-execution`
Expected: compiles (the `finalize` flush loop at lines ~166-176 still works — it flushes remaining in-memory tables for already-spilled partitions).

- [ ] **Step 6: Verify hash-join spills by default**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k join tests/test_spill_to_disk.py"`
Expected: PASS, including any test that no longer sets `hash_join_spill_threshold_bytes` explicitly (added in Task 12).

- [ ] **Step 7: Commit**

```bash
git add src/daft-local-execution/src/join/hash_join.rs
git commit -m "feat(swordfish): hash-join spill on by default via shared reservation (#1)"
```

---

## Task 8: Repartition streaming finalize (the 🔴 fix)

**Files:**
- Modify: `src/daft-shuffles/src/oneshot_writer.rs`
- Modify: `src/daft-local-execution/src/sinks/repartition.rs`
- Test: `src/daft-shuffles/src/oneshot_writer.rs` (`#[cfg(test)]`)

**Interfaces:**
- Produces: `write_partitions_one_shot_streaming(input_id, shuffle_id, shuffle_dirs, schema, compression, num_partitions, mut next_partition) -> DaftResult<Vec<PartitionCache>>` where `next_partition: impl FnMut(usize) -> DaftResult<MicroPartition>` yields partition `p`'s full data on demand (one partition resident at a time).

- [ ] **Step 1: Write a failing test for the streaming writer**

Add to `src/daft-shuffles/src/oneshot_writer.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::prelude::{DataType, Field, Schema, UInt8Array};
    use daft_core::series::IntoSeries;
    use daft_micropartition::MicroPartition;
    use daft_recordbatch::RecordBatch;

    use super::*;

    fn mp(schema: &SchemaRef, rows: usize) -> MicroPartition {
        let s = UInt8Array::from_field_and_values(
            Field::new("v", DataType::UInt8),
            (0..rows).map(|i| i as u8),
        )
        .into_series();
        let rb = RecordBatch::new_unchecked(schema.clone(), vec![s.into()], rows);
        MicroPartition::new_loaded(schema.clone(), vec![rb].into(), None)
    }

    #[tokio::test]
    async fn test_streaming_writer_matches_row_counts() -> DaftResult<()> {
        let tmp = tempfile::tempdir().unwrap();
        let dirs = vec![tmp.path().to_str().unwrap().to_string()];
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("v", DataType::UInt8)])).into();
        let rows_per_part = [10usize, 0, 25, 7];
        let schema_cl = schema.clone();
        let caches = write_partitions_one_shot_streaming(
            0,
            0,
            &dirs,
            schema.clone(),
            None,
            rows_per_part.len(),
            move |p| Ok(mp(&schema_cl, rows_per_part[p])),
        )
        .await?;
        assert_eq!(caches.len(), 4);
        let total: usize = caches.iter().map(|c| c.num_rows).sum();
        assert_eq!(total, 42);
        // Empty partition still produces a (zero-row) cache entry.
        assert_eq!(caches[1].num_rows, 0);
        Ok(())
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p daft-shuffles --lib oneshot_writer`
Expected: FAIL — `write_partitions_one_shot_streaming` not found.

- [ ] **Step 3: Add the streaming writer**

In `oneshot_writer.rs`, add a streaming variant that closes over `next_partition`, materializing one partition at a time:

```rust
/// Streaming variant of [`write_partitions_one_shot`]: instead of receiving all partitions
/// up-front (which would hold the whole input in memory), it pulls partition `p`'s data on demand
/// via `next_partition`, so peak memory is bounded to roughly one partition.
pub async fn write_partitions_one_shot_streaming<F>(
    input_id: u32,
    shuffle_id: u64,
    shuffle_dirs: &[String],
    schema: SchemaRef,
    compression: Option<arrow_ipc::CompressionType>,
    num_partitions: usize,
    mut next_partition: F,
) -> DaftResult<Vec<PartitionCache>>
where
    F: FnMut(usize) -> DaftResult<MicroPartition> + Send + 'static,
{
    let dir_idx = (input_id as usize) % shuffle_dirs.len();
    let shuffle_dir = format!("{}/daft_shuffle/{}", shuffle_dirs[dir_idx], shuffle_id);

    get_io_runtime(true)
        .spawn_blocking(move || -> DaftResult<Vec<PartitionCache>> {
            std::fs::create_dir_all(&shuffle_dir)?;
            let file_path = format!("{}/map_{}.arrow", shuffle_dir, input_id);
            let arrow_schema = Arc::new(schema.to_arrow()?);
            let write_options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(compression)
                .map_err(|e| {
                    DaftError::InternalError(format!("IPC compression init failed: {}", e))
                })?;
            let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                CountingFile::new(File::create(&file_path)?),
                arrow_schema.as_ref(),
                write_options,
            )
            .map_err(|e| DaftError::InternalError(format!("IPC writer init failed: {}", e)))?;

            let mut caches: Vec<PartitionCache> = Vec::with_capacity(num_partitions);
            for idx in 0..num_partitions {
                let partition = next_partition(idx)?;
                caches.push(write_one_partition(
                    partition,
                    partition_ref_id(input_id, idx),
                    &mut writer,
                    &arrow_schema,
                    &schema,
                    &file_path,
                )?);
                // `partition` is dropped here before the next one is pulled.
            }

            writer.finish().map_err(|e| {
                DaftError::InternalError(format!("IPC writer finish failed: {}", e))
            })?;
            writer
                .flush()
                .map_err(|e| DaftError::InternalError(format!("IPC writer flush failed: {}", e)))?;
            Ok(caches)
        })
        .await?
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p daft-shuffles --lib oneshot_writer`
Expected: PASS.

- [ ] **Step 5: Use the streaming writer in repartition finalize**

In `src/daft-local-execution/src/sinks/repartition.rs`, in the `Flight` finalize branch, replace the "flatten everything then write" sequence (the `flatten_per_partition` + `write_partitions_one_shot` calls, lines ~343-364) with a per-partition pull. Replace `flatten_per_partition` usage in finalize with a closure that builds one partition at a time from the spill stores + in-memory buckets:

```rust
                        RepartitionBackend::Flight {
                            shuffle_id,
                            shuffle_dirs,
                            local_server,
                            shuffle_address,
                            compression,
                            ..
                        } => {
                            // Flush remaining pre-repartitioned data in a blocking thread.
                            let mut states = get_io_runtime(true)
                                .spawn_blocking(move || -> DaftResult<Vec<RepartitionAccState>> {
                                    states
                                        .iter_mut()
                                        .try_for_each(RepartitionAccState::flush_pre_partitioned)?;
                                    Ok(states)
                                })
                                .await??;

                            let input_id = states.first().map_or(0, |s| s.input_id);
                            let part_schema = schema.clone();
                            // Pull one partition at a time: read its spilled batches + take its
                            // in-memory batches across all states. Bounds finalize to ~1 partition.
                            let next_partition = move |p: usize| -> DaftResult<MicroPartition> {
                                let mut chunks: Vec<RecordBatch> = Vec::new();
                                for state in states.iter_mut() {
                                    for store in &state.spill_stores {
                                        if store.is_spilled(p) {
                                            chunks.extend(store.read_bucket(p)?);
                                        }
                                    }
                                    chunks.extend(std::mem::take(&mut state.post_repartitioned[p]));
                                }
                                Ok(MicroPartition::new_loaded(
                                    part_schema.clone(),
                                    Arc::new(chunks),
                                    None,
                                ))
                            };

                            let partition_caches = write_partitions_one_shot_streaming(
                                input_id,
                                shuffle_id,
                                &shuffle_dirs,
                                schema,
                                compression,
                                num_partitions,
                                next_partition,
                            )
                            .await?;

                            local_server
                                .register_shuffle_partitions(shuffle_id, partition_caches.clone())
                                .await?;
                            Ok(BlockingSinkOutput::FlightPartitionRefs(
                                partition_caches
                                    .into_iter()
                                    .map(|partition| FlightPartitionRef {
                                        shuffle_id,
                                        server_address: shuffle_address.clone(),
                                        partition_ref_id: partition.partition_ref_id,
                                        num_rows: partition.num_rows,
                                        size_bytes: partition.size_bytes,
                                    })
                                    .collect(),
                            ))
                        }
```

Update the import in `repartition.rs`:
```rust
use daft_shuffles::{
    oneshot_writer::write_partitions_one_shot_streaming,
    server::flight_server::ShuffleFlightServer, shuffle_cache::CHUNK_TARGET_BYTES,
};
```
(Drop the `write_partitions_one_shot` import if the Ray branch doesn't use it; the Ray branch keeps using `flatten_per_partition`, so retain that function.)

- [ ] **Step 6: Build**

Run: `cargo build -p daft-shuffles && cargo build -p daft-local-execution`
Expected: compiles.

- [ ] **Step 7: Run repartition unit tests + build**

Run: `cargo test -p daft-local-execution --lib repartition && cargo test -p daft-shuffles --lib oneshot_writer && make build`
Expected: PASS (existing repartition helper tests still green; streaming writer test green).

- [ ] **Step 8: Commit**

```bash
git add src/daft-shuffles/src/oneshot_writer.rs src/daft-local-execution/src/sinks/repartition.rs
git commit -m "fix(swordfish): stream repartition finalize per-partition to bound peak memory"
```

---

## Task 9: Repartition reservation trigger + prefix cleanup

**Files:**
- Modify: `src/daft-local-execution/src/sinks/repartition.rs`
- Modify: `src/daft-local-execution/src/pipeline.rs` (pass full `SpillConfig` instead of a bare threshold)
- Test: existing repartition unit tests + Task 12 e2e

**Interfaces:**
- Consumes: `MemoryReservation` (Task 1), `SpillConfig` (Task 3).
- Replaces `RepartitionBackend::Flight.spill_threshold: Option<usize>` with the post-repartition reservation.

- [ ] **Step 1: Thread `SpillConfig` into the Flight backend**

In `pipeline.rs` repartition site, pass the `SpillConfig` (built in Task 3) into `try_new_flight` instead of a bare `Option<usize>`. Change the `try_new_flight` signature in `repartition.rs` from `spill_threshold: Option<usize>` to `spill_config: Option<SpillConfig>`, store it in `RepartitionBackend::Flight { spill_config: Option<SpillConfig>, .. }`, and update `spill_dirs()` to read `shuffle_dirs` (unchanged) and a new `spill_config()` accessor returning `Option<SpillConfig>`.

Update the pipeline call:
```rust
                    let spill_config = build_spill_config(cfg.repartition_spill_threshold_bytes, cfg);
                    let repartition_sink = RepartitionSink::try_new_flight(
                        *num_partitions,
                        schema.clone(),
                        shuffle_id,
                        repartition_spec.clone(),
                        shuffle_dirs,
                        compression.clone(),
                        shuffle_server,
                        shuffle_address,
                        spill_config,
                    )
```

- [ ] **Step 2: Add reservation + cap to `RepartitionAccState`**

Add fields `reservation: MemoryReservation` and `cap: Option<u64>`; seed them in `RepartitionAccState::new` (extend its signature) and in `make_state` from `self.spill_config()`. Imports:

```rust
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    resource_manager::{MemoryReservation, SpillableBuckets, get_or_init_memory_manager,
        reconcile_reservation},
    spill::{SpillConfig, SpillStore, SpillWriter},
};
```

- [ ] **Step 3: Make `RepartitionAccState` implement `SpillableBuckets`; replace the threshold check in `sink`**

`spill_post_repartitioned` already spills all buckets; for reconcile we want "spill the largest bucket." Add a `spill_one_bucket(p)` that writes bucket `p`'s in-memory batches to a fresh single-bucket store and clears them. Simpler: keep one `SpillWriter` per state created lazily over `num_partitions` buckets (matching agg), append bucket `p`'s batches, and clear. Restructure `RepartitionAccState` to hold an `Option<SpillWriter>` (lazily created) instead of building a new `SpillStore` per spill round:

```rust
    fn spill_bucket(&mut self, p: usize) -> DaftResult<()> {
        let batches = std::mem::take(&mut self.post_repartitioned[p]);
        let freed: usize = batches.iter().map(|b| b.size_bytes()).sum();
        self.post_repartitioned_size_bytes -= freed.min(self.post_repartitioned_size_bytes);
        if batches.is_empty() {
            return Ok(());
        }
        let num_partitions = self.post_repartitioned.len();
        let writer = match &mut self.spill_writer {
            Some(w) => w,
            None => {
                let w = SpillWriter::new(
                    num_partitions,
                    &self.schema,
                    self.spill_dirs.clone(),
                    "daft_repartition_spill_",
                )?;
                self.spill_writer = Some(w);
                self.spill_writer.as_mut().unwrap()
            }
        };
        for b in &batches {
            if b.len() > 0 {
                writer.write_batch(p, b)?;
            }
        }
        Ok(())
    }
```

Replace the `spill_stores: Vec<SpillStore>` field with `spill_writer: Option<SpillWriter>`, and at finalize seal it into one `SpillStore` (`spill_writer.take().map(|w| w.finish()).transpose()?`). Implement the trait:

```rust
impl SpillableBuckets for RepartitionAccState {
    fn resident_bytes(&self) -> u64 {
        self.post_repartitioned_size_bytes as u64
    }
    fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
        let Some((p, _)) = self
            .post_repartitioned
            .iter()
            .enumerate()
            .map(|(p, v)| (p, v.iter().map(|b| b.size_bytes()).sum::<usize>()))
            .filter(|(_, b)| *b > 0)
            .max_by_key(|(_, b)| *b)
        else {
            return Ok(false);
        };
        self.spill_bucket(p)?;
        Ok(true)
    }
}
```

In `sink`, replace the spill block:
```rust
                    if state.spill_dirs.is_empty() {
                        return Ok(state);
                    }
                    let mut s = state;
                    let cap = s.cap;
                    let mut res = std::mem::replace(
                        &mut s.reservation,
                        get_or_init_memory_manager().reservation(),
                    );
                    reconcile_reservation(&mut s, &mut res, cap)?;
                    s.reservation = res;
                    Ok(s)
```
(The swap avoids borrowing `s.reservation` and `s` mutably at once. Equivalent: extract the reservation, reconcile, put it back.)

- [ ] **Step 4: Update `flatten_per_partition` and finalize for the single-store shape**

`flatten_per_partition` (used by the **Ray** branch only now) and the Task 8 streaming closure both read `state.spill_stores`. Switch both to read the single sealed `SpillStore`. In the Ray finalize branch, seal each state's writer first:
```rust
                        RepartitionBackend::Ray => {
                            states
                                .iter_mut()
                                .try_for_each(RepartitionAccState::flush_pre_partitioned)?;
                            // Ray path never spills (spill_dirs empty) so spill_writer is None.
                            let (per_partition, _input_id) =
                                flatten_per_partition(states, num_partitions, schema.clone())?;
                            // ... unchanged ...
                        }
```
Update `flatten_per_partition`'s spill read to use `state.spill_store` (the sealed `Option<SpillStore>`), and in Task 8's `next_partition` closure read `state.spill_store` similarly. (Seal each state's writer inside the blocking flush step before building the closure.)

- [ ] **Step 5: Update the repartition unit tests for the new shape**

The existing tests reference `state.spill_stores` and call `state.spill_post_repartitioned()`. Update them to: call `state.spill_largest_bucket()` (or a loop spilling all buckets), and assert on the sealed store via `flatten_per_partition`. Keep the row-count assertions. Example replacement for `test_spill_then_flatten_recovers_all_rows`:

```rust
    #[tokio::test]
    async fn test_spill_then_flatten_recovers_all_rows() -> DaftResult<()> {
        let tmp = tempfile::tempdir().unwrap();
        let spill_dirs = vec![tmp.path().to_str().unwrap().to_string()];
        let num_partitions = 4;
        let total_rows = 1000usize;
        let mut state = make_state(num_partitions, spill_dirs);
        push_and_flush(&mut state, &make_mp(total_rows))?;
        // Spill every bucket.
        while state.spill_largest_bucket()? {}
        assert_eq!(state.post_repartitioned_size_bytes, 0);
        let (per_partition, _) =
            flatten_per_partition(vec![state], num_partitions, test_schema())?;
        let recovered: usize = per_partition.iter().map(|mp| mp.len()).sum();
        assert_eq!(recovered, total_rows);
        Ok(())
    }
```
Update the other three tests analogously (drive spilling via `spill_largest_bucket`; assert recovered row totals). Update `make_state` test helper to pass a cap of `None` and seed the reservation.

- [ ] **Step 6: Build + test**

Run: `cargo test -p daft-local-execution --lib repartition && make build`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/daft-local-execution/src/sinks/repartition.rs src/daft-local-execution/src/pipeline.rs
git commit -m "feat(swordfish): drive repartition spill from the shared reservation; daft_ prefix"
```

---

## Task 10: Dedup spill (grace pattern) (#3)

**Files:**
- Modify: `src/daft-local-execution/src/sinks/dedup.rs`
- Modify: `src/daft-local-execution/src/pipeline.rs` (pass a `SpillConfig` into `DedupSink::new`)
- Test: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k dedup tests/test_spill_to_disk.py"` (test added in Task 12)

**Interfaces:**
- Consumes: `reconcile_reservation`, `SpillableBuckets`, `MemoryReservation` (Task 1), `SpillWriter`/`SpillStore` (existing).
- `DedupSink::new(columns, spill_config)`.

- [ ] **Step 1: Wire a `SpillConfig` into `DedupSink` from the pipeline**

In `pipeline.rs` Dedup arm (lines ~1001-1020):

```rust
            let dedup_spill = build_spill_config(cfg.agg_spill_threshold_bytes, cfg);
            let dedup_sink =
                DedupSink::new(columns, dedup_spill).with_context(|_| PipelineCreationSnafu {
                    plan_name: physical_plan.name(),
                })?;
```
(Dedup reuses the `agg_spill_threshold_bytes` opt-out — it is an aggregation-shaped operator. Document this in the kwarg docstring update in Task 2 if not already.)

- [ ] **Step 2: Extend dedup state for spilling**

Rewrite `dedup.rs` state types:

```rust
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    resource_manager::{MemoryReservation, SpillableBuckets, get_or_init_memory_manager,
        reconcile_reservation},
    spill::{SpillConfig, SpillStore, SpillWriter},
};

#[derive(Default)]
pub(crate) struct SinglePartitionDedupState {
    partially_deduped: Vec<MicroPartition>,
    bytes: usize,
}

pub(crate) enum DedupState {
    Accumulating {
        inner_states: Vec<SinglePartitionDedupState>,
        spill_dirs: Option<Vec<String>>,
        spill_writer: Option<SpillWriter>,
        reservation: MemoryReservation,
        cap: Option<u64>,
    },
    Done,
}
```

`DedupState::new(num_partitions, spill_dirs, cap)` seeds the reservation from `get_or_init_memory_manager().reservation()`.

- [ ] **Step 3: Spill on push via reconcile**

In `push`, after partitioning + per-piece dedup (existing logic), track bytes and reconcile:

```rust
    fn push(&mut self, input: MicroPartition, columns: &[BoundExpr]) -> DaftResult<()> {
        let Self::Accumulating {
            inner_states,
            spill_dirs,
            spill_writer,
            reservation,
            cap,
        } = self
        else {
            panic!("DropDuplicatesSink should be in Accumulating state");
        };

        let partitioned = input.partition_by_hash(columns, inner_states.len())?;
        for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
            let deduped = p.dedup(columns)?;
            state.bytes += deduped.size_bytes();
            state.partially_deduped.push(deduped);
        }

        if let Some(dirs) = spill_dirs.as_ref() {
            let mut buckets = DedupBuckets {
                inner_states,
                spill_writer,
                spill_dirs: dirs.clone(),
                columns,
            };
            reconcile_reservation(&mut buckets, reservation, *cap)?;
        }
        Ok(())
    }
```

- [ ] **Step 4: Add the `DedupBuckets` adapter**

```rust
/// Adapter so `reconcile_reservation` can spill dedup buckets. Dedup is idempotent, so spilled
/// partially-deduped batches are re-deduped against in-memory ones at finalize.
struct DedupBuckets<'a> {
    inner_states: &'a mut [SinglePartitionDedupState],
    spill_writer: &'a mut Option<SpillWriter>,
    spill_dirs: Vec<String>,
    columns: &'a [BoundExpr],
}

impl SpillableBuckets for DedupBuckets<'_> {
    fn resident_bytes(&self) -> u64 {
        self.inner_states.iter().map(|st| st.bytes as u64).sum()
    }

    fn spill_largest_bucket(&mut self) -> DaftResult<bool> {
        let num_buckets = self.inner_states.len();
        let Some((p, _)) = self
            .inner_states
            .iter()
            .enumerate()
            .filter(|(_, st)| st.bytes > 0)
            .max_by_key(|(_, st)| st.bytes)
        else {
            return Ok(false);
        };
        let st = &mut self.inner_states[p];
        // Compact this bucket to a single deduped MicroPartition before spilling.
        let pieces = std::mem::take(&mut st.partially_deduped);
        st.bytes = 0;
        if pieces.is_empty() {
            return Ok(true);
        }
        let compacted = MicroPartition::concat(pieces)?.dedup(self.columns)?;
        if compacted.len() == 0 {
            return Ok(true);
        }
        let writer = match self.spill_writer {
            Some(w) => w,
            None => {
                let schema = compacted.schema();
                let w = SpillWriter::new(
                    num_buckets,
                    &schema,
                    self.spill_dirs.clone(),
                    "daft_dedup_spill_",
                )?;
                *self.spill_writer = Some(w);
                self.spill_writer.as_mut().unwrap()
            }
        };
        for rb in compacted.record_batches() {
            writer.write_batch(p, rb)?;
        }
        Ok(true)
    }
}
```

- [ ] **Step 5: Re-dedup spilled + in-memory at finalize**

Rewrite `finalize` to consume each state's in-memory buckets and spill store, and for each bucket concat (in-memory + spilled) then `dedup`:

```rust
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let columns = self.columns.clone();
        let num_partitions = self.num_partitions();
        spawner
            .spawn(
                async move {
                    // Seal each state: take in-memory buckets, finish spill writer into a store.
                    let mut inners: Vec<Vec<SinglePartitionDedupState>> = Vec::new();
                    let mut stores: Vec<Option<SpillStore>> = Vec::new();
                    for state in states {
                        let DedupState::Accumulating {
                            inner_states,
                            spill_writer,
                            ..
                        } = state
                        else {
                            panic!("DropDuplicatesSink should be in Accumulating state");
                        };
                        inners.push(inner_states);
                        stores.push(match spill_writer {
                            Some(w) => Some(w.finish()?),
                            None => None,
                        });
                    }
                    let stores = Arc::new(stores);

                    let mut tasks = tokio::task::JoinSet::new();
                    for p in 0..num_partitions {
                        let mut pieces: Vec<MicroPartition> = Vec::new();
                        for inner in inners.iter_mut() {
                            if let Some(st) = inner.get_mut(p) {
                                pieces.append(&mut st.partially_deduped);
                            }
                        }
                        let stores = stores.clone();
                        let columns = columns.clone();
                        tasks.spawn(async move {
                            for store in stores.iter().flatten() {
                                if store.is_spilled(p) {
                                    let batches = store.read_bucket(p)?;
                                    if !batches.is_empty() {
                                        let schema = batches[0].schema.clone();
                                        pieces.push(MicroPartition::new_loaded(
                                            schema,
                                            Arc::new(batches),
                                            None,
                                        ));
                                    }
                                }
                            }
                            if pieces.is_empty() {
                                return Ok::<Option<MicroPartition>, DaftError>(None);
                            }
                            Ok(Some(MicroPartition::concat(pieces)?.dedup(&columns)?))
                        });
                    }
                    let mut results = vec![];
                    while let Some(res) = tasks.join_next().await {
                        if let Some(mp) =
                            res.map_err(|e| DaftError::InternalError(e.to_string()))??
                        {
                            results.push(mp);
                        }
                    }
                    Ok(BlockingSinkOutput::Partitions(results))
                },
                Span::current(),
            )
            .into()
    }
```

Add `use common_error::DaftError;` if not present. Update `DedupSink::new` to accept and store `spill_config: Option<SpillConfig>`, and `make_state` to build `DedupState::new(self.num_partitions(), self.spill_config.as_ref().map(|sc| sc.spill_dirs.clone()), self.spill_config.as_ref().and_then(|sc| sc.cap()))`. Extend `multiline_display` to push `"Spill: enabled (grace dedup)"` when `spill_config.is_some()`.

- [ ] **Step 6: Build**

Run: `cargo build -p daft-local-execution && make build`
Expected: compiles.

- [ ] **Step 7: Correctness smoke test**

Run: `DAFT_RUNNER=native uv run python -c "import daft; daft.context.set_execution_config(spill_pool_bytes=1); df=daft.from_pydict({'a':[1,1,2,2,3]*1000}).distinct().sort('a'); print(df.to_pydict()['a'])"`
Expected: prints `[1, 2, 3]` (spilling forced by a 1-byte pool, result still correct).

- [ ] **Step 8: Commit**

```bash
git add src/daft-local-execution/src/sinks/dedup.rs src/daft-local-execution/src/pipeline.rs
git commit -m "feat(swordfish): add grace-style spill to Dedup (#3)"
```

---

## Task 11: Observability + remove deprecated `threshold_bytes`

**Files:**
- Modify: `src/daft-local-execution/src/spill/mod.rs` (drop `threshold_bytes`)
- Modify: operator files referencing `threshold_bytes` (none should remain after Tasks 4–10; verify)
- Modify: `src/daft-local-execution/src/resource_manager.rs` (pool usage accessor for metrics)
- Modify: per-operator `multiline_display` already updated; add spill counters to runtime stats if a lightweight hook exists

**Interfaces:**
- Produces: `MemoryManager::spill_used_bytes(&self) -> u64` for diagnostics.

- [ ] **Step 1: Add a spill-usage accessor + test**

In `resource_manager.rs`:

```rust
    /// Bytes of the spill pool currently held across all live reservations (diagnostics).
    pub fn spill_used_bytes(&self) -> u64 {
        self.state.lock().unwrap().spill_used_bytes
    }
```
Add a test:
```rust
    #[test]
    fn test_spill_used_bytes_tracks_reservations() {
        let manager = Arc::new(MemoryManager::new());
        manager.set_spill_pool_bytes(1000);
        let mut r = manager.reservation();
        assert_eq!(manager.spill_used_bytes(), 0);
        assert!(r.try_grow(300));
        assert_eq!(manager.spill_used_bytes(), 300);
        drop(r);
        assert_eq!(manager.spill_used_bytes(), 0);
    }
```
Run: `cargo test -p daft-local-execution --lib resource_manager::tests::test_spill_used_bytes_tracks_reservations`
Expected: PASS.

- [ ] **Step 2: Remove `threshold_bytes` from `SpillConfig`**

Grep first: `grep -rn "threshold_bytes" src/daft-local-execution/src`. Expected: only `spill/mod.rs` (the field) and `build_spill_config` (which sets it). Remove the field from the struct, drop the `threshold_bytes: pool_bytes` line from `SpillConfig::new`, and remove the temporary `let spill_threshold = ...threshold_bytes` line in `pipeline.rs` (Task 3 Step 5) if any remains. Remove `auto_spill_threshold` if now unused (grep to confirm).

- [ ] **Step 3: Build**

Run: `cargo build -p daft-local-execution`
Expected: compiles. If a `threshold_bytes` reference remains, the build fails — fix it in the offending operator file (it means that operator wasn't fully migrated).

- [ ] **Step 4: Commit**

```bash
git add src/daft-local-execution/src/resource_manager.rs src/daft-local-execution/src/spill/mod.rs src/daft-local-execution/src/pipeline.rs
git commit -m "refactor(swordfish): remove legacy per-operator spill threshold; add pool usage metric"
```

---

## Task 12: End-to-end spill tests

**Files:**
- Modify: `tests/test_spill_to_disk.py`
- Test: the file itself

**Interfaces:**
- Consumes: `daft.context.set_execution_config(spill_pool_bytes=..., {op}_spill_threshold_bytes=...)`.

- [ ] **Step 1: Add a helper context manager (if not already present) and read existing patterns**

Read `tests/test_spill_to_disk.py` to reuse its existing fixtures/markers (it already has `pytestmark` skipping Ray). Add tests using `daft.context.execution_config_ctx(spill_pool_bytes=<small>)` to force spilling.

- [ ] **Step 2: Write the hash-join-default test**

```python
def test_hash_join_spills_by_default():
    # No explicit hash_join_spill_threshold_bytes — must spill via the shared pool.
    with daft.context.execution_config_ctx(spill_pool_bytes=1 << 20):  # 1 MiB pool
        left = daft.from_pydict({"k": list(range(20000)), "v": list(range(20000))})
        right = daft.from_pydict({"k": list(range(20000)), "w": list(range(20000))})
        out = left.join(right, on="k").sort("k").to_pydict()
        assert out["k"] == list(range(20000))
```

- [ ] **Step 3: Write the dedup spill test**

```python
def test_dedup_spills_and_matches_in_memory():
    data = {"a": ([i % 5000 for i in range(50000)])}
    expected = sorted(set(data["a"]))
    with daft.context.execution_config_ctx(spill_pool_bytes=1 << 20):
        got = daft.from_pydict(data).distinct().sort("a").to_pydict()["a"]
    assert got == expected
```

- [ ] **Step 4: Write the repartition streaming-finalize test**

```python
def test_repartition_completes_under_tiny_pool():
    # Large-ish input, tiny pool: must spill during accumulation AND stream at finalize.
    n = 200000
    with daft.context.execution_config_ctx(spill_pool_bytes=1 << 20):
        df = daft.from_pydict({"k": list(range(n)), "v": list(range(n))})
        out = df.repartition(16, "k").sort("k").to_pydict()
    assert out["k"] == list(range(n))
```

- [ ] **Step 5: Write the global-pool multi-operator test**

```python
def test_two_spilling_operators_share_one_pool():
    # An aggregation feeding a sort, both under one small pool — must stay correct.
    n = 100000
    with daft.context.execution_config_ctx(spill_pool_bytes=1 << 20):
        df = daft.from_pydict({"g": [i % 1000 for i in range(n)], "v": list(range(n))})
        out = df.groupby("g").sum("v").sort("g").to_pydict()
    assert out["g"] == list(range(1000))
    # sum of v for group g = sum of i where i % 1000 == g
    expected = [sum(i for i in range(n) if i % 1000 == g) for g in range(1000)]
    assert out["v"] == expected
```

- [ ] **Step 6: Write the spill-on-vs-off equivalence test**

```python
import pytest

@pytest.mark.parametrize("op", ["sort", "agg", "dedup"])
def test_spill_matches_no_spill(op):
    n = 30000
    base = daft.from_pydict({"g": [i % 777 for i in range(n)], "v": list(range(n))})

    def run(df):
        if op == "sort":
            return df.sort("v").to_pydict()
        if op == "agg":
            return df.groupby("g").sum("v").sort("g").to_pydict()
        return df.select("g").distinct().sort("g").to_pydict()

    with daft.context.execution_config_ctx(
        sort_spill_threshold_bytes=0,
        agg_spill_threshold_bytes=0,
    ):
        no_spill = run(base)
    with daft.context.execution_config_ctx(spill_pool_bytes=1 << 20):
        spilled = run(base)
    assert no_spill == spilled
```

- [ ] **Step 7: Run the new tests**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/test_spill_to_disk.py"`
Expected: PASS (all new + existing).

- [ ] **Step 8: Commit**

```bash
git add tests/test_spill_to_disk.py
git commit -m "test(swordfish): e2e spill — hash-join default, dedup, repartition, shared pool"
```

---

## Self-review notes

- **Spec coverage:** Section 1 → Task 1; Section 2 shared change → Tasks 3–9; per-operator (sort/agg/window/hash-join/repartition) → Tasks 4–9; repartition streaming finalize 🔴 → Task 8; divisor 🟠 → subsumed by Task 3/9 (no per-state constant); prefix 🟡 → Task 9; Section 3 Dedup → Task 10; Section 4 config → Task 2, opt-out/cap → Task 3, observability → Tasks 7/11 + `multiline_display` per operator, error handling (RAII release) → Task 1; Section 5 testing → Tasks throughout + Task 12. All covered.
- **Oversized-bucket guard for dedup:** the spec's recursive sub-partition for a single oversized dedup bucket at finalize is **not** separately implemented; dedup finalize concats+dedups one bucket. This matches today's dedup finalize behavior (no recursion) and the agg recursion is the precedent if needed later. Flagged as a known limitation rather than a silent gap — if a single dedup bucket exceeds memory at finalize it can still OOM, same as today's grouped-agg non-decomposable path. Acceptable for this plan; revisit if it bites.
- **Build-green invariant:** `threshold_bytes` is retained through Tasks 3–10 and removed only in Task 11, so each task compiles independently.
- **Type consistency:** `reconcile_reservation(state, reservation, cap)`, `SpillableBuckets::{resident_bytes, spill_largest_bucket}`, `MemoryReservation::{try_grow, shrink, held}`, `MemoryManager::{reservation, spill_pool_bytes, set_spill_pool_bytes, spill_used_bytes}`, `SpillConfig::{pool_bytes, cap, partition_count}`, `write_partitions_one_shot_streaming(..)` are used consistently across tasks.
