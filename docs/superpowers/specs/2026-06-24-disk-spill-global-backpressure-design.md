# Disk Spill: Global Backpressure + Hash-Join Default + Dedup Spill

- **Date:** 2026-06-24
- **Status:** Approved design, ready for implementation planning
- **Scope:** `src/daft-local-execution` (swordfish native engine), `src/common/daft-config`, `src/daft-shuffles`

## Summary

Daft's native execution engine spills four blocking operators to disk (sort, grouped aggregation,
window, hash join), and a recent commit added a fifth (repartition / Flight shuffle). Each operator
decides *when* to spill by comparing **its own** resident bytes against a **static per-operator
threshold** (≈30% of RAM), blind to what every other operator is holding. The `MemoryManager`
semaphore exists but is only read for `total_bytes()` — nobody charges memory against it for
resident state.

This design replaces the per-operator static-threshold model with **one shared spill pool** that all
spill-capable operators draw from, where **memory pressure** — not a per-operator constant — triggers
spilling. It then:

1. **Turns hash-join spill on by default** by making the build side a normal participant in the
   shared pool (no separate enable flag).
2. **Wires the `MemoryManager` into real global backpressure** via a resident-reservation handle and
   a reserve-or-spill loop.
3. **Adds spilling to the Dedup (Distinct) operator**, which today accumulates unbounded in memory.

It also folds in two fixes for the recently-committed repartition spill (`90bfb0219`): a
**streaming finalize** (the committed version re-materializes the whole dataset at finalize, so it
does not actually lower peak memory) and a **concurrency-aware budget** (subsumed by the shared pool).

## Goals

- A single, shared spill budget governs all spill-capable operators, so total in-memory state across
  a plan is bounded regardless of how many operators spill concurrently.
- Spilling is triggered by true global memory pressure, not per-operator constants.
- Hash-join spilling is enabled by default.
- Dedup spills to disk under memory pressure.
- Repartition spill genuinely lowers peak memory (streaming finalize), not just the accumulation phase.
- No regression in results: spill-on and spill-off produce identical output.

## Non-goals

- Making **non-spillable** operators (pivot, cross-join, nested-loop join, global aggregate) block
  and backpressure on the pool. This reintroduces real deadlock risk (all operators wait, none can
  free) and needs a minimum-guarantee scheme. They keep today's behavior — no worse than now. A
  possible follow-on, explicitly out of scope here.
- A central pressure coordinator with a spiller registry and victim selection (considered as
  "Approach B" — more machinery and higher liveness risk than warranted now). The chosen design has
  a clean evolution path to it if fairness becomes a problem.
- Ray object-store spilling. Daft uses Ray purely as a task scheduler; all spilling is native-engine.

## Decisions (resolved during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Sequencing | **Foundation first** | #1 and #3 should plug into the global pool, not be wired against static thresholds we are about to remove. |
| Backpressure mechanism | **Reserve-or-spill (Approach A)** | Smallest correct step that makes spilling global and pressure-driven; reuses the existing semaphore and spill actions; safest on deadlock. |
| Operator for #3 | **Dedup (Distinct)** | Already hash-partitions by key; dedup is idempotent, so it is structurally a decomposable aggregation. Highest OOM risk, lowest implementation risk. |
| Repartition findings | **Fold into the plan** | Streaming-finalize and divisor fixes become part of foundation-first implementation. |

## Current state (as analyzed)

Spill infrastructure lives in `src/daft-local-execution/src/spill/mod.rs`, exposing two write shapes
over a common Arrow IPC stream-file primitive:

- `SpillWriter` / `SpillStore`: a fixed set of `N` hash buckets, one file each, read back
  whole-bucket. Used by hash join, grouped aggregation, window, repartition.
- `SpillRunWriter` / `SpilledRun` / `SpillRunReader`: a single sorted run, read back incrementally.
  Used by the external merge sort.

Files are written to round-robined `flight_shuffle_dirs` (default `["/tmp"]`) and deleted when the
owning handle is dropped.

Triggering today (`src/daft-local-execution/src/pipeline.rs:445-458`, `auto_spill_threshold`):
`None` → `max(64 MiB, 0.3 × total / divisor)`; `Some(0)` → disabled; `Some(n)` → explicit. Each
operator divides its budget across buckets internally and checks per-bucket/per-buffer. The
`MemoryManager` (`resource_manager.rs`) is a semaphore with `request_bytes()` → RAII permit, but the
permit is held only for a **task's duration** (`ExecutionTaskSpawner::spawn_with_memory_request`,
used only by UDF and actor-pool-project). There is no **resident** reservation for accumulated state.

## Architecture (Section 1): shared pool + resident reservations

### Shared spill pool

Repurpose `MemoryManager`'s budget as the single shared spill pool:
`spill_pool_bytes ≈ 0.3 × (DAFT_MEMORY_LIMIT or system RAM)`, configurable. Every spill-capable
operator, across all of its concurrency slots, draws from this one pool. This converts the 30% from a
*per-operator* bound (which N operators each claim → up to N×30% commit → OOM) into a *system* bound.

### `MemoryReservation` handle (new)

A handle obtained from `MemoryManager`, held inside each operator's `State` for the lifetime of its
resident data (distinct from the existing transient `MemoryPermit`):

- `try_grow(bytes) -> bool` — non-blocking charge against the pool; `false` means the pool is
  exhausted (denial). Backed by the manager's existing `available_bytes` accounting plus a
  non-blocking variant of `try_request_bytes`.
- `shrink(bytes)` — return bytes to the pool after spilling.
- `Drop` — release everything at finalize or on error (RAII; bytes must never leak).

### Reserve-or-spill loop

Replaces every per-bucket static-threshold check. On each `sink()`, after partitioning the morsel:

```
added = bytes of new rows
if reservation.try_grow(added):     # pool had room -> keep in RAM
    buffer in memory
else:                               # pool full -> relieve pressure
    spill this state's largest bucket(s) to disk (sticky)
    reservation.shrink(spilled_bytes)
    retry try_grow
    # if still denied and nothing left to spill -> proceed anyway (bounded overshoot)
```

### Deadlock-safety invariant (critical)

A spill-capable operator **never blocks**:

- Denied **with** resident data → spill its own buckets (always frees its *own* memory).
- Denied **with everything already spilled** → process the one morsel straight to disk, accepting a
  **bounded** overshoot of a single morsel (default 128K rows).

So every spilling operator is always live → no deadlock, no thrash (spill is sticky: once a bucket is
on disk it stays on disk, as grace-agg already does).

## Operator integration (Section 2)

### Shared change

`SpillConfig` loses `threshold_bytes` (the private per-operator budget) and keeps `spill_dirs` plus
an "enabled" marker. Each operator's `State` gains a `MemoryReservation` drawn from the global pool.
The spill **action** is unchanged; only the **trigger** moves from "bucket bytes > static budget" to
"`try_grow` denied," and the **victim** becomes "this state's largest resident bucket." All
per-operator `budget_per_bucket` / `threshold/(num_partitions²)` computations are deleted.

### Five operators

- **Grouped aggregate** (`sinks/grouped_aggregate.rs`): `maybe_spill` becomes "on denial, spill the
  largest bucket (compacting decomposable aggs first, as today), `shrink`, retry." Finalize keeps its
  recursion (`combine_decomposable_partials`); its `recursion_budget`, currently `budget_per_bucket`,
  is re-sourced from the pool size.
- **Window** (`sinks/window_*.rs`, `window_base.rs`): same pattern; non-decomposable, so it spills
  raw buckets (behavior unchanged, new trigger).
- **Sort** (`sinks/sort.rs`): single buffer (not N buckets); `max_concurrency = 1`. Accumulate,
  `try_grow`; on denial sort the buffer into a run, spill it, `shrink`. Same external merge sort,
  denial-driven trigger.
- **Hash join (#1)** (`join/hash_join.rs`): the build side joins the shared reservation path and
  **spills by default** — no separate enable flag. Today's `partition_count` derives from the
  (now-deleted) per-operator threshold (`spill/mod.rs:45-50`); re-derive it from `spill_pool_bytes`
  using the same 256 MiB-per-partition formula. `hash_join_spill_threshold_bytes` becomes an opt-out
  (`Some(0)` disables), consistent with the others. **Net for #1:** "on by default" stops being a
  magic-constant flip and becomes "hash join participates in the shared pool like every other
  spilling operator."
- **Repartition** (`sinks/repartition.rs`, Flight backend): migrates onto the shared reservation,
  which removes its concurrency-blind per-state threshold (see finding 🟠 below). **Plus the
  streaming-finalize fix (🔴), which is a prerequisite** — see next subsection.

### Repartition streaming finalize (required, finding 🔴)

The committed repartition spill bounds the **accumulation phase** but `finalize` calls
`flatten_per_partition`, which reads **every** spilled bucket back into a `Vec<MicroPartition>`
holding the whole input, then hands it to `write_partitions_one_shot` (which takes a fully
materialized `Vec`). Peak memory therefore lands at finalize and equals the full per-input dataset —
**the same peak as the no-spill version**. As written, the spill adds I/O without lowering the peak.

Fix: make finalize **stream partition-by-partition** — for each bucket `p`, read its spilled batches,
write that one partition to the combined IPC file, free it, advance. `write_one_partition`
(`oneshot_writer.rs:122-172`) already writes partitions sequentially into the file, so this means
feeding `write_partitions_one_shot` a per-partition lazy source instead of a materialized `Vec`. This
bounds finalize to ~one partition's worth, which is the actual goal. Without this, putting repartition
on the global pool still OOMs at finalize.

### Repartition budget (finding 🟠, subsumed)

The committed version calls `auto_spill_threshold(..., 1)` and applies the result as a **per-state**
threshold, but repartition does not override `max_concurrency`, so N states each accumulate up to
≈0.3×RAM → aggregate peak ≈ N×0.3×RAM. Under the shared pool this disappears (no per-state constant —
all states draw from one pool). Until migration, the standalone fix is to divide the threshold by
`get_compute_pool_num_threads()`.

### Cleanup (finding 🟡)

Repartition's spill-file prefix is `"repartition"`; align it with the `"daft_*_spill_"` convention as
part of the migration.

## Dedup spilling (Section 3, #3)

Dedup is idempotent: `dedup(dedup(A) ∪ dedup(B)) = dedup(A ∪ B)`, making it structurally a
decomposable aggregation. It already hash-partitions by the dedup columns into `num_partitions`
buckets and accumulates `partially_deduped` MicroPartitions per bucket (`sinks/dedup.rs:54-66`); it
simply never spills. Changes mirror grace aggregation:

- `SinglePartitionDedupState` gains a resident-bytes counter.
- `DedupState::Accumulating` gains `spill_dirs: Option<Vec<String>>`, a lazily-created `SpillWriter`
  (one IPC file per bucket, prefix `"daft_dedup_spill_"`), and a `MemoryReservation`.
- `push`: unchanged partition-then-dedup, then `try_grow` by added bytes. On denial → spill this
  state's largest bucket's `partially_deduped` batches, clear them, `shrink`, retry (sticky).
- `finalize`: for each bucket `p`, gather in-memory `partially_deduped` from all states **plus** each
  state's spilled batches for bucket `p` (`SpillStore::read_bucket`), concat, re-`dedup`. Mirrors
  `finalize_bucket` in grouped aggregation.
- **Oversized-bucket guard:** if one bucket's combined data exceeds the pool at finalize, recursively
  sub-partition by the dedup columns (sub-buckets hold disjoint keys, so concat-after-dedup is exact)
  — the same bounded-recursion trick as `combine_decomposable_partials`.

This reuses the existing `SpillWriter` / `SpillStore` N-bucket primitive verbatim and proves the
Section 1 reservation mechanism generalizes to a fresh operator.

## Config, observability, error handling (Section 4)

### Config

- New `spill_pool_bytes: Option<usize>` (env `DAFT_SPILL_POOL_BYTES`). `None` → derive
  `0.3 × (DAFT_MEMORY_LIMIT or system RAM)`. This single value is the shared pool, replacing every
  per-operator auto-derivation.
- Existing `{sort,agg,window,hash_join,repartition}_spill_threshold_bytes` degrade to **opt-outs**:
  `Some(0)` disables spilling for that operator (preserving today's escape hatch). Positive values
  are honored as a per-operator **cap** but log a one-time deprecation note (avoid two competing
  sizing knobs). Hash-join's old `None`-means-disabled special case is removed.

### Observability

- Add `bytes_spilled` and `spill_count` to per-node runtime stats.
- Add a global gauge for pool usage / denial count via the existing `Meter`.
- Extend each operator's `multiline_display` to advertise spill participation (grouped-agg already
  does at `grouped_aggregate.rs:596-598`).

### Error handling / invariants

- `MemoryReservation` releases on `Drop` on **every** path, including task errors mid-`sink()`.
- `try_grow` returns `false` (not error) on insufficient memory → triggers spill. The only hard rule:
  if a **single morsel** exceeds the entire pool, allow the overshoot rather than failing (one morsel
  must always be processable — the deadlock-safety invariant).
- Spill I/O failures (disk full) propagate as `DaftError::IoError` and fail the query — unchanged.

## Testing (Section 5)

### Rust unit

- `MemoryReservation` grow/shrink/drop accounting (including release on error/drop).
- Reserve-or-spill fires a spill on denial; a fully-spilled operator proceeds (no deadlock).
- Repartition streaming finalize writes correct per-partition byte ranges.

### Python end-to-end

Extend `tests/test_spill_to_disk.py` (native-only; Ray skipped, as today):

- Hash-join spills **by default** (no explicit threshold set).
- Dedup spills and returns results identical to in-memory.
- Repartition: a large input under a small `DAFT_MEMORY_LIMIT` that the current code OOMs on →
  completes with correct row counts and partitioning (proves streaming finalize).
- **Global-pool win:** a plan with two spilling operators under a small pool stays bounded and
  correct — the scenario today's per-operator 30%×N over-commit fails.
- Parametrized correctness: every operator, spill-on vs `Some(0)` spill-off → identical results.

## Implementation phasing

1. **Foundation:** `MemoryReservation` + non-blocking `try_grow` on `MemoryManager`; `spill_pool_bytes`
   config; reserve-or-spill helper. Migrate sort/agg/window onto it (already spill-capable). Verify no
   regressions and that aggregate memory across operators is bounded.
2. **#1 Hash join:** migrate build side onto the reservation; remove the disabled-by-default special
   case; re-derive `partition_count` from the pool.
3. **Repartition:** streaming finalize (🔴), then migrate onto the reservation (🟠 subsumed), prefix
   cleanup (🟡).
4. **#3 Dedup:** add grace-style spill mirroring grouped aggregation.
5. **Config/observability cleanup + deprecations; tests throughout.**

## Risks / open questions

- **Fairness:** a denied operator can only spill its *own* memory; if a different operator is the
  hog, the small one may spill uselessly then overshoot. Acceptable for now; the evolution path is
  Approach B (central coordinator) if real workloads show thrash.
- **Pool fraction (0.3):** inherited from the current per-operator default. May need tuning now that
  it is a single system-wide budget rather than per-operator; exposed via `spill_pool_bytes`.
- **Deprecation of positive per-operator thresholds:** confirm no internal callers rely on positive
  values for sizing; the cap semantics are a compatibility shim, not a long-term knob.
