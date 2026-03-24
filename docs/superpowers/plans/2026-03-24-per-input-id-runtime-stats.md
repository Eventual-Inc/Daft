# Per-Input-ID Runtime Stats Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Isolate runtime stats per input_id so `try_finish(input_id)` returns only that input_id's stats, then resets them.

**Architecture:** Nodes create a new `RuntimeStats` per `(node_id, input_id)` and register it with the stats manager via message. The stats manager replaces its global `node_map` with a `(NodeID, InputId) -> Arc<dyn RuntimeStats>` map. `TakeInputSnapshot` extracts and removes stats for a specific input_id. Periodic ticks aggregate across all input_ids per node.

**Tech Stack:** Rust, tokio channels, atomic counters

**Base branch:** `colin/plan-caching` (fetch with `git fetch origin colin/plan-caching`)

**Spec:** `docs/superpowers/specs/2026-03-24-per-input-id-runtime-stats-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/daft-local-execution/src/runtime_stats/mod.rs` | Modify | Stats manager: new message types, `input_stats` map, remove global `node_map` RuntimeStats |
| `src/daft-local-execution/src/runtime_stats/values.rs` | No change | RuntimeStats trait + DefaultRuntimeStats unchanged |
| `src/daft-local-execution/src/pipeline.rs` | Modify | Remove `runtime_stats()` from PipelineNode trait |
| `src/daft-local-execution/src/run.rs` | Modify | `try_finish` uses `take_input_snapshot` instead of `request_snapshot` |
| `src/daft-local-execution/src/sources/source.rs` | Modify | Create per-input_id stats, register with stats manager |
| `src/daft-local-execution/src/intermediate_ops/intermediate_op.rs` | Modify | Create per-input_id stats, register with stats manager |
| `src/daft-local-execution/src/sinks/blocking_sink.rs` | Modify | Create per-input_id stats, register with stats manager |
| `src/daft-local-execution/src/streaming_sink/base.rs` | Modify | Create per-input_id stats, register with stats manager |
| `src/daft-local-execution/src/join/join_node.rs` | Modify | Create per-input_id stats, register with stats manager |
| `src/daft-local-execution/src/join/build.rs` | Modify | Use per-input_id stats |
| `src/daft-local-execution/src/join/probe.rs` | Modify | Use per-input_id stats |
| `src/daft-local-execution/src/concat.rs` | Modify | Create per-input_id stats, register with stats manager |

---

### Task 1: Add new message types and restructure stats manager state

**Files:**
- Modify: `src/daft-local-execution/src/runtime_stats/mod.rs`
- Modify: `src/daft-local-execution/src/pipeline.rs`

This task changes the stats manager to track per-input_id stats instead of global per-node stats. The `PipelineNode::runtime_stats()` method is removed from the trait since nodes no longer have a single global RuntimeStats.

- [ ] **Step 1: Add new message variants and InputId import**

In `src/daft-local-execution/src/runtime_stats/mod.rs`, add `InputId` to imports and add new variants to `StatsManagerMessage`:

```rust
use crate::pipeline_message::InputId;

pub enum StatsManagerMessage {
    NodeEvent(usize, bool),
    SnapshotRequest(oneshot::Sender<ExecutionEngineFinalResult>),
    RegisterInputStats(NodeID, InputId, Arc<dyn RuntimeStats>),
    TakeInputSnapshot(InputId, oneshot::Sender<ExecutionEngineFinalResult>),
}
```

- [ ] **Step 2: Add `register_input_stats` and `take_input_snapshot` to RuntimeStatsManagerHandle**

```rust
impl RuntimeStatsManagerHandle {
    // ... existing methods ...

    pub fn register_input_stats(
        &self,
        node_id: NodeID,
        input_id: InputId,
        stats: Arc<dyn RuntimeStats>,
    ) {
        if let Err(e) = self.0.send(StatsManagerMessage::RegisterInputStats(node_id, input_id, stats)) {
            log::warn!(
                "Unable to register input stats for node {node_id}, input {input_id}: {e}"
            );
        }
    }

    pub async fn take_input_snapshot(
        &self,
        input_id: InputId,
    ) -> DaftResult<ExecutionEngineFinalResult> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(StatsManagerMessage::TakeInputSnapshot(input_id, tx))
            .map_err(|_| {
                common_error::DaftError::InternalError(
                    "RuntimeStatsManager was already finished; cannot take input snapshot".to_string(),
                )
            })?;
        rx.await.map_err(|_| {
            common_error::DaftError::InternalError(
                "RuntimeStatsManager dropped before sending input snapshot response".to_string(),
            )
        })
    }
}
```

- [ ] **Step 3: Restructure `try_new` — remove RuntimeStats collection from pipeline, keep only NodeInfo**

Change `try_new` to only collect `node_info_map: HashMap<NodeID, Arc<NodeInfo>>` from the pipeline tree walk. Remove the `node_map` that stores `(NodeInfo, RuntimeStats)` pairs. The pipeline tree walk becomes:

```rust
let mut node_info_map = HashMap::new();
let _ = pipeline.apply(|node| {
    let node_info = node.node_info();
    node_info_map.insert(node_info.id, Arc::new(node_info));
    Ok(TreeNodeRecursion::Continue)
});
```

Pass `node_info_map` into `new_impl` instead of `node_map`.

- [ ] **Step 4: Restructure `new_impl` event loop — replace `node_map` with `node_info_map` + `input_stats`**

Change `new_impl` signature: accept `node_info_map: HashMap<NodeID, Arc<NodeInfo>>` instead of `node_map: HashMap<NodeID, (Arc<NodeInfo>, Arc<dyn RuntimeStats>)>`.

Add `input_stats: HashMap<(NodeID, InputId), Arc<dyn RuntimeStats>>` as local state in the event loop.

Handle new message variants in the event loop:

```rust
StatsManagerMessage::RegisterInputStats(node_id, input_id, stats) => {
    input_stats.insert((node_id, input_id), stats);
}
StatsManagerMessage::TakeInputSnapshot(input_id, respond_tx) => {
    let mut snapshot = Vec::new();
    let keys_to_remove: Vec<_> = input_stats.keys()
        .filter(|(_, iid)| *iid == input_id)
        .copied()
        .collect();
    for key in keys_to_remove {
        if let Some(stats) = input_stats.remove(&key) {
            if let Some(node_info) = node_info_map.get(&key.0) {
                snapshot.push((node_info.clone(), stats.flush()));
            }
        }
    }
    let _ = respond_tx.send(ExecutionEngineFinalResult::new(snapshot));
}
```

Update the existing `SnapshotRequest` handler to aggregate from `input_stats` using `StatSnapshot::add` (defined in Step 5):

```rust
StatsManagerMessage::SnapshotRequest(respond_tx) => {
    let mut per_node: HashMap<NodeID, StatSnapshot> = HashMap::new();
    for ((node_id, _), stats) in &input_stats {
        let snap = stats.snapshot();
        per_node.entry(*node_id)
            .and_modify(|existing| existing.add(&snap))
            .or_insert(snap);
    }
    let mut snapshot = Vec::new();
    for (node_id, combined) in per_node {
        if let Some(node_info) = node_info_map.get(&node_id) {
            snapshot.push((node_info.clone(), combined));
        }
    }
    let _ = respond_tx.send(ExecutionEngineFinalResult::new(snapshot));
}
```

Update the periodic tick to use `input_stats`:

```rust
_ = interval.tick() => {
    // ... existing process stats code ...

    if input_stats.is_empty() {
        continue;
    }

    // Aggregate per node_id across input_ids
    let mut per_node: HashMap<NodeID, StatSnapshot> = HashMap::new();
    for ((node_id, _), stats) in &input_stats {
        let snap = stats.snapshot();
        per_node.entry(*node_id)
            .and_modify(|existing| existing.add(&snap))
            .or_insert(snap);
    }

    for (node_id, snapshot) in &per_node {
        if let Some(progress_bar) = &progress_bar {
            progress_bar.handle_event(*node_id, snapshot);
        }
        snapshot_container.push((*node_id, snapshot.to_stats()));
    }

    // ... existing subscriber notification code ...
}
```

Update the final snapshot collection (after the loop) to use `input_stats`:

```rust
let mut final_snapshot = Vec::new();
for ((node_id, _input_id), runtime_stats) in &input_stats {
    if let Some(node_info) = node_info_map.get(node_id) {
        let event = runtime_stats.flush();
        final_snapshot.push((node_info.clone(), event));
    }
}
// Aggregate duplicates per node_id
let mut aggregated: HashMap<NodeID, (Arc<NodeInfo>, StatSnapshot)> = HashMap::new();
for (node_info, snapshot) in final_snapshot {
    aggregated.entry(node_info.id)
        .and_modify(|(_, existing)| existing.add(&snapshot))
        .or_insert((node_info, snapshot));
}
let final_nodes: Vec<_> = aggregated.into_values().collect();
ExecutionEngineFinalResult::new(final_nodes)
```

- [ ] **Step 5: Add `StatSnapshot::add` method for aggregation**

In `src/common/metrics/src/snapshot.rs`, add `add` to `StatSnapshotImpl` trait and implement for all variants. Derived fields (`selectivity`, `amplification`) are recomputed from summed base fields:

```rust
// In the trait:
pub trait StatSnapshotImpl: Send + Sync {
    fn to_stats(&self) -> Stats;
    fn to_message(&self) -> String;
    fn add_assign(&mut self, other: &Self);
}

// Then on the enum:
impl StatSnapshot {
    pub fn add(&mut self, other: &StatSnapshot) {
        match (self, other) {
            (StatSnapshot::Default(a), StatSnapshot::Default(b)) => a.add_assign(b),
            (StatSnapshot::Source(a), StatSnapshot::Source(b)) => a.add_assign(b),
            (StatSnapshot::Filter(a), StatSnapshot::Filter(b)) => a.add_assign(b),
            (StatSnapshot::Explode(a), StatSnapshot::Explode(b)) => a.add_assign(b),
            (StatSnapshot::Udf(a), StatSnapshot::Udf(b)) => a.add_assign(b),
            (StatSnapshot::HashJoinBuild(a), StatSnapshot::HashJoinBuild(b)) => a.add_assign(b),
            (StatSnapshot::Write(a), StatSnapshot::Write(b)) => a.add_assign(b),
            _ => {} // Mismatched variants — same node always produces same variant
        }
    }
}
```

Each `add_assign` sums atomic fields. For `FilterSnapshot`: sum `cpu_us`, `rows_in`, `rows_out`, recompute `selectivity = rows_out as f64 / rows_in as f64`. For `ExplodeSnapshot`: same pattern with `amplification = rows_out as f64 / rows_in as f64`. For `UdfSnapshot`: sum base fields, merge `custom_counters` by summing values for matching keys.

- [ ] **Step 6: Update NodeEvent handling — no longer flush from node_map**

The `NodeEvent(node_id, false)` (finalize) handler on `plan-caching` flushes from global `node_map` and calls `on_exec_emit_stats` + `on_exec_operator_end` on subscribers.

Changes:
- Remove the `flush()` call and `on_exec_emit_stats` call (stats are now flushed per-input_id via `TakeInputSnapshot`)
- **Keep** the `on_exec_operator_end` subscriber notification and progress bar `finalize_node` call (these are lifecycle events, not stat snapshots)
- The `NodeEvent(node_id, true)` (activate) handler stays the same

- [ ] **Step 7: Remove `runtime_stats()` from PipelineNode trait**

In `src/daft-local-execution/src/pipeline.rs`, remove the `fn runtime_stats(&self) -> Arc<dyn RuntimeStats>` method from the `PipelineNode` trait. This will cause compile errors in all node implementations — those are fixed in subsequent tasks.

- [ ] **Step 8: Verify compilation of runtime_stats module**

Run: `cargo check -p daft-local-execution --features python 2>&1 | head -50`

Expected: Compile errors in node implementations (source, intermediate, sinks, join, concat) due to removed `runtime_stats()` method. The runtime_stats module itself should have no errors.

- [ ] **Step 9: Commit**

```bash
git add src/daft-local-execution/src/runtime_stats/mod.rs src/daft-local-execution/src/pipeline.rs
git commit -m "feat: restructure RuntimeStatsManager for per-input_id stat tracking"
```

---

### Task 2: Update `try_finish` in run.rs

**Files:**
- Modify: `src/daft-local-execution/src/run.rs`

- [ ] **Step 1: Change non-last input_id path to use `take_input_snapshot`**

In `NativeExecutor::try_finish`, change the `else` branch (non-last input_id) from:

```rust
Ok(async move { stats_handle.request_snapshot().await }.boxed())
```

to:

```rust
Ok(async move { stats_handle.take_input_snapshot(input_id).await }.boxed())
```

- [ ] **Step 2: Commit**

```bash
git add src/daft-local-execution/src/run.rs
git commit -m "feat: try_finish returns per-input_id stats via take_input_snapshot"
```

---

### Task 3: Update IntermediateNode to create per-input_id stats

**Files:**
- Modify: `src/daft-local-execution/src/intermediate_ops/intermediate_op.rs`

IntermediateNode already has per-input_id state tracking via `input_trackers: HashMap<InputId, InputTracker>`. The pattern: when a new input_id is first seen, create a new RuntimeStats and register it.

- [ ] **Step 1: Add stats_manager handle and stats factory to IntermediateNode**

The node needs access to the stats manager handle to register new per-input_id stats, and a way to create new RuntimeStats instances. Add to the node struct:

```rust
struct IntermediateNode {
    // ... existing fields ...
    // Remove: runtime_stats: Arc<dyn RuntimeStats>
    // Add:
    stats_manager: RuntimeStatsManagerHandle,
    node_id: NodeID,
}
```

Store the stats factory (the `make_runtime_stats` closure or the meter + node_info needed to create stats) so new stats can be created per input_id.

- [ ] **Step 2: Create per-input_id RuntimeStats in handle_morsel**

In `handle_morsel`, when a new input_id is encountered (new InputTracker created), also create a new RuntimeStats:

```rust
let stats = intermediate_op.make_runtime_stats(&meter, &node_info);
stats_manager.register_input_stats(node_id, input_id, stats.clone());
// Store stats in InputTracker or a parallel HashMap<InputId, Arc<dyn RuntimeStats>>
```

Use this per-input_id stats for all `add_rows_in`/`add_rows_out`/`add_cpu_us` calls within that input_id's processing.

- [ ] **Step 3: Remove `runtime_stats()` from IntermediateNode's PipelineNode impl**

Remove the method that returned the global runtime_stats.

- [ ] **Step 4: Verify compilation**

Run: `cargo check -p daft-local-execution --features python 2>&1 | head -30`

- [ ] **Step 5: Commit**

```bash
git add src/daft-local-execution/src/intermediate_ops/intermediate_op.rs
git commit -m "feat: IntermediateNode creates per-input_id RuntimeStats"
```

---

### Task 4: Update SourceNode to create per-input_id stats

**Files:**
- Modify: `src/daft-local-execution/src/sources/source.rs`

Sources receive morsels from the source stream. Each morsel has an input_id. On first sight of a new input_id, create and register a new SourceStats.

- [ ] **Step 1: Add stats_manager handle and per-input_id stats map to SourceNode**

Replace the single `runtime_stats: Arc<SourceStats>` with a mechanism to create per-input_id SourceStats. The source's `start()` method needs access to the stats manager handle.

- [ ] **Step 2: In the source stream processing loop, create stats per input_id**

When processing `PipelineMessage::Morsel { input_id, partition }`, look up or create a `SourceStats` for that input_id. Register new stats with `stats_manager.register_input_stats(node_id, input_id, stats)`.

- [ ] **Step 3: Remove `runtime_stats()` from SourceNode's PipelineNode impl**

- [ ] **Step 4: Verify compilation**

Run: `cargo check -p daft-local-execution --features python 2>&1 | head -30`

- [ ] **Step 5: Commit**

```bash
git add src/daft-local-execution/src/sources/source.rs
git commit -m "feat: SourceNode creates per-input_id RuntimeStats"
```

---

### Task 5: Update BlockingSinkNode to create per-input_id stats

**Files:**
- Modify: `src/daft-local-execution/src/sinks/blocking_sink.rs`

BlockingSinkNode already has per-input_id ExecutionContext. Create per-input_id RuntimeStats when setting up each ExecutionContext.

- [ ] **Step 1: Create per-input_id stats in ExecutionContext setup**

When a new input_id arrives and a new ExecutionContext is created, also create a new RuntimeStats and register it. Store it in the ExecutionContext.

- [ ] **Step 2: Remove global runtime_stats field and `runtime_stats()` from PipelineNode impl**

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p daft-local-execution --features python 2>&1 | head -30`

- [ ] **Step 4: Commit**

```bash
git add src/daft-local-execution/src/sinks/blocking_sink.rs
git commit -m "feat: BlockingSinkNode creates per-input_id RuntimeStats"
```

---

### Task 6: Update StreamingSinkNode to create per-input_id stats

**Files:**
- Modify: `src/daft-local-execution/src/streaming_sink/base.rs`

Same pattern as BlockingSinkNode — per-input_id ExecutionContext already exists.

- [ ] **Step 1: Create per-input_id stats in ExecutionContext setup**

- [ ] **Step 2: Remove global runtime_stats field and `runtime_stats()` from PipelineNode impl**

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p daft-local-execution --features python 2>&1 | head -30`

- [ ] **Step 4: Commit**

```bash
git add src/daft-local-execution/src/streaming_sink/base.rs
git commit -m "feat: StreamingSinkNode creates per-input_id RuntimeStats"
```

---

### Task 7: Update JoinNode to create per-input_id stats

**Files:**
- Modify: `src/daft-local-execution/src/join/join_node.rs`
- Modify: `src/daft-local-execution/src/join/build.rs`
- Modify: `src/daft-local-execution/src/join/probe.rs`

JoinNode currently creates a single RuntimeStats and passes clones to both build and probe contexts. Change: build and probe create their own per-input_id stats.

- [ ] **Step 1: Pass stats_manager handle and stats factory to build/probe**

JoinNode passes the stats manager handle, node_id, and `make_runtime_stats` info to build and probe execution contexts. Build and probe create per-input_id stats when they spawn a `process_single_input` task for a new input_id.

- [ ] **Step 2: In build.rs `process_single_input`, create per-input_id stats**

When `process_single_input(input_id, ...)` is called, create a new RuntimeStats and register it.

- [ ] **Step 3: In probe.rs `process_single_input`, create per-input_id stats**

Same pattern as build.

- [ ] **Step 4: Remove global runtime_stats from JoinNode and its PipelineNode impl**

- [ ] **Step 5: Verify compilation**

Run: `cargo check -p daft-local-execution --features python 2>&1 | head -30`

- [ ] **Step 6: Commit**

```bash
git add src/daft-local-execution/src/join/
git commit -m "feat: JoinNode creates per-input_id RuntimeStats for build and probe"
```

---

### Task 8: Update ConcatNode to create per-input_id stats

**Files:**
- Modify: `src/daft-local-execution/src/concat.rs`

- [ ] **Step 1: Create per-input_id stats when processing morsels**

ConcatNode currently asserts `input_id == 0`. If that still holds, create stats for input_id 0 on first morsel. Otherwise, create per input_id.

- [ ] **Step 2: Remove global runtime_stats and `runtime_stats()` from PipelineNode impl**

- [ ] **Step 3: Verify compilation**

Run: `cargo check -p daft-local-execution --features python 2>&1 | head -30`

- [ ] **Step 4: Commit**

```bash
git add src/daft-local-execution/src/concat.rs
git commit -m "feat: ConcatNode creates per-input_id RuntimeStats"
```

---

### Task 9: Update any remaining nodes and fix compilation

**Files:**
- Any remaining files that implement `PipelineNode::runtime_stats()`

- [ ] **Step 1: Find all remaining `runtime_stats()` implementations**

Run: `grep -rn "fn runtime_stats" src/daft-local-execution/src/`

Fix any remaining implementations.

- [ ] **Step 2: Full compilation check**

Run: `DAFT_DASHBOARD_SKIP_BUILD=1 cargo check -p daft-local-execution --features python`

Expected: Clean compilation.

- [ ] **Step 3: Commit any remaining fixes**

---

### Task 10: Update tests

**Files:**
- Modify: `src/daft-local-execution/src/runtime_stats/mod.rs` (tests module)

- [ ] **Step 1: Update existing tests to register input stats instead of passing global stats**

The existing tests create `DefaultRuntimeStats` and pass them in `node_map`. Update them to:
1. Pass only `node_info_map` to `new_impl`
2. After creating the stats manager, use `handle.register_input_stats(node_id, input_id, stats)` to register stats
3. Adjust assertions as needed

- [ ] **Step 2: Add test for `take_input_snapshot`**

```rust
#[tokio::test(start_paused = true)]
async fn test_take_input_snapshot_isolates_stats() {
    // Create stats manager with no initial stats
    // Register stats for (node_0, input_0) and (node_0, input_1)
    // Add different row counts to each
    // take_input_snapshot(input_0) should return only input_0's stats
    // take_input_snapshot(input_1) should return only input_1's stats
    // After taking, the stats should be removed (second take returns empty)
}
```

- [ ] **Step 3: Add test for periodic snapshot aggregation**

```rust
#[tokio::test(start_paused = true)]
async fn test_periodic_snapshot_aggregates_across_input_ids() {
    // Register stats for (node_0, input_0) and (node_0, input_1)
    // Add rows to both
    // Wait for periodic tick
    // Subscriber should receive aggregated stats for node_0
}
```

- [ ] **Step 4: Run all tests**

Run: `cargo test -p daft-local-execution --features python -- runtime_stats`

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/daft-local-execution/src/runtime_stats/mod.rs
git commit -m "test: update runtime stats tests for per-input_id isolation"
```

---

### Task 11: Integration test with `make build` + `make test`

- [ ] **Step 1: Full build**

Run: `DAFT_DASHBOARD_SKIP_BUILD=1 make build`

Expected: Clean build.

- [ ] **Step 2: Run native execution tests**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v -x tests/dataframe/" 2>&1 | tail -30`

Expected: Tests pass.

- [ ] **Step 3: Commit any final fixes**
