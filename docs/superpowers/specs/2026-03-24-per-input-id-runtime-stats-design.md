# Per-Input-ID Runtime Stats Isolation

## Problem

RuntimeStats currently uses global atomic counters per pipeline node. All input_ids flowing through a long-lived pipeline accumulate into the same counters. When `try_finish(input_id)` is called, the returned stats reflect all input_ids, not just the one that finished.

## Design

### Core change

Nodes create a **new `RuntimeStats` instance per `(node_id, input_id)`** when they first encounter a new input_id. They register it with the stats manager via message. The stats manager owns the per-input_id stats map and can extract/remove stats for a specific input_id on demand.

### New message types

```rust
pub enum StatsManagerMessage {
    NodeEvent(usize, bool),
    SnapshotRequest(oneshot::Sender<ExecutionEngineFinalResult>),
    // New:
    RegisterInputStats(NodeID, InputId, Arc<dyn RuntimeStats>),
    TakeInputSnapshot(InputId, oneshot::Sender<ExecutionEngineFinalResult>),
}
```

### Stats manager state

Add a new map alongside the existing `node_map`:

```rust
// Existing: node_id -> (NodeInfo, RuntimeStats) for global/aggregate use
node_map: HashMap<NodeID, (Arc<NodeInfo>, Arc<dyn RuntimeStats>)>

// New: (node_id, input_id) -> RuntimeStats for per-input isolation
input_stats: HashMap<(NodeID, InputId), Arc<dyn RuntimeStats>>
```

### Event loop handling

- **RegisterInputStats**: Insert into `input_stats`
- **TakeInputSnapshot**: For each entry matching the input_id, call `flush()`, collect `(NodeInfo, StatSnapshot)` pairs, remove entries, return via oneshot
- **Periodic tick**: Aggregate all `input_stats` entries per node_id for subscriber/progress bar snapshots (or just use the existing global `node_map` stats — see open question)
- **SnapshotRequest** (existing): Continue using global `node_map`

### Node-side changes

Where nodes currently call `ctx.runtime_stats.add_rows_in(...)`, they need to:
1. Look up (or create) the per-input_id RuntimeStats for the current morsel's input_id
2. Register it with the stats manager if newly created
3. Record stats on the per-input_id instance

Each node type (source, intermediate, streaming sink, blocking sink, join build/probe) needs a mechanism to map `input_id -> Arc<dyn RuntimeStats>`. A `HashMap<InputId, Arc<dyn RuntimeStats>>` on the node's execution context works — input_id is available at each call site since morsels carry it.

### RuntimeStatsManagerHandle additions

```rust
impl RuntimeStatsManagerHandle {
    // Existing: activate_node, finalize_node, request_snapshot

    pub fn register_input_stats(&self, node_id: NodeID, input_id: InputId, stats: Arc<dyn RuntimeStats>);

    pub async fn take_input_snapshot(&self, input_id: InputId) -> DaftResult<ExecutionEngineFinalResult>;
}
```

### try_finish changes (in run.rs)

- **Non-last input_id path**: Call `stats_handle.take_input_snapshot(input_id)` instead of `request_snapshot()`
- **Last input_id path**: Unchanged — `finish()` flushes everything remaining

### What stays the same

- The `RuntimeStats` trait is unchanged
- `DefaultRuntimeStats` is unchanged — just instantiated more times
- The existing global `node_map` stats continue for progress bars and subscriber periodic updates
- `finish()` for pipeline teardown is unchanged

## Open question

**Periodic snapshots**: Should the 200ms tick use the existing global `node_map` (which would now be empty/unused since stats go to per-input instances), or aggregate from `input_stats`?

Recommendation: Remove the global `node_map` RuntimeStats entirely. Periodic tick aggregates from `input_stats` by summing snapshots per node_id. This avoids having two parallel stat tracking paths.
