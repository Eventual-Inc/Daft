# Pipeline Node Fingerprinting

Move plan fingerprinting from `LocalPhysicalPlan` into the distributed pipeline nodes / task builder layer.

## Problem

Currently `LocalPhysicalPlan::fingerprint()` computes a structural hash of the entire plan tree. This is expensive, fragile (must handle every node type), and lives in the wrong layer — the plan doesn't know whether two instances are "functionally the same" in the context of a pipeline. The pipeline nodes do know this.

## Design

### Core Change

Add `plan_fingerprint: u64` to `SwordfishTaskBuilder`. Pipeline nodes assign it. Remove `fingerprint()` / `fingerprint_impl()` from `LocalPhysicalPlan` and the Python binding.

### Fingerprint Flow

| Operation | Fingerprint behavior |
|---|---|
| `SwordfishTaskBuilder::new(plan, node, fingerprint)` | Caller provides fingerprint |
| `map_plan(node, f)` | Preserves existing fingerprint |
| `pipeline_instruction(node, f)` | Preserves (calls `map_plan`) |
| `combine_with(left, right, node, f)` | Hash of `left.fingerprint ^ right.fingerprint ^ node.node_id()` |

### Node Categories

**Same fingerprint for all emitted tasks** (most nodes):
- Sources: `ScanSourceNode`, `InMemorySourceNode`, `GlobScanSourceNode` — use `node_id` as fingerprint
- Append-only nodes via `pipeline_instruction`: `ProjectNode`, `FilterNode`, `ExplodeNode`, `UnpivotNode`, `TopNNode`, `SampleNode`, `AggregateNode`, `DistinctNode`, `PivotNode`, `UdfNode`, `VllmNode`, `WindowNode`, `ConcatNode`, `IntoBatchesNode` — fingerprint passes through unchanged
- Mixed nodes that create new plans from materialized outputs: `SortNode`, `IntoPartitionsNode`, joins, shuffles, `SinkNode` — use `node_id` as fingerprint for new builders since all new plans are structurally identical

**Different fingerprint per task:**
- `MonotonicallyIncreasingIdNode` — each task has a unique offset, so hash `node_id` with a per-task counter
- `LimitNode` first stage (`map_plan` in execution loop) — preserves incoming fingerprint
- `LimitNode` second stage (new `SwordfishTaskBuilder::new`) — unique fingerprint per task (different limit/offset values)

### Propagation

`SwordfishTask` and `TaskContext` gain a `plan_fingerprint: u64` field, set at `build()` time from the builder.

### Removals

- `LocalPhysicalPlan::fingerprint()` and `fingerprint_impl()` (~500 lines)
- `PyLocalPhysicalPlan::fingerprint()` Python binding
- `fingerprint` entry in `daft/__init__.pyi`

## Files to Modify

1. `src/daft-local-plan/src/plan.rs` — remove fingerprint methods
2. `src/daft-local-plan/src/python.rs` — remove Python binding
3. `daft/daft/__init__.pyi` — remove type stub
4. `src/daft-distributed/src/scheduling/task.rs` — add fingerprint to builder/task/context
5. `src/daft-distributed/src/pipeline_node/mod.rs` — update `pipeline_instruction` (no change needed, it uses `map_plan`)
6. `src/daft-distributed/src/pipeline_node/scan_source.rs` — pass fingerprint to `new()`
7. `src/daft-distributed/src/pipeline_node/in_memory_source.rs` — pass fingerprint
8. `src/daft-distributed/src/pipeline_node/glob_scan_source.rs` — pass fingerprint
9. `src/daft-distributed/src/pipeline_node/monotonically_increasing_id.rs` — unique fingerprint per task
10. `src/daft-distributed/src/pipeline_node/limit.rs` — stage-dependent fingerprinting
11. All other pipeline nodes that call `SwordfishTaskBuilder::new()` — pass `node_id`-based fingerprint
