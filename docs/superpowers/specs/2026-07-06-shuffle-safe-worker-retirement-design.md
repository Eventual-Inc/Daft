# Shuffle-Safe Worker Retirement — Design Spec

**Date:** 2026-07-06
**Status:** Approved
**Scope:** Ray runner correctness fix — prevent Daft's autoscaling downscaler from killing a worker whose Flight shuffle server is still (or will be) needed by other workers.

---

## Problem

With `DAFT_AUTOSCALING_DOWNSCALE_ENABLED=1`, a distributed shuffle (e.g. `write_deltalake` with a `ShuffleRead->CatalogWrite` stage) can crash with:

```
daft.exceptions.DaftCoreException: DaftError::External Tonic error: ...
h2 protocol error: error reading a body from connection
BrokenPipe: stream closed because of a broken pipe
```

Root cause chain:

1. `RaySwordfishWorker::is_idle()` / `release()` (`src/daft-distributed/src/python/ray/worker.rs:120-154`) gate purely on `active_task_details` — whether the scheduler currently has tasks assigned to that worker. They have zero awareness of the worker's Arrow-Flight shuffle server still being read from by *other* workers' `ShuffleRead` tasks.
2. A worker that finishes its own assigned tasks is marked idle immediately, even though its Flight server may still hold shuffle data that hasn't been fetched yet.
3. After `DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS` (default 60s), `retire_idle_workers` (`worker_manager.rs:335-508`) selects it and calls `release()`, which calls `shutdown()` → `ray.kill()` (`daft/runners/flotilla.py:417-418`) — an immediate, forceful process kill.
4. `ray.kill()` skips Rust destructors entirely, so the *existing* graceful-drain mechanism (`FlightServerConnectionHandle::shutdown()`, `src/daft-shuffles/src/server/flight_server.rs:360-369`, normally invoked via `impl Drop for NativeExecutor` at `src/daft-local-execution/src/run.rs:674-679`) never fires.
5. Any in-flight or future read from that worker's Flight server breaks mid-stream → `BrokenPipe`/h2 error → job failure.

Ray's own autoscaler additionally reclaims the now-actor-less node and logs `AUTOSCALER_DRAIN_IDLE` — a downstream symptom, not the trigger.

This is a genuine correctness gap: retiring workers based only on "no active scheduled tasks" is unsafe for Daft's pull-based shuffle, where a worker's Flight server must stay alive until every reader that needs it has finished.

---

## Non-Goals

- Changes to the local (non-Ray) runner — this bug is specific to the Ray autoscaling downscaler.
- General Ray-cluster-autoscaler coordination (Ray's own native idle-node draining is out of scope; Daft cannot control it, only avoid killing actors prematurely on its side).
- Handling data-dependent/dynamic partition counts — confirmed not applicable here (shuffle partition counts are static at plan-translation time).

---

## Architecture

Two independent, complementary mechanisms:

1. **Precise tracking (primary correctness mechanism)**: a `ShuffleLifecycleTracker` that knows exactly which workers are unsafe to retire because their shuffle output hasn't been fully consumed yet.
2. **Graceful shutdown (defense-in-depth)**: replace the forceful `ray.kill()` with the Flight server's existing drain-and-wait shutdown, so that even an untracked edge case degrades gracefully instead of crashing.

```
Dispatcher::await_completed_tasks
  ├─ shuffle-write task completes → tracker.record_contribution(shuffle_id, worker_id)
  └─ shuffle-read task completes  → tracker.record_read_completed(shuffle_id)

RayWorkerManager::retire_idle_workers
  └─ idle-by-duration candidates, filtered by tracker.is_safe_to_retire(worker_id)
       └─ release() → graceful_shutdown() (drain, then exit) [was: ray.kill()]
```

### Files changed

| File | Change |
|---|---|
| `src/daft-distributed/src/scheduling/task.rs` | Add `shuffle_role: Option<ShuffleTaskRole>` to `TaskDetails` |
| `src/daft-distributed/src/pipeline_node/shuffles/repartition.rs`, `gather.rs`, `backends/flight.rs` | Populate `shuffle_role` (`Write`/`Read` + `shuffle_id`) on task builders |
| `src/daft-distributed/src/scheduling/dispatcher.rs` | On task completion, call tracker hooks based on `shuffle_role` |
| `src/daft-distributed/src/python/ray/worker_manager.rs` | New `ShuffleLifecycleTracker` owned by `RayWorkerManagerState`; `retire_idle_workers` gated by it |
| `src/daft-distributed/src/python/ray/worker.rs` | `release()` calls graceful shutdown path instead of forceful kill |
| `daft/runners/flotilla.py` | New `graceful_shutdown()` actor method; `cleanup_plan_shuffle` purges tracker entries for cancelled/failed plans |
| `src/daft-shuffles/src/server/flight_server.rs` | No change — existing drain mechanism (`FlightServerConnectionHandle::shutdown()`) is reused, just reached via a new path |

All changes are additive to existing structs/enums; no existing call sites break.

---

## `ShuffleLifecycleTracker`

```rust
struct ShuffleLifecycleTracker {
    // Workers that have produced map output for a given shuffle so far.
    shuffle_contributors: HashMap<ShuffleId, HashSet<WorkerId>>,
    // Reduce-partition reads not yet completed for a given shuffle.
    shuffle_reads_remaining: HashMap<ShuffleId, usize>,
}
```

- `record_contribution(shuffle_id, worker_id)`: insert into `shuffle_contributors[shuffle_id]`; if `shuffle_reads_remaining` has no entry yet, seed it from the statically-known partition count for that shuffle's consuming node (`RepartitionSpec` explicit count / input partition count, or `1` for `GatherNode` — both fixed at plan-translation time, confirmed non-data-dependent).
- `record_read_completed(shuffle_id)`: decrement `shuffle_reads_remaining[shuffle_id]`; at `0`, remove both the count entry and the `shuffle_contributors[shuffle_id]` entry, releasing every contributing worker from that constraint. Guarded by task-context identity so a retried read task cannot double-decrement.
- `is_safe_to_retire(worker_id)`: `true` iff `worker_id` does not appear in any `shuffle_contributors` set that still has an entry (i.e., still has `reads_remaining > 0`).

`shuffle_id` is already deterministic and known at plan-translation time (`(query_idx << 32) | node_id`), so no new ID scheme is needed.

### Identifying shuffle tasks

`TaskDetails` gains:

```rust
enum ShuffleTaskRole { Write { shuffle_id: ShuffleId }, Read { shuffle_id: ShuffleId } }
```

populated when `RepartitionNode`/`GatherNode` build their write and read task builders — they already compute `shuffle_id` via `make_shuffle_id` at that point. `Dispatcher::await_completed_tasks` pattern-matches this field to route the two tracker calls; no task-graph re-inspection needed.

---

## Graceful Shutdown

- New actor method `graceful_shutdown()` added to the Python `RaySwordfishActor` (`flotilla.py`), running **inside** the actor process so Rust destructors execute normally (unlike `ray.kill()`, which is invoked externally and skips them).
- It explicitly invokes `FlightServerConnectionHandle::shutdown()` (already implemented, `flight_server.rs:360-369`): stop accepting new Flight connections, finish in-flight streams, then the actor exits normally.
- `RaySwordfishWorker::release()` (`worker.rs:139-154`) calls this instead of the current `shutdown()`/`ray.kill()` path.
- Bounded by a timeout (fixed 30s constant) — if the drain hangs (e.g. a wedged reader), fall back to `ray.kill()` so retirement can't block forever. This preserves today's worst-case behavior while making the common case clean.

---

## Error Handling

- **Plan cancellation/failure**: `cleanup_plan_shuffle` (`flotilla.py:650-657`) additionally purges `ShuffleLifecycleTracker` entries for that plan's shuffle_ids (filterable via the `query_idx` encoded in `shuffle_id`), so a failed plan can't permanently pin workers.
- **Worker death**: the existing `mark_worker_died` path also removes that worker from every `shuffle_contributors` set — nothing can be "waiting" on a worker that's already gone.
- **Retried read tasks**: decrement of `shuffle_reads_remaining` is keyed off logical reduce-partition identity (task context), not raw completion-event count, so a retry after transient failure doesn't double-decrement and free a worker too early.

---

## Testing Plan

1. **Unit — `ShuffleLifecycleTracker`**: contribution tracking, decrement-to-zero clears the entry, a single worker contributing to multiple concurrent shuffles, retried read task does not double-decrement.
2. **Unit — `retire_idle_workers`**: idle-by-duration candidates correctly filtered by tracker state (mocked tracker).
3. **Integration (most important — this is what would have caught the original bug)**: `DAFT_RUNNER=ray`, `DAFT_AUTOSCALING_DOWNSCALE_ENABLED=1` with a short idle timeout, run a shuffle-heavy query (repartition + write). Assert no crash, correct output, and (if observable) that a worker is not released while it still has an outstanding shuffle contribution.
4. **Python — graceful shutdown**: simulate a slow/blocked Flight reader against a worker slated for release; assert the read completes successfully rather than erroring, and that `release()` still completes (via drain, not force-kill) once the read finishes.
5. **Regression**: existing Ray runner and scheduling tests pass unchanged.

---

## Open Questions (resolved)

- **Is a `server_address → WorkerId` map needed?** No — the tracker keys everything by `WorkerId` directly (via `TaskDetails`/`Dispatcher`), so the physical Flight address is never needed for correctness. Not building this mapping (YAGNI).
- **Cross-plan safety**: `RayWorkerManager` and its worker pool are shared across all plans/queries on a given runner instance, so tracking must be (and is) global, not per-plan — confirmed via `retire_idle_workers`'s existing scope.
- **Data-dependent partition counts**: confirmed not applicable — all relevant partition counts (`RepartitionNode`, `GatherNode`) are fixed at plan-translation time.
