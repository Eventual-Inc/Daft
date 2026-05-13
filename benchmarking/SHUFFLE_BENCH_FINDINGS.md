# Flight Shuffle Bench Findings & Next Steps

Hardware: AWS i8g.4xlarge (16 vCPU Graviton, 32 GiB RAM, 3.4 TiB local NVMe instance store, 200 GiB EBS gp3 attached). Workload: synthetic `key u64 + 4 × u64` rows generated in `seal_bench`. All numbers M=200 inputs × N=8192 outputs × 8 GiB unless noted. NVMe cold-cache means `drop_caches` between phases.

## What we proved

### Read-side server-side concat is the win

`DAFT_SHUFFLE_READ_CONCAT_BYTES=4194304` (4 MiB target chunks). Server reads M source batches per partition, concats arrow-level into chunks crossing the target size, emits as fewer/bigger `FlightData`. **86 s → 68 s total wall at N=8192/M=200/8 GiB on NVMe (21% faster)**. Preserves per-map-task file isolation (FT). No rewrite phase. Memory transient bounded.

### Arrow-merge seal is strictly worse than read-side concat

| | total | breakdown |
|---|---|---|
| no-op (baseline) | 86 s | map 65 + read 21 |
| arrow-merge seal | 84 s | map 65 + seal 15 + read 4 |
| **read-side concat** | **68 s** | map 65 + read 5 |

Same per-Flight-message savings, but seal pays in disk I/O (rewrite phase scales with data size) while concat pays in RAM bandwidth (transient memcpy). At 100 GiB and 1 TB scales seal would balloon to minutes/hours; concat stays bounded.

### Pipelined seal violates fault tolerance

Merging a partition's contributions before all map tasks confirm = if a task is retried after its data was merged, retry produces duplicates. Code committed under `DAFT_SHUFFLE_PIPELINE_THRESHOLD` env var but **must not be used in production** as-is.

### Write-side K-coalescing as designed is net negative

Grouping K adjacent partitions into one IPC batch on write:
- Writer step time is **unchanged** (3.8s/task at K=1, K=16, K=64 alike). So per-IPC-encode cost isn't the dominant write bottleneck.
- Read time gets **K× worse** because K reducers all need the same shared batch (read amplification).

| K | Map | Read | Total |
|---|---|---|---|
| 1 | 62.7 | 5.3 | **68.0** |
| 16 | 63.1 | 7.3 | 70.4 |
| 64 | 62.0 | 20.2 | 82.2 |

Code remains in tree (`DAFT_SHUFFLE_WRITE_COALESCE_K`) but should be ignored / reverted.

### Where the read time actually goes (instrumented)

Per-message attribution at N=8192 cold-read, no concat (20.5 s total read):

| Bucket | Wall-equivalent | % |
|---|---|---|
| Client gRPC decode + Flight IPC parse (`stream.next()`) | 19.4 s | **95%** |
| Server file open syscalls | 5.9 s | 29% |
| Server flatbuffer Message read | 7.9 s | 38% |
| Server body bytes read | 0.015 s | 0.07% |
| Server send-gap (between yields) | 0.08 s | 0.4% |
| Client arrow→daft Series convert | 0.11 s | 0.5% |

(Server-side counters accumulate across 16 concurrent reducers; ÷16 for per-reducer wall.)

**The bottleneck is per-FlightData fixed cost on the client (~189 μs/batch)**, dominated by whatever runs inside `arrow_flight::decode::FlightRecordBatchStream::next()`. Body bytes read is essentially free at NVMe sequential speed.

### Where the write time actually goes (instrumented 2026-05-13)

Per map task at N=8192/M=200/8 GiB on NVMe with read-concat, oneshot/never (counter source: `write_agg` module, totals / 200 map tasks):

| Bucket | per task | per nonempty future | notes |
|---|---|---|---|
| join_wall (writer thread blocked on join_all) | **2960 ms** | — | wall observed by the writer |
| spawn_total (sum of per-future service time) | 4055 ms | 494 us | total CPU inside `concat_one_partition` |
| blocking_wall (file create + write loop + finish) | 503 ms | — | the spawn_blocking section |
| `MicroPartition::concat` | 189 ms | 23 us | metadata-only, cheap |
| `concat_or_get` (RecordBatch::concat of ~8 batches/part) | **1605 ms** | **195 us** | **biggest single bucket** |
| `try_into` (daft RB → arrow RB) | 132 ms | 16 us | |
| `concat_batches` (write-side K-merge) | 19 ms | 2 us | K=1 passthrough |
| `ipc_write` (encode + disk write of merged batch) | 293 ms | 35 us | inside spawn_blocking |

Total per map task wall = join_wall + blocking_wall ≈ **3.46 s** (matches the original 3.8 s/task figure).

Inner timers cover 234 us per future; mean per-future service time is 494 us. **The gap (~260 us / future) is overhead inside the spawned future but outside the timed inner work** — chiefly tokio::spawn task setup/poll, the `parts.iter().sum::<len>()` and `size_bytes()` passes, and atomic adds. At 1.6 M spawned futures per shuffle, that gap accounts for ~525 s of total CPU (~3.3 s/task).

**Confirmed root causes:**
1. **`concat_or_get` dominates inside the future.** 195 us × 8192 = 1.6 s of CPU per task. Each output partition for one map task is `Vec<MicroPartition>` of length ~8 (one per input chunk) → `RecordBatch::concat` on 8 small batches of 5 cols × 5 KB.
2. **tokio::spawn overhead matches or exceeds the timed work.** ~260 us / future of untimed in-future overhead × 1.6 M spawned futures swamps the actual concat work.
3. **IPC encode is not the bottleneck.** 35 us / batch × 8192 = 287 ms/task — confirms the K-sweep result that batch count isn't the lever.

## Code state on branch `colin/fun`

Latest commit: `2a3ea4734` (push fresh before starting work)

### Env vars that work / should be used

- `DAFT_SHUFFLE_READ_CONCAT_BYTES=4194304` — **the win.** Read-side server concat with 4 MiB target chunks. Always on for any production-shape workload.
- `DAFT_SHUFFLE_BENCH_COLD_READ=1` — bench-only. `sync` + `drop_caches` between map and read phases so reads exercise disk on EBS / cold path. Bench must run under sudo for `/proc/sys/vm/drop_caches` to be writable.

### Env vars that exist but are net negative / dangerous

- `DAFT_SHUFFLE_PIPELINE_THRESHOLD` — pipelined per-partition seal. **FT-unsafe.** Don't enable.
- `DAFT_SHUFFLE_WRITE_COALESCE_K` — write-side K-grouping. **Net negative.** Don't enable.
- `DAFT_SHUFFLE_SEAL_MERGE=1` — arrow-merge seal. Functional but strictly worse than read-side concat. Useful for A/B only.
- `DAFT_SHUFFLE_READ_PREFETCH` — file open prefetch fan-out. Default 1. Earlier bench showed >1 regresses.

### Instrumentation already in place (read side)

In `src/daft-shuffles/src/server/flight_server.rs::read_agg`:
- `FLIGHTDATAS_EMITTED`, `MSG_HEADER_READ_US`, `MSG_BODY_READ_US`, `SEND_GAP_US` (server)
- `CLIENT_BATCH_DELIVERY_US`, `CLIENT_CONVERT_US`, `CLIENT_BATCHES_RECEIVED` (client)
- `OPEN_US`, `SPECS_OPENED`, `BYTES_SHIPPED`, `HANDLER_US` (existing)
- `LOCAL_*` (existing local path)

Bench prints a `--- Read path attribution ---` block before exiting.

### Instrumentation already in place (write side, oneshot)

In `src/daft-shuffles/src/multi_partition_cache.rs::write_agg`:
- `SPAWN_TASKS`, `SPAWN_TASKS_NONEMPTY`, `SPAWN_TOTAL_US`, `JOIN_WALL_US` (scheduling)
- `MP_CONCAT_US`, `MP_CONCAT_OR_GET_US`, `TRY_INTO_US` (inside spawned futures)
- `CONCAT_BATCHES_US`, `IPC_WRITE_US`, `FILE_WRITE_BYTES`, `BLOCKING_WALL_US` (inside spawn_blocking)
- `ONESHOT_CALLS` (= number of map tasks)

Bench prints a `--- Write path attribution (oneshot) ---` block before exiting.

### EC2 layout

- Instance: `i-03ee7d2afe417f0df` (us-west-2, AZ us-west-2a)
- Mounts (need to re-establish after any reboot — device names shuffle):
  - `/dev/nvme2n1` (3.4 TiB local NVMe instance store) → `/mnt/nvme`
  - `/dev/nvme1n1` (200 GiB EBS gp3) → `/mnt/ebs`
  - Confirm via `lsblk -ndo NAME,SIZE,MODEL` (look at MODEL column — "Instance Storage" vs "Elastic Block Store")
- Repo: `/home/ec2-user/daft` on branch `colin/fun`
- Cargo target: `/mnt/nvme/target` (set via `CARGO_TARGET_DIR`)
- Binary: `/mnt/nvme/target/release/seal_bench`
- Already raised fd ulimit to 1M via `/etc/security/limits.d/nofile.conf` (sudo invocation inherits it)
- Cost: ~$1.50/hr; terminate when done with `aws ec2 terminate-instances --region us-west-2 --instance-ids i-03ee7d2afe417f0df`

### Building the bench

```bash
source ~/.cargo/env
export CARGO_TARGET_DIR=/mnt/nvme/target
cd ~/daft && git fetch origin colin/fun && git reset --hard origin/colin/fun
cargo build --release -p daft-shuffles --bin seal_bench
```

Incremental rebuild is ~15-20 s. Full from scratch ~5 min on this instance.

### Canonical bench invocation

```bash
sudo -E env \
  BIN=/mnt/nvme/target/release/seal_bench \
  TMPDIR=/mnt/nvme/tmp \
  bash -c '
ulimit -n 1048576
sync && echo 3 > /proc/sys/vm/drop_caches
DAFT_SHUFFLE_BENCH_COLD_READ=1 \
  DAFT_SHUFFLE_READ_CONCAT_BYTES=4194304 \
  "$BIN" --writer oneshot --seal never \
    --inputs 200 --outputs 8192 --bytes 8GiB --payload-cols 4 \
    --map-conc 16 --read-conc 16
'
```

Swap `TMPDIR=/mnt/ebs/tmp` for EBS, swap `--bytes` to scale, swap `--writer` for `multi_file` / `append` to compare.

## Next steps (in priority order)

### 1. Drop the per-partition `tokio::spawn`

**Strongest signal from the instrumentation.** With 8192 partitions of ~5 KB and 8 chunks each, each spawned future does ~234 us of inner work but costs ~260 us of in-future overhead on top — i.e. the scheduling itself outweighs the work being scheduled.

Replace `for parts in partitions_per_output { tokio::spawn(...) } + join_all` in `write_partitions_one_shot` with one of:
- **A single `spawn_blocking` that loops serially** over all N partitions doing `concat_one_partition`. With map_conc=16 we already saturate 16 cores at the *outer* layer, so the inner fan-out is buying nothing.
- **A bounded `rayon::par_iter`** over partitions if we want CPU parallelism within a map task. But this only helps if other map tasks aren't already keeping cores busy.

Expected upside: cut the 2.96 s/task join_wall to ~spawn_total / 16 (current inner work) ≈ 250 ms/task → writer step drops from 3.46 s to ~750 ms, total wall from 68 s to ~50 s.

### 2. Cheaper `concat_or_get` for shuffle-shaped input

195 us × 8192 = 1.6 s/task spent in `RecordBatch::concat` of ~8 tiny batches per partition. Two angles:
- **Concat at the MicroPartition level lazily.** Today `MicroPartition::concat` produces an MP with chunks vec = concat-of-chunks, then `concat_or_get` discovers >1 chunks and runs `RecordBatch::concat`. The IPC writer only needs *some* arrow RecordBatch — could we emit each input chunk as its own batch and let read-side concat amortize? **Probably no** — that pushes batch count from 1 per partition to 8 per partition, multiplying per-FlightData fixed cost (the read-side bottleneck).
- **Skip the `MicroPartition::concat` round-trip when `parts.len() == 1`.** Common case in production. Save ~23 us/partition. Marginal.
- **Pre-concat at sink time** rather than at finalize. If the RepartitionSink already holds a single MP per partition by the time finalize runs, this entire path becomes a no-op. Worth checking what the sink looks like in production.

### 3. Eliminate the inner `try_into` arrow→arrow conversion

`daft_recordbatch::RecordBatch::try_into::<arrow_array::RecordBatch>()` costs 16 us/partition = 132 ms/task. If `concat_or_get` returns a daft RB and the IPC writer needs an arrow RB, can the writer take the daft RB directly (or just borrow the underlying Arrow arrays)? Probably small but cheap to fix.

### 4. Confirm read-side concat on EBS

Earlier EBS sweep was done without read-concat. Re-run oneshot/never at N ∈ {512, 2048, 8192} with `DAFT_SHUFFLE_READ_CONCAT_BYTES=4194304` and `DAFT_SHUFFLE_BENCH_COLD_READ=1`, `TMPDIR=/mnt/ebs/tmp`. Compare against existing `/mnt/ebs/ebs_sweep.csv`. Expected: smaller absolute win than NVMe because EBS is already bandwidth-bound, but still meaningful at high N.

### 5. Scale-up runs

Once writer step is reduced via (2), run at 100 GiB and at the N=25000 production target:

```bash
DAFT_SHUFFLE_READ_CONCAT_BYTES=4194304 ./seal_bench \
  --writer oneshot --seal never \
  --inputs 200 --outputs 25000 --bytes 100GiB ...
```

Validate that:
- Read-side concat memory transient stays bounded (16 reducers × 4 MiB = 64 MiB — should be fine).
- Map phase scales linearly with data (already saw this at 8 GiB).
- Total wall ≈ floor + per-batch tax + writer overhead. Closer to floor as data grows.

### 6. Production wiring

The bench drives the Flight server directly. Production uses `RepartitionSink::finalize` (writes) and `FlightClientManager::fetch_partition` (reads). Read-side concat is server-side so it's automatically applied to reads — only the **server's env var** needs to be set. Decide where to surface this:

- Hard-code default `chunk_target = 4 MiB` in `ShuffleFlightServer::new`?
- Add a `read_chunk_target_bytes` field to a server config struct + plumb through `daft-distributed`?
- Keep env-only for now and document?

### 7. Clean up dead variants

Either rip out or hide behind aggressive feature gates:
- `pipeline_threshold` in `SealConfig` + `seal_partition_internal` (FT-unsafe).
- `DAFT_SHUFFLE_WRITE_COALESCE_K` (net negative; the `row_ranges` plumbing on `PartitionCache` + `ByteRangeSpec` is dead weight without it).

If we keep `row_ranges`, consider whether it has any other use case (e.g., row-level slicing for skew handling). If not, revert the type changes.

## Quick reference

### File locations
- Bench source: `src/daft-shuffles/src/bin/seal_bench.rs`
- Read-side concat impl: `src/daft-shuffles/src/server/flight_server.rs` (`concat_specs_into_flight_datas_sync`)
- Write-side coalesce impl (dead): `src/daft-shuffles/src/oneshot_writer.rs` (the `coalesce_k` block)
- Read counters: `src/daft-shuffles/src/server/flight_server.rs::read_agg`
- Seal/coalescer: `src/daft-shuffles/src/coalescer.rs`
- PartitionCache: `src/daft-shuffles/src/shuffle_cache.rs` (with new `row_ranges` field)

### Don't do
- Don't run `flight-shuffle` integration tests locally — they hang. Verify code via `cargo check -p daft-shuffles` only.
- Don't rebuild the dashboard frontend (set `DAFT_DASHBOARD_SKIP_BUILD=1` if invoking `make build`).
- Don't change M (number of map tasks) — that's planner/scheduler scope, off-limits for this work.

### Datapoints to beat
The headline data point we want to improve is:
- **N=8192, M=200, 8 GiB, NVMe, cold-read, oneshot/never + read-side concat = 68 s total** (map 63 / read 5).
- Map dominates. Cutting writer step by 2× would put us at ~50 s.
- 4 s floor for pure read+write of 8 GiB at NVMe ~2 GB/s.
