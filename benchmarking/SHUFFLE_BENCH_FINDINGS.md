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

### Where the write time actually goes (NOT YET INSTRUMENTED)

Per map task at N=8192 (~4.4 s total per task):
- `partition_by_hash` p50: 470 ms (11%)
- accumulate per-part p50: 3 ms (<1%)
- writer (concat+IPC+disk write) p50: **3800 ms (88%)**

But K-coalesce sweep showed the writer step is **not** dominated by IPC encode count. **Suspects (need timing to confirm):**
1. ~8192 `tokio::spawn` calls per map task for the parallel `concat_one_partition` futures — scheduler overhead at this scale could be enormous.
2. Per-partition `MicroPartition::concat` + `concat_or_get` overhead amortized over 1-8 small chunks each.
3. Per-batch RecordBatch construction in arrow.

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

### 1. Instrument the writer hot loop in `oneshot_writer.rs`

Goal: attribute the 3.8 s/task writer step into buckets so we can target the real bottleneck.

Add `write_agg` module in `src/daft-shuffles/src/multi_partition_cache.rs` (or a new home) parallel to `read_agg`. Counters needed:
- `SPAWN_TASKS` — count of `tokio::spawn` calls (= num_partitions × num_map_tasks).
- `SPAWN_TOTAL_US` — wall time spent inside the per-partition `concat_one_partition` futures.
- `MP_CONCAT_US` — time inside `MicroPartition::concat(parts)`.
- `MP_CONCAT_OR_GET_US` — time inside `combined.concat_or_get()`.
- `IPC_WRITE_US` — time inside `writer.write(&arrow_batch)`.
- `FILE_WRITE_BYTES` — bytes the IPC writer pushed to disk.

The `concat_one_partition` closure in `write_partitions_one_shot` runs inside `tokio::spawn`. Wrap each phase in `Instant::now()` and atomic adds. The IPC encode + disk write happens in the outer `spawn_blocking` block — measure separately.

Add a `log_write_agg_summary` and call it from `seal_bench::main` after phase 1, mirroring the read-side breakdown. Print like:

```
--- Write path attribution ---
spawn tasks:                 N × M = 1638400
spawn total / concat / concat_or_get / ipc_write totals (ms): ... / ... / ... / ...
per-partition spawn / concat / concat_or_get / ipc_write (us): ... / ... / ... / ...
```

Run `oneshot/never/N=8192/8GiB` with cold-read AND read-concat to get a definitive breakdown.

**Expected finding:** either `tokio::spawn` overhead (the 1.6M spawns per shuffle is huge) or `MicroPartition::concat` per-call overhead dominates. The K-sweep ruled out IPC encode.

### 2. Pick a write-side fix based on what (1) reveals

Likely candidates:
- **Drop the per-partition `tokio::spawn`.** Run `concat_one_partition` serially on the writer thread. The parallel concat is fan-out-then-collect; with N=8192 partitions of 5 KiB each, there's no real parallelism win to be had at this granularity. Spawn cost almost certainly dominates the work.
- **Avoid `MicroPartition::concat` when input is `Vec<MP>` of length 1.** Common case (production usually has 1 chunk per map task input).
- **Skip the `concat_or_get` → `arrow_array::RecordBatch::try_from` round-trip** if upstream already produces a single RecordBatch.

### 3. Confirm read-side concat on EBS

Earlier EBS sweep was done without read-concat. Re-run oneshot/never at N ∈ {512, 2048, 8192} with `DAFT_SHUFFLE_READ_CONCAT_BYTES=4194304` and `DAFT_SHUFFLE_BENCH_COLD_READ=1`, `TMPDIR=/mnt/ebs/tmp`. Compare against existing `/mnt/ebs/ebs_sweep.csv`. Expected: smaller absolute win than NVMe because EBS is already bandwidth-bound, but still meaningful at high N.

### 4. Scale-up runs

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

### 5. Production wiring

The bench drives the Flight server directly. Production uses `RepartitionSink::finalize` (writes) and `FlightClientManager::fetch_partition` (reads). Read-side concat is server-side so it's automatically applied to reads — only the **server's env var** needs to be set. Decide where to surface this:

- Hard-code default `chunk_target = 4 MiB` in `ShuffleFlightServer::new`?
- Add a `read_chunk_target_bytes` field to a server config struct + plumb through `daft-distributed`?
- Keep env-only for now and document?

### 6. Clean up dead variants

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
