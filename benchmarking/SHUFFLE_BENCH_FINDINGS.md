# Flight Shuffle Bench Findings & Next Steps

Hardware: AWS i8g.4xlarge (16 vCPU Graviton, 32 GiB RAM, 3.4 TiB local NVMe instance store, 200 GiB EBS gp3 attached). Workload: synthetic `key u64 + 4 × u64` rows generated in `seal_bench`. All numbers M=200 inputs × N=8192 outputs × 8 GiB unless noted. NVMe cold-cache means `drop_caches` between phases.

## What we proved

### Dropping the per-partition `tokio::spawn` is the biggest win

`write_partitions_one_shot` previously fanned out 1.6 M `tokio::spawn` futures per shuffle (N=8192 partitions × M=200 map tasks) for parallel `concat_one_partition`, then `join_all`'d before writing. Removing this and doing the concat serially inside the existing `spawn_blocking` cut total wall **69.5 s → 27.5 s (60% reduction)** on the same workload. Writer step p50: 3925 ms → 913 ms. Outer map_conc=16 already saturates cores, so the inner fan-out bought nothing.

Surprisingly, removing the spawn also made the actual work faster — per-future inner timers all dropped:

| | spawn-fanout | serial inline | speedup |
|---|---|---|---|
| per-future mean service time | 494 us | 73 us | 6.7× |
| mp_concat | 23 us | 3 us | 7.7× |
| mp_concat_or_get | 195 us | 21 us | 9.3× |
| try_into | 16 us | 4 us | 4× |
| ipc_write (already spawn_blocking) | 35 us | 33 us | 1.06× |

The work-side speedup is most likely cross-thread cache contention being eliminated — 1.6 M tasks running on 16 cores were thrashing the cache and shared allocators. Serial on one thread per map task lets the small ~5 KB batches stay hot.

### Read-side server-side concat is the win

`DAFT_SHUFFLE_READ_CONCAT_BYTES=4194304` (4 MiB target chunks). Server reads M source batches per partition, concats arrow-level into chunks crossing the target size, emits as fewer/bigger `FlightData`. **86 s → 68 s total wall at N=8192/M=200/8 GiB on NVMe (21% faster)**. Preserves per-map-task file isolation (FT). No rewrite phase. Memory transient bounded.

### Arrow-merge seal is strictly worse than read-side concat

| | total | breakdown |
|---|---|---|
| no-op (baseline) | 86 s | map 65 + read 21 |
| arrow-merge seal | 84 s | map 65 + seal 15 + read 4 |
| read-side concat | 68 s | map 65 + read 5 |
| **read-concat + spawn-removed** | **27.5 s** | map 23 + read 5 |

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

### Where the read time actually goes (instrumented; with read-side concat)

Read time at N=8192/M=200/8 GiB NVMe cold-read with read-concat is 4.8 s (16 reducer goroutines, ÷16 for per-reducer wall):

| Bucket | Total wall | Per reducer | Per unit |
|---|---|---|---|
| **decode** (`StreamReader::next` per source batch) | **30.9 s** | **1.93 s** | 18 us / src batch |
| stream_init (`StreamReader::try_new` — re-parses schema header per range) | 4.1 s | 254 ms | 2.5 us / init |
| open (`File::open` syscall) | 2.0 s | 126 ms | 1 us / open |
| merge (`concat_batches` per output chunk) | 1.3 s | 79 ms | 153 us / chunk |
| encode (`IpcDataGenerator::encode` per output chunk) | 0.45 s | 28 ms | 54 us / chunk |
| seek | 0.005 s | <1 ms | — |

Source-batch volume: **1.6 M source batches read**, output: 8192 chunks. Every batch is unique (K=1 write side), so a decode cache wouldn't help.

Bench-driven inefficiency: the bench issues **8192 separate `do_get` calls** (one per output partition) and each one opens all 200 map files → 1.6M opens. Within a single `do_get` we already group by file, but cross-`do_get` we re-open. This explains the 1.6M open count; opens themselves are cheap (1 us) so the actual cost is small, but `stream_init` (4 s) is the per-range schema re-parse and *that* could be cut by sharing a `StreamReader` across ranges of the same file.

**The bottleneck is the per-source-batch IPC decode at 18 us × 1.6 M = 30 s.** Each ~5 KB IPC batch pays flatbuffer-parse + array-construction fixed cost. Two angles to attack:
1. Write fewer, larger IPC batches per file (K-coalesce on write side) — but the prior K-sweep showed read amplification offsets the gain when K-grouped partitions are shared across separate `do_get`s. Combine K-coalesce with reducer-side `do_get` batching (request K output partitions in one call) to capture the savings.
2. Skip the per-range schema re-parse (`stream_init`) — open file once, seek, use a lower-level arrow decode API with the schema we already have. Saves 4 s.

### Prior read attribution (no concat — for historical reference)

At N=8192 cold-read, no concat (20.5 s total read):

| Bucket | Wall-equivalent | % |
|---|---|---|
| Client gRPC decode + Flight IPC parse (`stream.next()`) | 19.4 s | **95%** |
| Server file open syscalls | 5.9 s | 29% |
| Server flatbuffer Message read | 7.9 s | 38% |
| Server body bytes read | 0.015 s | 0.07% |
| Server send-gap (between yields) | 0.08 s | 0.4% |
| Client arrow→daft Series convert | 0.11 s | 0.5% |

(Server-side counters accumulate across 16 concurrent reducers; ÷16 for per-reducer wall.)

### Where the write time actually goes (post-spawn-removal, instrumented 2026-05-13)

Per map task at N=8192/M=200/8 GiB on NVMe with read-concat, oneshot/never, **after** dropping the per-partition tokio::spawn (counter source: `write_agg`, totals / 200 map tasks):

| Bucket | per task | per nonempty future | notes |
|---|---|---|---|
| blocking_wall (file create + concat loop + ipc write + finish) | **939 ms** | — | total wall of the writer's spawn_blocking |
| spawn_total (sum of `concat_one_partition` calls) | 605 ms | 73 us | CPU inside concat (no longer spawned, name retained) |
| `MicroPartition::concat` | 25 ms | 3 us | |
| `concat_or_get` (RecordBatch::concat of ~8 batches/part) | 173 ms | 21 us | |
| `try_into` (daft RB → arrow RB) | 38 ms | 4 us | |
| `concat_batches` (write-side K-merge) | 0.1 ms | 0 us | K=1 passthrough |
| `ipc_write` (encode + disk write of merged batch) | 272 ms | 33 us | |

Total per map task wall = blocking_wall ≈ **940 ms** (was 3.46 s under spawn-fanout). join_wall is now 0 since we no longer await spawned futures.

What's left in blocking_wall but unattributed: file create + cache push allocations + `writer.finish()` ≈ 940 − 605 − 272 ≈ 60 ms/task. Negligible.

**Remaining writer-side cost is essentially the bare CPU work:** 605 ms of concat + 272 ms of IPC write per task. ~880 ms of useful work / 940 ms wall = 94% efficient.

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
- `FLIGHTDATAS_EMITTED`, `MSG_HEADER_READ_US`, `MSG_BODY_READ_US`, `SEND_GAP_US` (server, no-concat path)
- `CLIENT_BATCH_DELIVERY_US`, `CLIENT_CONVERT_US`, `CLIENT_BATCHES_RECEIVED` (client)
- `OPEN_US`, `SPECS_OPENED`, `BYTES_SHIPPED`, `HANDLER_US` (existing; `OPEN_US` wraps the whole `concat_specs_into_flight_datas_sync` spawn_blocking when read-concat is on — see CONCAT_* below for the real breakdown)
- `CONCAT_OPENS`, `CONCAT_OPEN_US`, `CONCAT_SEEK_US`, `CONCAT_STREAM_INIT_US`, `CONCAT_DECODE_US`, `CONCAT_SOURCE_BATCHES`, `CONCAT_MERGE_US`, `CONCAT_ENCODE_US`, `CONCAT_OUT_CHUNKS` (read-concat path attribution; added 2026-05-13)
- `LOCAL_*` (existing local path)

Bench prints `--- Read path attribution ---` plus a `--- Read-concat path attribution ---` block when read-concat is enabled.

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

### 1. Phase 1 is now partition_by_hash-dominated

After the writer fix, Phase 1 is 22.6 s with this breakdown:
- partition_by_hash p50/p99: **626 / 3253 ms** ← biggest, long tail
- writer (now concat+IPC inline) p50/p99: 913 / 1680 ms
- accumulate p50/p99: 5.5 / 51.9 ms

`partition_by_hash` is now the long pole on the map side. Per-map-task cost varies 5× between p50 and p99. Worth instrumenting *inside* partition_by_hash to see whether it's hash-eval, scatter, or downstream MicroPartition construction; and whether the tail is data skew or scheduler contention.

### 2. Reduce per-source-batch IPC decode cost on the read side

After splitting `OPEN_US` into real buckets (see "Where the read time actually goes"), the read-side wall is dominated by **decode** (1.93 s/reducer, 18 us × 1.6M source batches), not opens (1 us/open) and not network. Two complementary angles:

- **Skip the per-range schema re-parse.** Currently each ranged spec inside `concat_specs_into_flight_datas_sync` builds a fresh `StreamReader::try_new(chain(schema_header, body))`, which re-parses the schema flatbuffer. That's the entire `stream_init` line (254 ms/reducer). Use `arrow_ipc::reader::read_record_batch` (or hold one `StreamReader` open across all ranges of the same file and seek directly) so the per-range overhead drops.
- **Coalesce write-side batches AND coalesce read-side `do_get` calls in concert.** Prior K-sweep showed K-coalesce regresses read time because partitions inside a K-group are fetched by different `do_get`s (=> the shared batch is re-decoded N times). If the bench / production caller batched K output partitions into a single `do_get`, the per-batch decode work would amortize K-fold over the source-batch count. The seal_bench reducer currently does one `do_get` per output partition.

### 3. Cheaper `concat_or_get` for shuffle-shaped input

Now 21 us/future × 8192 = 173 ms/task. Less urgent than (1)/(2) but still ~18% of writer-side wall. Skip `MicroPartition::concat` when `parts.len() == 1` (common case in production); pre-concat at sink time if feasible.

### 4. Eliminate the inner `try_into` arrow→arrow conversion

`daft_recordbatch::RecordBatch::try_into::<arrow_array::RecordBatch>()` is 4 us/partition = 38 ms/task. Cheap to fix if the IPC writer can borrow the daft RB's Arrow arrays directly.

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
- **N=8192, M=200, 8 GiB, NVMe, cold-read, oneshot/never + read-side concat + spawn-removed = 27.5 s total** (map 22.6 / read 4.8). Map throughput 362 MiB/s in-mem.
- Prior baselines for reference:
  - Initial (no concat, no spawn-removed): 86 s
  - After read-side concat: 68 s
  - After spawn-removed: **27.5 s**
- 4 s floor for pure read+write of 8 GiB at NVMe ~2 GB/s. We're ~7× above floor.
