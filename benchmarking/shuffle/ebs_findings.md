# Shuffle on EBS-only Ray cluster — findings

Empirical study of Daft's three shuffle algorithms (`flight_shuffle`, `map_reduce`,
`pre_shuffle_merge`) vs Ray Data, on a 32-worker AWS Ray cluster with **only EBS
gp3 storage** (no NVMe). Builds on `shuffle_benchmark_report.md` which used an
NVMe-equipped cluster; the goal here is to understand which paths still hold up
when local storage is network-attached gp3.

Wheel built from `colin/shuffle-testing` (latest `colin/flight-shuffle-perf` +
`colin/parquet-perf` merged into `main`).

## Cluster shape

- 32 worker nodes: `m5.2xlarge` (8 vCPU / 32 GiB RAM / 1 TiB gp3 root)
- 1 head node: `m5.2xlarge`
- Total: 256 CPUs, ~792 GiB cluster RAM, ~318 GiB object store
- **No NVMe.** Everything (including `flight_shuffle_dirs`) lives on the EBS gp3
  root volume mounted at `/opt/daft-spill`.
- Python 3.12.13, Ray 3.0.0.dev0, AWS us-west-2

## Methodology

- TPC-H lineitem from S3, sized by `.limit(target_rows)` with
  `APPROX_BYTES_PER_ROW=145`. Source: `eventual-dev-benchmarking-fixtures`
  (sf1000 for ≤170 GB, sf10000 for 256–512 GB).
- Daft: `df.repartition(N, "L_ORDERKEY").write_parquet(out_dir)`,
  `flight_shuffle_dirs=["/opt/daft-spill"]` for the flight backend.
- Ray Data: `ds.repartition(N, keys=[...]).materialize()`. (Switched from
  `write_parquet` after observing it added ~36% wall vs materialize-only on
  one apples-to-apples cell.)
- Ray Data required a one-line patch to
  `ray/data/_internal/arrow_ops/transform_pyarrow.py` — `np.mod(.., out=hashes)`
  crashes under numpy 2.4 / pandas 3.0 because the `.values` array is read-only.
- Outputs cleaned cluster-wide between cells via a Ray remote `shutil.rmtree`
  hitting every alive node ID.
- Single trial per cell (the cluster was warm; observed noise on a repeated
  flight_shuffle row was within 3-5%). One ray_data cell was re-run twice and
  showed 152s vs 340s with ~5 min of autoscaler chatter explaining the second —
  treat individual Ray Data numbers as having ±2× variance until verified.

## Headline grid — wall seconds (lower is better)

```
 size  parts | daft flt  daft mr  daft psm  ray_data
-----------------------------------------------------
   50    100 |   30.9s    27.5s     29.7s    48.1s
   50    200 |   36.0s    29.0s     28.9s   119.0s
   50    500 |   53.5s    36.1s     29.0s   260.4s
   50   1000 |   84.1s    58.2s     32.4s   528.5s
  100    100 |   35.8s    34.6s     37.4s   101.7s
  100    200 |   42.0s    34.5s     45.5s   218.6s
  100    500 |   59.3s    40.8s     50.8s   527.3s
  100   1000 |   98.0s    56.3s     44.7s   338.7s
  170    100 |   67.9s    85.1s     86.9s   144.9s
  170    200 |   61.8s    67.4s     77.6s   240.3s
  170    500 |   82.9s    48.7s     74.4s   329.9s
  170   1000 |  123.9s    63.4s     76.3s   515.8s
  256    100 |  176.0s     ERR       ERR    880.1s
  256    200 |  178.6s     ERR       ERR     —
  256    500 |  219.3s     ERR       ERR     —
  256   1000 |  224.1s     ERR       ERR     —
  512    100 |  441.3s     ERR       ERR     —
  512    200 |  447.1s     ERR       ERR     —
  512    500 |  446.8s     ERR       ERR     —
  512   1000 |  459.2s     ERR       ERR     —
```

`ERR` = engine OOM (see Failure modes below). `—` = not run after the engine
failed at lower scale.

## Headlines

1. **flight_shuffle is the only daft algorithm that survives at 256+ GB on
   this cluster.** `map_reduce` and `pre_shuffle_merge` both OOM workers at the
   256 GB / 100p cell because they materialize shuffle intermediates in Ray's
   object store; with 30 GB workers and 318 GB cluster object store, holding a
   256 GB shuffle is over budget. flight_shuffle bypasses the object store by
   writing IPC files directly to `/opt/daft-spill` and streaming reads back via
   Arrow Flight.

2. **flight_shuffle is strikingly flat across partition counts at 256/512 GB.**
   At 512 GB, 100p vs 1000p differs by **4%** (441s vs 459s). Partition
   selection is essentially free at large scale — disk throughput dominates.
   At 50/100 GB the partition cost is large (30.9s → 84.1s at 50 GB, +172%);
   the small-data flight_shuffle regression flagged in `shuffle_benchmark_report.md`
   reproduces here.

3. **At 50–170 GB, `map_reduce` and `pre_shuffle_merge` beat flight_shuffle**
   at all but the lowest partition counts:
   - 170 GB / 500p: mr 48.7s vs flt 82.9s (-41%)
   - 170 GB / 1000p: mr 63.4s vs flt 123.9s (-49%)
   - The crossover where flight_shuffle starts winning is around 170 GB / 200p;
     above that, flight_shuffle wins through stability rather than speed.

4. **Ray Data 2–10× slower than the best daft algo at every comparable cell.**
   Ray Data's partition-count scaling is brutal: 50 GB on the same data goes
   48s → 119s → 260s → 528s as parts go 100 → 200 → 500 → 1000. Even after
   switching to `materialize()` (no write), 256 GB / 100p still cost 880s,
   ~5× the daft flight_shuffle run on the same cell.

5. **Ray Data run noise floor is ~30-50 seconds** of autoscaler "No available
   node types" stalls at the start of every run, even when the cluster has not
   changed. Two consecutive runs of the same Ray Data cell measured 340s vs
   152s; the gap is entirely cluster autoscaler chatter, not the engine. Daft
   does not show this pattern.

## Failure modes at scale

**`map_reduce` 256 GB / 100p:** worker OOM. A randomly-selected worker (e.g.
172.31.63.145) hits 95% of its 31 GB physical memory threshold and Ray kills
the actor. The actor that died was `_LimitCounterImpl` (the
`DistributedLimit` counter), but the root cause is that map_reduce's shuffle
intermediates live in the object store and 32 × ~10 GB of object store fills
up under spilling pressure, causing memory contention with the actor processes.

**`pre_shuffle_merge` 256 GB / 100p:** identical pattern — worker OOM at 30 GB,
shuffle intermediates pinning the object store. The "ShuffleRead->PhysicalWrite"
actor used 8.84 GB resident at time of kill, plus the partition_caches in the
local server held more.

**`flight_shuffle` survives** because per-partition IPC files are written to
disk in `oneshot_writer.rs::write_partitions_one_shot` at the finalize step,
freeing the `RepartitionAccState::post_repartitioned` memory before the reduce
phase begins. No object-store fan-out.

## The actual bottleneck: gp3 throughput ceiling

Ran the 512 GB / 1000p cell again with `iostat -dx 2` + `sar -n DEV 2` on one
worker for the full 460s. Single worker, but the disk-throughput story is
unambiguous:

| metric                | median   | p95       | peak      | gp3 baseline |
|-----------------------|---------:|----------:|----------:|-------------:|
| **write throughput**  | **125.1 MB/s** | 125.3 MB/s | 134.7 MB/s | **125 MB/s** |
| read throughput       | 0 MB/s   | 125.2 MB/s | 180.4 MB/s | 125 MB/s |
| write IOPS            | 522      | 590       | 642       | 3000         |
| read IOPS             | 0        | 1370      | 1999      | 3000         |
| **disk %util**        | **98%**  | 100%      | 100%      | —            |
| net rx                | 98 MB/s  | 354 MB/s  | 796 MB/s  | ~3 GB/s      |
| net tx                | 16 MB/s  | 252 MB/s  | 906 MB/s  | ~3 GB/s      |

Writes are **pinned at the gp3 throughput baseline of 125 MB/s with 98% disk
utilization at the median**. IOPS is fine. Network has 3-5× headroom. EBS write
throughput is the wall.

### Map vs reduce phase breakdown (one worker, 460s wall)

| phase                                | window     | duration | % of wall | dominant I/O                  |
|--------------------------------------|------------|---------:|----------:|-------------------------------|
| setup / S3 enum / ray init           | 0–68s      | 68s      | 15%       | idle                          |
| map phase (write IPC partitions)     | 68–284s    | 216s     | 47%       | write @ ~124 MB/s (at ceiling) |
| reduce phase (read partitions + write output) | 284–432s | 148s | 32% | mixed: r ~100 MB/s, w ~70-120 MB/s |
| output drain                         | 432–476s   | 44s      | 10%       | write tail                    |

- **Map is the heavier disk consumer** and the only phase that holds 125 MB/s.
- Reduce reads run at ~100 MB/s — below the ceiling, because they're
  overlapped with output writes and network sends.
- One worker wrote ~25 GB and read ~12 GB over the run; total cluster disk
  traffic ≈ 32 × 37 GB = 1.18 TB for a 512 GB shuffle (consistent with
  the expected ~2× = 1 TB plus metadata overhead).

### What this means for tuning

Provisioning gp3 throughput is the single biggest available win on this cluster
shape. Order-of-magnitude estimates:

| gp3 throughput | est. map phase | est. 512 GB wall | notes |
|----------------|---------------:|-----------------:|-------|
| 125 MB/s (default) | 216s         | 460s             | current |
| 250 MB/s           | ~108s        | ~350s            | -24%   |
| 500 MB/s           | ~54s         | ~300s            | -35%   |
| 1000 MB/s (gp3 max)| ~27s         | ~270s            | -41%, then setup+reduce dominates |

The setup+reduce time (~260s on the current run, of which ~68s is fixed setup)
becomes the limit past ~500 MB/s.

## Code-level observations on `daft-shuffles/src/server/`

The read side (flight_server.rs + stream.rs) is **better tuned for EBS than the
write side**:

✅ Already EBS-friendly
- Ranges sorted by start offset (kind to readahead)
- One FD per physical file via range coalescing
- `BufReader` wrapping the file
- Async I/O end-to-end (cf8ab3804)
- Streaming yields per batch; readers consume in parallel with disk reads

⚠️ EBS-specific tweaks worth trying
1. **Bump `BufReader::with_capacity` from default 8 KB → 1 MB.** On EBS each
   syscall pays a real network RTT; aggregating more bytes per syscall helps.
2. **Parallelize ranges within a file.** Currently `flight_server.rs:338-347`
   iterates ranges serially with `seek + take`. EBS has headroom for parallel
   in-flight ops (gp3 supports up to 16K provisioned IOPS).
3. **`posix_fadvise(POSIX_FADV_SEQUENTIAL)`** — free hint that bumps kernel
   readahead window. Near-zero gain on NVMe, real gain on EBS.
4. **Consider `tokio-uring`** — `tokio::fs::File` is backed by a blocking
   thread pool on Linux, so each in-flight read parks a worker until EBS
   responds.

The write side (`oneshot_writer.rs`, called from `repartition.rs::finalize`) is
where `ipc_write_ms` accounts for 67% of map-task time per the prior report's
profiling. Coalescing per-partition output batches before flushing remains the
#1 recommended next swing, and would benefit EBS more than NVMe (per-file
metadata ops are more expensive on EBS).

## Open questions / things not measured here

- **Network shuffle vs disk shuffle split**: `flight_shuffle` workers serve
  Arrow Flight to remote readers. We didn't measure the split between disk
  read time vs network send time during the reduce phase. Worker network peaks
  at 906 MB/s for 1-2s windows, but median is much lower (16 MB/s tx) — suggests
  it's not a sustained network problem.
- **Compression**: `flight_shuffle` supports lz4 and zstd IPC compression but
  the default is no compression. On EBS-bound workloads, trading CPU for ~2×
  effective disk throughput would likely be a large win. Not tested.
- **Provisioned throughput experiment**: the estimates above are linear
  extrapolations. Actual measurement requires re-running on a cluster with
  higher gp3 throughput provisioned.
- **`map_reduce` / `pre_shuffle_merge` at 256+ GB**: would require workers
  with > 30 GB RAM, or a Ray object store configuration that more aggressively
  spills before OOM. Not pursued here.

## Reproduction

The harness lives at `benchmarking/shuffle/`:
- `benchmark.py` — original single-cell driver
- `ebs_findings_harness.py` — drop-in harness used for this study (writes
  per-cell JSONL rows, cluster-wide cleanup between cells, supports
  `--ray-data-mode=materialize|write`)
- `install_daft_built.sh` — push a built wheel to all workers in parallel

Sample invocations:

```bash
# 512 GB / 1000p flight_shuffle on this cluster
python benchmarking/shuffle/ebs_findings_harness.py \
  --engine daft --algo flight_shuffle --size-gb 512 --partitions 1000

# Capture per-worker disk + network during a run
ssh worker.ip "iostat -dx 2 360 > /tmp/iostat.log &"
ssh worker.ip "sar -n DEV 2 360 > /tmp/sar_net.log &"
# ... launch the cell ...
```
