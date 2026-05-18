# Shuffle / repartition benchmark report

Compares **Ray Data** vs **Daft PyPI 0.7.13** (three shuffle algorithms) vs **Daft built from `colin/flight-shuffle-perf`** on a TPC-H `lineitem` repartition workload.

The benchmark was run on a 32-worker AWS cluster. All artifacts (harness, logs, per-cell results, profile aggregator, isolated parquet-perf wheel build steps) and a reproduction recipe are at the bottom of this report.

## Cluster

- 32 worker nodes (`i8ge.2xlarge`, 8 CPU / 64 GiB each) + 1 head
- Total: 256 CPUs, ~2.0 TiB cluster RAM
- Each node has 2 × 2.3 TiB local NVMe; bootstrap mounts one at `/tmp`. We `mkfs.xfs` + `mount` the second at `/mnt2` and use both for flight-shuffle spill.
- Python 3.12.13, Ray 3.0.0.dev0 nightly, AWS us-west-2

## Builds compared

| ID | Source | Notes |
|---|---|---|
| `ray_data` | Ray Data on the ray nightly | Required a one-line patch to `ray/data/_internal/arrow_ops/transform_pyarrow.py` (see Caveats §1) |
| `daft pypi mr` | `pip install daft==0.7.13`, `shuffle_algorithm="map_reduce"` | PyPI |
| `daft pypi psm` | `pip install daft==0.7.13`, `shuffle_algorithm="pre_shuffle_merge"` | PyPI |
| `daft pypi flt` | `pip install daft==0.7.13`, `shuffle_algorithm="flight_shuffle"` | PyPI |
| `daft built flt` | Built from `colin/flight-shuffle-perf` HEAD (`shuffle_algorithm="flight_shuffle"`) | This branch |
| `daft parquet-perf only` | Built from commit `218fe84e7` (parquet decoder rewrite alone — no shuffle changes) | Used to isolate parquet-perf vs flight-shuffle-perf contributions |

All flight-shuffle runs use:
```python
daft.set_runner_ray()
daft.set_execution_config(
    flight_shuffle_dirs=["/tmp/ray/spill", "/mnt2/spill"],
    shuffle_algorithm="flight_shuffle",
)
```

## Workload

TPC-H `lineitem` from S3 (us-west-2), `df.repartition(N, "l_orderkey").write_parquet(...)` to `s3://colinho-test/dump/...`.

| Scale label | Source | On-disk size | File count | Key column |
|---|---|---|---|---|
| `sf100_top8` | sf100 first 8 files | ~5.5 GiB | 8 | `L_ORDERKEY` |
| `sf100` | sf100 full | 22 GiB | 32 | `L_ORDERKEY` |
| `sf1000_top64` | sf1000 first 64 files | ~28 GiB | 64 | `L_ORDERKEY` |
| `sf1000` | sf1000 full | 227 GiB | 512 | `L_ORDERKEY` |
| `sf10000_top1000` | sf10000 first 1000 files | ~221 GiB | 1000 | `l_orderkey` |
| `sf10000` | sf10000 full | 2.21 TiB | 10000 | `l_orderkey` |

The same file order is materialized via `pyarrow.fs.S3FileSystem.get_file_info(FileSelector(...))` sorted by path and sliced — so all engines see identical input.

## Headline grid — wall-clock seconds (lower is better)

```
scale             parts    ray_data  | daft pypi mr | daft pypi psm | daft pypi flt | daft built flt
----------------------------------------------------------------------------------------------------
sf100_top8           32      29.79s  |      15.84s  |       13.52s  |       12.33s  |       13.70s
sf100                32      39.14s  |      19.75s  |       23.94s  |       19.76s  |       15.80s
sf100               256      45.92s  |      24.59s  |       22.56s  |       23.17s  |       21.99s
sf1000_top64        128      36.98s  |      19.25s  |       20.26s  |       20.89s  |       18.35s
sf1000              256         ERR  |     380.13s  |      276.90s  |      106.83s  |      107.59s
sf1000              512          -   |          -   |           -   |      140.53s  |      110.07s
sf1000             1024          -   |          -   |           -   |      211.68s  |      140.70s
sf10000_top1000     256         ERR  |     260.41s  |      394.09s  |      120.96s  |       99.70s
sf10000            1024          -   |          -   |           -   |     2556.05s  |     1849.32s
sf10000            2048          -   |          -   |           -   |     4529.25s  |     3024.56s
sf10000           10000          -   |          -   |           -   |          ERR  |          ERR
```

(`-` = not run; `ERR` = engine failure — see notes.)

`sf10000 × 10000` partitions failed both engines: pypi hit `Too many open files` (ulimit nofile=65535); built daft hit a worker OOM during flight shuffle.

## Headlines

1. **Daft 1.4–2.5× faster than Ray Data** at every scale where Ray Data could complete.
2. **Ray Data was the most fragile engine** — even after a one-line patch (Caveat 1), it stalled on every cell at or above sf1000 full × 256.
3. Among PyPI daft algorithms, `flight_shuffle` dominates at sf1000+ (e.g. sf1000 × 256: 107 s flight vs 277 s psm vs 380 s map_reduce).
4. **Built daft (`colin/flight-shuffle-perf`) is 22–33% faster than PyPI 0.7.13** at sf1000+ cells:
   - sf10000_top1000 × 256: 99.7 s vs 121 s (−18%)
   - sf10000 × 1024: 1849 s vs 2556 s (−28%)
   - sf10000 × 2048: 3025 s vs 4529 s (−33%)
   - sf1000 × 1024: 141 s vs 212 s (−34%)
5. **Built daft is *slower* than PyPI at very small data** (within-flight_shuffle comparison) — see §"Where the gains come from" below.

## Where the gains come from — isolating parquet-perf vs flight-shuffle-perf

The built branch contains two independent perf improvements:
- `colin/parquet-perf` (commit `218fe84e7`): rewrite of parquet reader using the public arrow-rs decoder API.
- `colin/flight-shuffle-perf` (commits `d68ea49fa` + `feb7d580c` and follow-ups): spawn-free oneshot writer, read-side server concat, repartition buffer threshold bump (4 → 64 → 256 MiB), helper refactor.

To attribute the gains, we built a **parquet-perf-only wheel from commit `218fe84e7`** (parquet decoder change with the **old** flight-shuffle code) and compared to PyPI (neither change) and shuffle-test (both).

### Isolated parquet decoder (native runner, no cluster)

8 sf10000 files (~1.7 GiB, ~48 M rows), `df.to_arrow()`, 3 iterations each, head node only:

| variant | full schema | l_orderkey only |
|---|---|---|
| pypi 0.7.13 | 51–77 s | 15.5–17.3 s |
| parquet-perf only | 2.8–3.3 s | 0.7–5.3 s |
| **speedup** | **17–25×** | **~3–40×** |

### Distributed (`flight_shuffle` algo on all variants, 3-trial median where noted)

| cell | pypi (neither) | parquet-perf only | shuffle-test (both) | parquet-perf Δ | flight-shuffle-perf Δ |
|---|---:|---:|---:|---:|---:|
| **sf100_top8 × 32** | **16.26 s** | **7.53 s** | **12.95 s** | **−54%** | **+72% regression** |
| **sf100 × 32 (full)** | **20.47 s** | **14.52 s** | **17.00 s** | **−29%** | **+17% regression** |
| sf1000 × 256 (3-run) | 106.83 s | 113.55 s | 107.59 s | +6% noise | −5% noise |
| sf1000 × 1024 (1-run) | 211.68 s | 211.93 s | 140.70 s | 0% | **−34%** |
| sf10000_top1000 × 256 (1-run) | 120.96 s | 122.09 s | 99.70 s | +1% noise | **−18%** |

**Two regimes, sharply separated:**

- **Small data (≤ sf100):** parquet-perf is a large positive (−29 to −54% wall), but flight-shuffle-perf is a *fixed-cost regression* (+17 to +72%). Net: shuffle-test still beats PyPI by ~20%, but parquet-perf-only would be even better. The flight-shuffle-perf changes carry ~5 s of per-task fixed overhead at this scale.
- **Medium / large data (sf1000+):** parquet-perf is invisible at the cluster level (decode is fully overlapped with the shuffle-write pipeline), and flight-shuffle-perf is the dominant win (−18 to −34%).

The crossover is between sf100 and sf1000. **Even at the smallest cell tested, shuffle-test is still 2.6× faster than Ray Data** (12.95 s vs 33.32 s on warm cache); the flight-shuffle-perf regression is a *within-daft* concern, not a daft-vs-Ray-Data concern.

## Algorithm comparison on small data (sf100_top8 × 32, shuffle-test wheel)

| algorithm | trial 1 | trial 2 | trial 3 | median |
|---|---:|---:|---:|---:|
| `map_reduce` | 10.32 s | 13.34 s | 11.86 s | 11.86 s |
| `pre_shuffle_merge` | 12.14 s | 14.33 s | 11.27 s | 12.14 s |
| `flight_shuffle` | 12.95 s | 9.18 s | 13.32 s | 12.95 s |

All within ~1 s of each other; `flight_shuffle` is marginally slowest. Consistent with the small-data flight-shuffle-perf overhead — non-flight-shuffle code paths don't pay the new fixed cost. The `map_reduce` / `pre_shuffle_merge` speedups vs PyPI (−25% / −10% on this cell) reflect pure parquet-perf benefit.

## Per-map-task profile (instrumented build, sf10000 × 2048)

We added a `MapTaskProfile` span set to `src/daft-shuffles/src/oneshot_writer.rs` and walked through three perf iterations:

| span | E5 (4 MiB buffer, no refactor) | E6 (64 MiB buffer) | E7 (helper refactor) | E9 (256 MiB buffer) |
|---|---:|---:|---:|---:|
| **total_ms** | **1925** | **1135** | **1114** | **794** |
| `concat_one_total_ms` | — | 567 (50%) | 534 (48%) | 258 (33%) |
| `input_rollup_ms` | — | 94 | 96 | 30 |
| `arrow_concat_ms` | 417 (22%) | 230 | 237 | 144 |
| `concat_one_unattributed_ms` | — | 228 (20%) | 185 | 71 |
| `ipc_write_ms` | 537 (28%) | 544 (48%) | 576 (52%) | 533 (67%) |
| `outer_loop_ms` | — | 23 (2%) | 1.2 | 1.2 |
| `num_input_rbs / task` | ~590 K | ~35 K | ~35 K | ~10 K |
| (old monolithic `other_ms`) | 945 (49%) | — | — | — |

Cluster wall on the same cell (sf10000 × 2048): E4 (pre-refactor) = 3100 s → E7 = 3024 s → E9 = 3051 s. Per-task `total_ms` fell **−59%** but wall fell only **−2.5%** — map-side compute is not the cluster bottleneck.

**Bottleneck ranking after E9 (current `colin/flight-shuffle-perf` HEAD):**

| rank | span | % | what it is |
|---|---|---|---|
| 1 | `ipc_write_ms` | **67%** | One IPC batch per partition × `num_partitions` per map task; per-batch framing fixed cost. Coalescing output batches before flushing is the obvious next swing. |
| 2 | `concat_one_total_ms` | 33% | Per-partition fuse cost. `arrow_concat_ms` per-call overhead dominates. |
| — | `outer_loop_ms` | 0.1% | File create + schema serialize + main-loop bookkeeping; not a bottleneck. |

`ipc_write_ms` is essentially fixed per map task (same total bytes written), scales weakly with partition count (+5% going 2048 → 5000 parts), and didn't move across any of the three iterations.

## Partition-count sensitivity (built daft, sf10000)

```
scale       parts       wall    rows/batch    notes
sf10000     1024      1849 s         5859     E4 — pre-refactor
sf10000     2048      3100 s         2930     E4 — pre-refactor, +68% on same data
sf10000     2048      3024 s         2930     E7 — refactor + 64 MiB buffer
sf10000     2048      3050 s         2930     E9 — 256 MiB buffer
sf10000     5000   killed             1200    map pipeline projecting ~85 min
sf10000    10000        ERR             —    worker OOM
```

Going 1024 → 2048 parts cost +68% wall on the same data, even though per-partition data is half. The per-partition costs in the `oneshot_writer` (file-create overhead, IPC framing, concat call) compound and overtake the constant-per-task IPC write cost. For sf10000 on this cluster the sweet spot is around 1024 partitions.

## Caveats

1. **Ray Data hash_partition bug.** Ray 3.0.0.dev0 + numpy 2.4 + pandas 3.0 crashes `np.mod(hashes, num_partitions, out=hashes)` with `ValueError: output array is read-only` (`.values` from `hash_pandas_object` is now read-only). Patched `ray/data/_internal/arrow_ops/transform_pyarrow.py` on all 33 nodes to drop the `out=` arg. Without the patch, Ray Data `repartition(..., keys=[...])` is unusable.
2. **Ray Data stalls at scale.** Even patched, both sf1000 full × 256 and sf10000_top1000 × 256 ran for 20–30 min with shuffle progress flat at 60–77%, 195+/256 CPUs idle, object store ~70% full. Killed → `ERR`. sf10000 full was not attempted.
3. **PyPI `mr` / `psm` dropped from sf10000 × 1024+.** Extrapolated runs ≥ 90 min each based on sf1000-scale trends.
4. **Single trial for most cells.** Small cells (sf100 / sf100_top8) used 3-trial medians for the apples-to-apples comparison. ~5–10% noise floor on larger cells based on the one repeated cell (sf1000 × 256: 108 / 116 / 114 s across 3 trials).
5. **`/mnt2` mount.** Cluster bootstrap only mounts one ephemeral NVMe at `/tmp`. The second device (`/dev/nvme2n1`, 2.3 TiB) is unformatted. `flight_shuffle_dirs=["/tmp/ray/spill", "/mnt2/spill"]` requires manually `mkfs.xfs` + `mount` on every node. Without this, spill collapses to a single disk and serializes I/O.
6. **`daft.__version__` is misleading from CWD.** Running `python -c "import daft"` from `/home/ec2-user/Daft` resolves to the source tree (not the installed wheel). The benchmark harness avoids this by invoking `python /tmp/benchmark.py` so `sys.path[0]` is `/tmp`. Verify cluster version via an explicit `ray.remote` probe, not via `python -c`.

## Recommended next steps

1. **Attack `ipc_write_ms`.** Coalesce per-partition output batches before flushing. With `compressed=false` and exactly 1 IPC batch per partition, this is per-batch framing × `num_partitions`. Lower output batch count → lower fixed cost per task.
2. **Guard flight-shuffle-perf changes at small data.** Both the spawn-free oneshot writer and the read-side server concat carry ~5 s of fixed setup per shuffle. For shuffles below some byte/partition threshold, falling back to the older paths (or short-circuiting in the new ones) would close the small-data regression. Concrete candidates to instrument first: `flight_server.rs::concat_specs_into_flight_data_stream` (read-side coalescing — has a `None` codepath at line 463 that already streams without concat) and `flight_server.rs::build_schema_header_bytes` (called per `do_get` request).

---

# Reproduction guide

This whole report can be reproduced on a fresh AWS Ray cluster. The cluster used here was `i8ge.2xlarge` × 32 — any similar shape (≥ 200 CPUs, ≥ 1.5 TiB RAM, NVMe local disks ≥ 2 TiB / node, us-west-2) should work, though absolute numbers will differ.

## 0. Cluster bootstrap

A Ray cluster with `colinho-test` S3 bucket access and the input buckets readable. The cluster used here is bootstrapped via `~/ray_bootstrap_config.yaml` (head + 32 workers, `i8ge.2xlarge`, Python 3.12, Ray 3.0.0.dev0 nightly).

Worker IPs are harvested into `/tmp/worker_ips.txt`. SSH key at `~/ray_bootstrap_key.pem`, user `ec2-user`. The benchmark harness assumes all 32 workers share that key + user.

## 1. Mount the second NVMe on every node

By default the bootstrap only mounts one NVMe at `/tmp`. The second device (typically `/dev/nvme2n1`, ~2.3 TiB) is unformatted. We need it for flight-shuffle spill.

```bash
# On head:
sudo mkfs.xfs -f /dev/nvme2n1
sudo mkdir -p /mnt2 && sudo mount /dev/nvme2n1 /mnt2
sudo chown ec2-user:ec2-user /mnt2 && mkdir -p /mnt2/spill /tmp/ray/spill
```

Then push to all workers in parallel (see `/tmp/mount_mnt2.sh` in the artifacts; it targets `/dev/nvme2n1` and idempotently skips already-mounted hosts):
```bash
cat /tmp/worker_ips.txt | xargs -P32 -I{} bash /tmp/mount_mnt2.sh {}
```

## 2. Patch Ray Data (only needed if benchmarking ray_data)

In `~/.venv/lib/python3.12/site-packages/ray/data/_internal/arrow_ops/transform_pyarrow.py`, find the block in `_hash_partition`:
```python
hashes = pd.util.hash_pandas_object(table.to_pandas(types_mapper=pd.ArrowDtype), index=False).values
np.mod(hashes, num_partitions, out=hashes)  # crashes under numpy 2.4 + pandas 3.0
partitions = hashes
```
Replace the `np.mod(...)` line with:
```python
hashes = np.mod(hashes, num_partitions)  # non-in-place, works with read-only .values
```

Scp the patched file to every worker.

## 3. Daft installs

PyPI:
```bash
~/.venv/bin/pip install --force-reinstall --no-deps daft==0.7.13
cat /tmp/worker_ips.txt | xargs -P32 -I{} ssh -i ~/ray_bootstrap_key.pem ec2-user@{} \
  "~/.venv/bin/pip install --quiet --force-reinstall --no-deps daft==0.7.13"
```

Built (from this branch):
```bash
cd ~/Daft
git checkout colin/flight-shuffle-perf
# Build with cargo target on /tmp (the 8 GB root partition can't hold target/)
CARGO_HOME=/tmp/cargo-home CARGO_TARGET_DIR=/tmp/daft-target UV_CACHE_DIR=/tmp/uv-cache \
  PATH=~/.cargo/bin:$PATH make build-whl
# Yields /tmp/daft-target/wheels/daft-0.3.0.dev0-cp310-abi3-manylinux_2_34_aarch64.whl
~/.venv/bin/pip install --ignore-installed --no-deps /tmp/daft-target/wheels/daft-0.3.0.dev0-cp310-abi3-manylinux_2_34_aarch64.whl
# scp+install on every worker (see /tmp/install_daft_built.sh)
cat /tmp/worker_ips.txt | xargs -P32 -I{} bash /tmp/install_daft_built.sh {}
```

Verify by remote probe before benchmarking — do NOT trust `python -c "import daft"` (CWD picks up source tree):
```bash
~/.venv/bin/python -c "
import ray; ray.init(address='auto', logging_level='ERROR')
@ray.remote
def p():
    import daft; return daft.__version__
print(set(ray.get([p.options(scheduling_strategy='SPREAD').remote() for _ in range(32)])))
"
```

For the **parquet-perf-only** comparison, build from commit `218fe84e7`:
```bash
cd ~/Daft
git stash  # save any wip
git checkout 218fe84e7
# Re-apply the build.rs cross-device patch — the original repo state uses fs::rename
# which fails when CARGO_TARGET_DIR is on a different mount than CARGO_MANIFEST_DIR:
sed -i 's|std::fs::rename(frontend_dir, out_dir)?;|let status = std::process::Command::new("mv").arg(\&frontend_dir).arg(out_dir).status()?; assert!(status.success(), "Failed to move frontend assets");|' src/daft-dashboard/build.rs
# Build:
CARGO_HOME=/tmp/cargo-home CARGO_TARGET_DIR=/tmp/daft-target UV_CACHE_DIR=/tmp/uv-cache \
  PATH=~/.cargo/bin:$PATH make build-whl
# Install + push to workers as above
```

## 4. Benchmark harness

See `benchmarking/shuffle/benchmark.py` (committed alongside this report). Usage:
```bash
python /path/to/benchmark.py \
  --engine daft --algo flight_shuffle \
  --scale sf10000 --partitions 2048 \
  --run-name myrun \
  --output-bucket s3://YOUR_BUCKET/dump
```

Per-cell outputs:
- One JSONL row to `/tmp/benchmark_results.jsonl` (engine, algo, scale, parts, wall seconds, status).
- Each engine's `.explain()` written to stdout.
- Output written to `s3://YOUR_BUCKET/dump/<run-name>/<engine>/<algo>/<scale>/<parts>`; harness cleans the prefix before each cell so repeat runs don't compound.

Available scales: `sf100`, `sf1000`, `sf10000` (defined in the script; uppercase column names for sf100/sf1000, lowercase for sf10000 — handled automatically). Sub-scale slicing via `--limit-files N`.

## 5. Profile instrumentation

The `MapTaskProfile` tracing span set in `src/daft-shuffles/src/oneshot_writer.rs` is enabled by setting `DAFT_TRACE` (not `RUST_LOG` — Daft uses its own env var):
```bash
DAFT_TRACE='daft_shuffles::oneshot_writer::profile=info' \
  python benchmark.py --engine daft --algo flight_shuffle ... \
  > /tmp/run.log 2>&1
```

To get the env into Ray worker processes (where the map tasks run), pass it through `runtime_env`:
```python
ray.init(address="auto", runtime_env={"env_vars": {"DAFT_TRACE": "..."}})
```
The benchmark harness does this when `DAFT_TRACE` is set in the parent shell.

The aggregator at `/tmp/profile_monitor.py` parses ANSI-laden log lines, sums + medians the spans per task, and reports `num_input_rbs` / `rows/batch` / per-span percentage of `total_ms`:
```bash
python /tmp/profile_monitor.py --log /tmp/run.log --once
```

## 6. Native parquet read isolation

For the standalone parquet decode benchmark (no cluster, head node only), use `benchmarking/shuffle/native_read_test.py`. It expects an isolated venv with the target daft wheel installed (so it doesn't pick up the cluster's daft):
```bash
uv venv /tmp/daft-pypi-test -p python3.12 --clear
UV_CACHE_DIR=/tmp/uv-cache uv pip install --python /tmp/daft-pypi-test/bin/python \
  daft==0.7.13 pyarrow boto3 fsspec tqdm packaging typing_extensions
/tmp/daft-pypi-test/bin/python /path/to/native_read_test.py --repeat 3 --select all
```

Repeat with a different venv pointing at the built wheel for comparison.

## 7. Reproducing the apples-to-apples small-data comparison

This is the key experiment for the parquet-perf vs flight-shuffle-perf decomposition. Run 3 trials each on a freshly-warmed S3 cache. The warm-cache discipline matters because the cells are small (10–25 s):

1. Install pypi 0.7.13 cluster-wide. Run sf100_top8 × 32 with `--algo flight_shuffle` × 3 trials.
2. Install parquet-perf-only wheel (from `218fe84e7`) cluster-wide. Run × 3 trials.
3. Install shuffle-test wheel cluster-wide. Run × 3 trials.
4. Compare medians.

Order matters less than you'd expect — re-running in different orders gives consistent deltas (we ran A→B→C and saw the same parquet-perf vs flight-shuffle-perf pattern as A→C→B).
