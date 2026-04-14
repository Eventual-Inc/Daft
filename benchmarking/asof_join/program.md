# asof join autoresearch

This is an experiment to have an LLM autonomously optimize Daft's asof join performance.

## Setup

The repo is Daft (a distributed query engine in Rust + Python). The asof join implementation lives in:

- **`src/daft-recordbatch/src/ops/joins/mod.rs`** - `RecordBatch::asof_join()` - the main function you modify. Hash-partitions by "by" keys, sorts within each bucket, then merges.
- **`src/daft-recordbatch/src/ops/joins/asof_join.rs`** - `asof_join_backward()` - the linear-scan merge algorithm. Fair game to modify.
- **`src/daft-local-execution/src/join/asof_join.rs`** - the execution operator. Currently single-threaded (`max_probe_concurrency = 1`), concatenates all partitions before joining.

Supporting files (read-only context):
- `src/daft-recordbatch/src/ops/partition.rs` - `partition_by_hash`, `partition_by_value`
- `src/daft-recordbatch/src/ops/sort.rs` - sort implementation
- `src/daft-core/src/series/ops/hash.rs` - hash functions on Series

## What you're optimizing

**Metric**: wall-clock time for `daft_native` asof join on the benchmark. Lower is better.

**Benchmark command** (run from ~/Daft):
```bash
source .venv/bin/activate && python -m benchmarking.asof_join --scale small --local_data_dir /tmp/asof_data --systems daft_native --n_runs 3 --skip_warmup
```

The benchmark reads unsorted parquet files (1M left rows, 10M right rows, 10K entities), runs the asof join, and reports median wall time.

**Correctness check**:
```bash
source .venv/bin/activate && DAFT_RUNNER=native make test EXTRA_ARGS="-x tests/dataframe/test_asof_join.py"
```

All 52 tests must pass. A faster but incorrect join is worthless.

## Build

After modifying Rust code:
```bash
source ~/.cargo/env && make build-release > /tmp/build.log 2>&1
```

Incremental builds take ~1-2 minutes (only the changed crate recompiles). Full rebuilds take ~15 minutes.

## What you CAN modify

- `src/daft-recordbatch/src/ops/joins/mod.rs` - the asof_join method
- `src/daft-recordbatch/src/ops/joins/asof_join.rs` - the merge algorithm
- `src/daft-local-execution/src/join/asof_join.rs` - the execution operator

## What you CANNOT modify

- Test files
- Benchmark harness files (benchmarking/asof_join/)
- prepare.py / data_generation.py
- Python API surface

## Current state and known bottlenecks

**Current algorithm**: Hash-partition both sides by "by" keys into ~N buckets (adaptive, targeting 50K rows/bucket). Within each bucket, sort by [by_keys, on_key] then linear-scan merge.

**Baseline performance** (this machine): TBD after first run.

**Known bottlenecks**:
1. **String sort within buckets**: Hash collisions mean multiple entities per bucket, so we still sort by the string "entity" column. DuckDB avoids this by using fine-grained partitioning where each bucket has exactly one entity, then sorting only by the int64 "on" key.
2. **Single-threaded**: The entire join runs on one thread. DuckDB parallelizes both the partitioning (sink) and the per-bucket merge (source).
3. **Linear scan**: `asof_join_backward` does a linear scan. DuckDB uses exponential search + binary search within sorted runs.
4. **Full materialization**: The operator concatenates ALL partitions into one RecordBatch before joining. No streaming.

**DuckDB's approach** (reference):
1. Hash partition by BY keys into fine-grained bins
2. Sort within each bin by ON key only (no string sort)
3. Exponential search + binary search for matching
4. Parallel sink and source

## The experiment loop

LOOP FOREVER:

1. Look at the current state: read `results.tsv`, the current code in `mod.rs` and `asof_join.rs`
2. Pick an optimization idea. Prioritize ideas with the highest expected impact.
3. Modify the Rust code.
4. Build: `source ~/.cargo/env && make build-release > /tmp/build.log 2>&1`
5. Check for build errors: `grep -i error /tmp/build.log | head -10`
6. If build fails, fix and rebuild. If stuck after 3 attempts, revert and try something else.
7. Run tests: `source .venv/bin/activate && DAFT_RUNNER=native make test EXTRA_ARGS="-x tests/dataframe/test_asof_join.py" > /tmp/test.log 2>&1`
8. Check test results: `tail -5 /tmp/test.log`
9. If tests fail, fix or revert.
10. Run benchmark: `source .venv/bin/activate && python -m benchmarking.asof_join --scale small --local_data_dir /tmp/asof_data --systems daft_native --n_runs 3 --skip_warmup 2>&1 | tee /tmp/bench.log`
11. Extract result: `grep "daft_native" /tmp/bench.log`
12. Log to results.tsv
13. If improved: `git add -A && git commit -m "perf(asof-join): <description>"`
14. If not improved: `git checkout -- src/`
15. Go to 1.

**NEVER STOP.** The human may be asleep. Keep experimenting until manually interrupted.

## results.tsv format

Tab-separated, 5 columns:
```
commit	med_time_s	status	scale	description
```

- commit: short git hash (7 chars)
- med_time_s: median wall time in seconds
- status: `keep`, `discard`, or `crash`
- scale: `small` or `medium`
- description: what the experiment tried
