# MCAP pushdown benchmark results

Run date: 2026-07-09, Apple Silicon macOS, native Daft runner, Python 3.11.

## ABC-130k access status

The pinned 390 MB performance episode is gated and this environment has no
`HF_TOKEN`. The harness exited successfully without reading payload data:

```json
{
  "status": "skipped",
  "reason": "ABC-130k is gated. Accept its Hugging Face terms, then export HF_TOKEN before running; no data was read."
}
```

The exact deferred target is:

```text
hf://datasets/XDOF/ABC-130k@29136bc9b9e38d320b00ffcddbbe4cd0e3278c58/data/train/clip_the_socks_to_the_hanger/episode_0ab8f08a-eb80-40f4-99f7-3d83db25df83/episode.mcap
```

This is intentionally reported as an access blocker, not as a parser result.
Run the cold, warm, and local commands in the adjacent README after providing a
token to fill in the ABC-specific table.

## Instrumented range-I/O regression

The local HTTP regression writes an uncompressed, indexed MCAP with 128
independently indexed camera chunks and counts every byte served by a
range-capable server. The filtered query is expressed through Daft's planner:

```python
daft.read_mcap(url).where(daft.col("log_time") == 64).select("log_time")
```

| Equivalent read | Rows | Bytes served |
|---|---:|---:|
| Full metadata projection | 128 | 67,552,162 |
| Planner time pushdown | 1 | 2,102,814 |

The measured transfer reduction is **32.1x**. In a separate timed run of the
same fixture, the full query took 0.373 seconds and the planner-filtered query
took 0.0110 seconds, a **33.8x wall-time speedup**. The test asserts a minimum
10x transfer reduction and also verifies the exact row result. Bytes served can
exceed object size because the current `DaftFile` bridge uses buffered range
readers and does not yet feed the scan operator's native `IOStats`; this is why
the benchmark uses server-side byte accounting.

## Public Hugging Face sanity run

The full staged harness also ran three warm repetitions on the public 1.4 MB
D2E-480p MCAP. It validated 49,748 total messages and 421 identical rows for the
explicit-reader, planner, and unpushed topic/time predicates.

| Stage | Median seconds | Rows materialized |
|---|---:|---:|
| Full `read_mcap` count | 6.860 | 49,748 |
| Full scan + Arrow filter baseline | 6.780 | 49,748 scanned / 421 selected |
| Explicit topic/time pushdown | 6.361 | 421 |
| Planner topic/time pushdown | 6.395 | 421 |

That fixture shows only 1.06x wall-time speedup because roughly six seconds of
per-query Hugging Face setup dominates a 1.4 MB object. It is a correctness and
latency-floor check, not evidence against chunk pruning. The 16x range-byte
result is the representative selectivity signal available without ABC access;
the 390 MB ABC run remains required before making a dataset-specific wall-time
claim.
