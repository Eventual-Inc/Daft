# MCAP pushdown benchmark results

Run dates: 2026-07-09 through 2026-07-10, Apple Silicon macOS, native Daft
runner, Python 3.11.

## ABC-130k gated smoke run

Gated access succeeded for the pinned 18,071,465-byte smoke episode. The harness
ran in remote-warm mode over Hugging Face Xet with three recorded repetitions:

```text
hf://datasets/XDOF/ABC-130k@29136bc9b9e38d320b00ffcddbbe4cd0e3278c58/data/train/clip_the_socks_to_the_hanger/episode_5b33995f-ba4a-49f8-bfb7-c6c034df0865/episode.mcap
```

The file has 9,849 messages in 18 indexed Zstd chunks. The automatically chosen
predicate selected `/left-wrist-camera` over the centered 1% of the 30.1-second
recording, returning nine messages. Reader, planner, and post-scan results agreed
exactly.

| Stage | Median seconds | Rows/output |
|---|---:|---:|
| Full `read_mcap` count | 14.403 | 9,849 |
| Full metadata projection + Arrow filter | 12.685 | 9,849 scanned / 9 selected |
| Topic-only reader filter | 14.577 | 895 |
| Time-only reader filter | 2.967 | 99 |
| Explicit topic + time reader filter | 3.018 | 9 |
| Planner topic + time filter | 3.038 | 9 |
| Filtered binary payload materialization | 2.630 | 9 / 49,267 payload bytes |

The measured speedups against the equivalent unpushed scan are **4.20x** for
explicit reader filters and **4.18x** for the planner predicate. This small-file
smoke is dominated by remote setup latency. Topic-only filtering did not improve
wall time because every recurring ABC stream shares the same chunks; the
observed gain comes from pruning the time range.

The one-shot exact-object lookup took 0.402 seconds and native summary sniff took
3.869 seconds. These are setup observations, not medians, and source object size
is not measured network traffic.

The same run decoded nine real ABC H.264 frames at 640x480:

| Video projection | Median seconds | Arrow bytes |
|---|---:|---:|
| Frame metadata, no RGB `data` | 10.353 | 2,322 |
| Frame metadata + RGB `data` | 9.932 | 8,296,893 |

Remote timing noise made the metadata-only projection slightly slower, so this
run supports no video latency claim. It does demonstrate correct decode and a
3,573x reduction in materialized Arrow bytes when RGB is projected away.

## ABC-130k 390 MB remote-cold run

The production-size run used this pinned 389,778,169-byte episode:

```text
hf://datasets/XDOF/ABC-130k@29136bc9b9e38d320b00ffcddbbe4cd0e3278c58/data/train/clip_the_socks_to_the_hanger/episode_0ab8f08a-eb80-40f4-99f7-3d83db25df83/episode.mcap
```

It contains 81,811 messages in 369 indexed Zstd chunks over 255.2 seconds. A
remote-cold, process-isolated Xet run selected `/right-wrist-camera` over the
centered 1% time window. The equivalent post-scan, explicit-reader, planner, and
ABC wrapper queries all returned the same 74 rows.

| Stage | Seconds | Rows/output |
|---|---:|---:|
| Full `read_mcap` count | 314.452 | 81,811 |
| Full metadata projection + Arrow filter | 280.395 | 81,811 scanned / 74 selected |
| Topic-only reader filter | 255.398 | 7,437 |
| Time-only reader filter | 5.270 | 823 |
| Explicit topic + time reader filter | 6.142 | 74 |
| Planner topic + time filter | 6.007 | 74 |
| Bounded `abc.messages` topic + time filter | 5.391 | 74 |
| Filtered binary payload materialization | 5.059 | 74 / 1,581,262 payload bytes |

The explicit reader was **45.65x faster** and the planner predicate was
**46.68x faster** than the equivalent unpushed query. Topic-only filtering
remained near full-scan cost, confirming that the gain comes from time-based
chunk pruning rather than merely emitting fewer channel rows. The ABC wrapper
was within run-to-run noise of direct `read_mcap`; its measured latency ratio
was 0.88x, not a claim that the wrapper accelerates the native reader.

The same cold process separately measured the dataset API boundary:

| Bounded setup stage | Seconds | Rows |
|---|---:|---:|
| Exact object metadata | 0.580 | 1 |
| Native MCAP summary sniff | 3.934 | 1 file |
| Task-scoped `abc.raw` catalog + exact selection | 2.216 | 1 |
| `abc.metadata` normalized range sniff | 3.650 | 1 |

The catalog stage listed only the selected task and preserved the pinned
revision. Neither catalog nor metadata stage read message payloads.

Eight real H.264 frames from the selected right-wrist stream took 8.282 seconds
with image `data` projected out and 8.355 seconds with RGB images. The two
projections materialized 2,080 and 9,771,192 Arrow bytes respectively. Remote
latency was effectively unchanged, but projection avoided **4,698x** of output
materialization.

## ABC-130k 390 MB local mirror

A three-repeat local-mirror run on the identical pinned object isolates parser,
decompression, and planner costs from Hugging Face transport:

| Stage | Median seconds | Rows/output |
|---|---:|---:|
| Full `read_mcap` count | 0.269 | 81,811 |
| Full metadata projection + Arrow filter | 0.302 | 81,811 scanned / 74 selected |
| Topic-only reader filter | 0.231 | 7,437 |
| Time-only reader filter | 0.0128 | 823 |
| Explicit topic + time reader filter | 0.0119 | 74 |
| Planner topic + time filter | 0.0130 | 74 |
| Bounded `abc.messages` topic + time filter | 0.0146 | 74 |
| Filtered binary payload materialization | 0.0125 | 74 / 1,581,262 payload bytes |

Local explicit and planner pushdowns were **25.42x** and **23.17x** faster than
the equivalent unpushed query. Task catalog discovery took 0.092 seconds and
normalized metadata took 0.024 seconds. This confirms that indexed pruning
improves the parser/decompression path itself; the much larger remote absolute
times are dominated by object transport.

Local video medians were 0.0825 seconds without RGB output and 0.100 seconds
with eight RGB frames. As with the cold run, projection changes materialization
far more than decode cardinality work.

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

## Stateful video decode smoke

A local indexed MCAP smoke used 128 conforming Foxglove H.264 messages (64x48,
eight GOPs), interleaved with 128 non-video sensor messages. The video topic was
pushed into `read_mcap` before decode and the codec context persisted across
16-message source batches. The query used a 16-frame time window beginning at a
keyframe, exercising the reverse keyframe lookup and ranged forward read. Three
recorded runs produced these medians:

| Projection | Median seconds | Rows | Arrow bytes |
|---|---:|---:|---:|
| Frame metadata, no RGB `data` | 0.00628 | 16 | 1,728 |
| Frame metadata + RGB `data` | 0.0140 | 16 | 149,488 |

This is a bounded functional/memory-allocation smoke. It demonstrates that
projection pushdown avoids RGB ndarray materialization (2.2x lower wall time on
this fixture) while still running the stateful decoder to preserve correct frame
cardinality. The real 390 MB runs above cover production codec, local, and
remote-I/O behavior.

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
latency-floor check, not evidence against chunk pruning. The synthetic 32.1x
range-byte result remains the controlled transfer-selectivity signal; the real
ABC runs span 4.18–4.20x on the setup-dominated 18 MB smoke and 45.65–46.68x on
the 390 MB remote-cold episode.
