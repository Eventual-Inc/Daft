# MCAP pushdown benchmark: ABC-130k

This benchmark measures the MCAP path Daft is optimizing. Object discovery is
separate from payload reads: it times both an exact `from_glob_path` lookup and
the task-scoped `daft.datasets.abc.raw` catalog before selecting one episode.
Direct message stages start at `daft.read_mcap` and are checked against the
bounded `daft.datasets.abc.messages` wrapper. The default input is an exact 390
MB episode pinned to ABC-130k revision
`29136bc9b9e38d320b00ffcddbbe4cd0e3278c58`, so dataset changes cannot silently
move the baseline.

The script reports these stages as structured JSON:

1. object metadata scan (`path`, object size, split/task/episode identity),
2. native range-read MCAP summary/schema/channel/chunk sniff,
3. task-scoped ABC catalog discovery followed by an exact episode filter and `limit(1)`,
4. normalized `daft.datasets.abc.metadata` for that one bounded episode,
5. full `read_mcap` message count,
6. a deliberately unpushed full metadata scan followed by the equivalent Arrow filter,
7. topic-only, time-only, and combined reader-filtered reads,
8. the same combined predicate expressed with Daft `.where(...)`, exercising planner pushdown,
9. the equivalent bounded `daft.datasets.abc.messages` query,
10. raw payload materialization and allocation throughput,
11. stateful Foxglove video decode without image materialization,
12. stateful Foxglove video decode with RGB image materialization.

The combined reader, planner, and ABC wrapper result counts must equal the
unpushed baseline. The report computes median pushdown speedups only between
those equivalent queries and reports wrapper/direct latency as a separate ratio.
`source_file_bytes` is the object size, not a claim about network bytes;
`arrow_bytes` and `payload_value_bytes` are bytes actually materialized by the
process.

The catalog stage deliberately scopes discovery to the benchmark episode's split
and task before applying the episode ID and `limit(1)`. It is not a timing for
listing all 130,703 episodes. The metadata stage runs after the native summary
stage in the same process and can therefore reuse cached footer ranges; use a
fresh process when measuring isolated cold metadata latency.

For ABC's deep Hugging Face layout, `abc.raw` uses the official Hub client's
paginated recursive-tree API instead of walking every episode directory through
the generic glob bridge. It preserves an `@revision`-qualified root, so catalog,
metadata, and message stages all address the same pinned release.

## Authentication and setup

ABC-130k is gated. Accept its terms on Hugging Face and expose a read token; the
benchmark passes it explicitly through `HuggingFaceConfig` and enables Xet by
default.

```bash
make .venv
make build
export HF_TOKEN=hf_...
```

Without a token, or when Hugging Face returns a gated/authorization response, the
script emits a JSON `skipped` result and exits successfully. It never hides parser,
planner, or row-count mismatches as skips.

## Cold, warm, and local runs

Run each mode in a fresh process. Cold mode creates an isolated HF Hub/Xet cache
before importing Daft and deletes it afterward. This removes client-side cache
effects at process start, but the fixed stage sequence intentionally shares that
cache, so later stages can reuse ranges fetched by earlier stages. CDN and
service-side caches can also still be warm. Treat cold mode as a first-run trace;
use warm and local medians for like-for-like pushdown ratios.

```bash
.venv/bin/python benchmarking/mcap/benchmark_abc130k.py \
  --mode remote-cold --repeat 3 --output /tmp/mcap-cold.json

.venv/bin/python benchmarking/mcap/benchmark_abc130k.py \
  --mode remote-warm --repeat 3 --output /tmp/mcap-warm.json
```

Warm mode executes one unrecorded pass of every reader stage before recording.
For a local mirror, pass either the exact MCAP file or the mirror root that
contains `data/train/...`:

```bash
.venv/bin/python benchmarking/mcap/benchmark_abc130k.py \
  --mode local --local-mirror /data/ABC-130k --repeat 5 \
  --output /tmp/mcap-local.json
```

Use `--no-xet` for an HTTP comparison. Use `--include-plan` to include optimized
logical/physical plans in the report.

When the indexed channel catalog contains a `foxglove.CompressedVideo` schema,
the harness automatically benchmarks its first topic through
`daft.read_mcap(..., decode_video=True)`. Override it with `--video-topic`, or
bound image work with `--video-frame-limit` (32 by default). The metadata-only
decode projection deliberately omits `data`, measuring codec/cardinality work
without RGB ndarray materialization.

## Selection and smoke episodes

By default the harness chooses the busiest non-empty indexed topic (so the
centered interval is likely to contain messages) and a centered 1% time window.
This is an intentionally time-selective query and the selection policy is
recorded in the result. Pin explicit production predicates when comparing commits:

```bash
.venv/bin/python benchmarking/mcap/benchmark_abc130k.py \
  --mode remote-warm \
  --topic /camera_0/color/image_raw/compressed \
  --start-time 1700000000000000000 \
  --end-time 1700000001000000000
```

For a quick access/correctness smoke, replace `--uri` with this pinned 18 MB
episode. Keep exact URIs rather than a recursive HF glob: glob revision
propagation is not reliable enough for a reproducible payload benchmark.

```text
hf://datasets/XDOF/ABC-130k@29136bc9b9e38d320b00ffcddbbe4cd0e3278c58/data/train/clip_the_socks_to_the_hanger/episode_5b33995f-ba4a-49f8-bfb7-c6c034df0865/episode.mcap
```

Payload materialization defaults to the selected topic/time window. Pass
`--payload-scope full` for an end-to-end episode payload scan, or
`--payload-limit N` for a bounded correctness smoke test. Compare cold, warm, and
local results before attributing first-run time to the MCAP parser: gated access,
Xet chunk fetches, and Hugging Face caching can dominate a remote run.
