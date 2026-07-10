# MCAP pushdown benchmark: ABC-130k

This benchmark measures the MCAP path Daft is optimizing. Object discovery is a
small, separate `from_glob_path` stage; every message-level stage starts at
`daft.read_mcap`. The default input is an exact 390 MB episode pinned to ABC-130k
revision `29136bc9b9e38d320b00ffcddbbe4cd0e3278c58`, so dataset changes cannot
silently move the baseline.

The script reports these stages as structured JSON:

1. object metadata scan (`path`, object size, split/task/episode identity),
2. native range-read MCAP summary/schema/channel/chunk sniff,
3. full `read_mcap` message count,
4. a deliberately unpushed full metadata scan followed by the equivalent Arrow filter,
5. topic-only, time-only, and combined reader-filtered reads,
6. the same combined predicate expressed with Daft `.where(...)`, exercising planner pushdown,
7. raw payload materialization and allocation throughput.

The combined reader and planner result counts must equal the unpushed baseline.
The report computes median pushdown speedups only between those equivalent
queries. `source_file_bytes` is the object size, not a claim about network bytes;
`arrow_bytes` and `payload_value_bytes` are bytes actually materialized by the
process.

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
