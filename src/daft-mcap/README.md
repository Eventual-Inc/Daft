# MCAP I/O profiling

Native scan tasks pass their `IOStatsContext` into the MCAP reader. The backend
records GET/HEAD/LIST operations and bytes; `NativeMcapReader::read_stats()` adds
logical summary seeks/reads, summary size, indexed chunk reads/fetches, and buffer
hits. The logical counters are also emitted at debug level when a reader drops.
Seeks update offsets without issuing I/O. These counters are not OS syscall
counts. Backend GET counts are not exact wire-request counts: redirects, retries,
and Xet reconstruction can perform additional network requests.

The footer contains `summary_start`. After fetching the footer, the reader knows
the distance from that offset to EOF (`summary_tail_bytes`, including footer and
trailing magic). Its existing 8 MiB summary read-ahead fetches that whole tail in
one GET when it fits. Larger summaries can require more fetches; inspect the
counters before increasing the buffer budget. A normal ranged open costs one
HEAD plus three GETs: leading magic, footer, summary. Remote files at most 64 KiB
are fetched in one GET and parsed from memory.

## Opt-in ABC-130k integration test

Accept access at <https://huggingface.co/datasets/XDOF/ABC-130k> and export an
authorized `HF_TOKEN` through your normal credential setup. The test reads that
environment variable; it does not print the token. It defaults to a pinned
602 MB validation episode, but reads only the first batch (1,000 messages).
Read-ahead/overlapping chunks can fetch more than that batch's payload.

```sh
cargo test --locked -p daft-mcap --test remote_profile -- --ignored --nocapture
```

If the token is in a gitignored `.env` instead of the shell environment:

```sh
uv tool run --from 'python-dotenv[cli]' dotenv -f .env run -- cargo test --locked -p daft-mcap --test remote_profile -- --ignored --nocapture
```

Each file reports cumulative counters after `open` and after `batch_cap` or
`eof`, followed by dataset totals. Subtract open counters from final counters to
isolate message reads. A capped sample is not a full-dataset throughput estimate.
This profiles the native reader directly, not Python discovery/plan construction.

Optional environment variables:

| Variable | Meaning |
| --- | --- |
| `MCAP_PROFILE_URIS` | Whitespace-separated remote MCAP URIs; overrides the sample |
| `MCAP_PROFILE_MAX_BATCHES` | Per-file batch cap; default `1`, `0` scans to EOF |
| `MCAP_PROFILE_TOPIC` | One topic to select |
| `MCAP_PROFILE_START_TIME` / `MCAP_PROFILE_END_TIME` | Nanoseconds, inclusive start / exclusive end |
| `MCAP_PROFILE_XET` | `1` enables Xet; default HTTP ranges give a simpler baseline |

The default test suite includes a local HTTP range server regression that checks
GETs, HEADs, and bytes against server-observed counts, and verifies that many
logical reads are satisfied from each buffer. No external access is needed:

```sh
cargo test --locked -p daft-mcap remote_requests_match_io_stats_and_logical_reads
```

## Measured baseline (2026-09-09)

Default pinned ABC-130k episode, HTTP mode, 602,196,185-byte file. Counts below
are cumulative, from separate first-batch and full-scan runs; times depend on
the network and machine. These are one-episode measurements, not a dataset-wide
estimate or exact wire-request counts.

| Phase | Messages | GETs | HEADs | Bytes read | Elapsed |
| --- | ---: | ---: | ---: | ---: | ---: |
| Open (full-scan run) | 0 | 3 | 1 | 125,563 | 1.71 s |
| First batch | 1,000 | 4 | 1 | 8,322,227 | 3.18 s |
| Full episode | 288,765 | 82 | 1 | 652,128,552 | 84.18 s |

The summary tail is **125,518 bytes**. Its 2 seeks and 1,246 logical reads
require just **2 fetches** (footer, then complete summary tail); 1,244 reads hit
the summary buffer. The 8-byte leading magic probe is the third startup GET.

The full scan requests **579 chunks**: **500 buffer hits, 79 fetches**. Together
with startup, that is 82 GETs. Total bytes exceed file size by about **8.3%**
because read-ahead ranges can overlap and the summary/footer are read separately.
For this episode, chunk read-ahead is the larger optimization opportunity; the
summary is already fetched in one piece after the footer. Read-ahead policy is
unchanged by this profiling instrumentation.
