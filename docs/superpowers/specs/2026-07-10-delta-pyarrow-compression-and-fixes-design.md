# PyArrow Delta Writer: Compression + Correctness Fixes

**Date:** 2026-07-10
**Status:** Approved
**Supersedes:** the native Delta writer from `docs/superpowers/specs/2026-07-09-s3-delta-fast-path-design.md` (Workstream 2), which is abandoned — see Motivation.
**Branch:** `feat/delta-pyarrow-compression`, off `main`.

## Motivation

An earlier branch replaced Daft's PyArrow Delta data-file writer with a native Rust
one. Benchmarking it retired that idea:

| 2M rows, local disk | wall | throughput |
|---|---|---|
| native `ParquetWriter` | 0.193s | 169 MB/s |
| `pyarrow.parquet.ParquetWriter` | 0.027s | 1236 MB/s |

The ~7× gap is in Daft's **native parquet writer**, not the Delta layer — measured with
the Delta layer stripped out entirely. It is not fixed overhead (it widens with row
count) and it is not compression (uncompressed and snappy cost the same). The obvious
suspect, `FileStorageBackend`'s 4 KB write buffer, was tested and **ruled out**: raising
it to 1 MB changed 0.193s to 0.190s.

At 169 MB/s the native writer would likely cap S3 throughput, since a multipart upload
with its 100-way part concurrency can exceed 1 GB/s. PyArrow encodes with ~7× of
headroom above any realistic upload rate. The native writer's advantages — no GIL, no
per-file `get_file_info` stat, native multipart — cannot be realized behind an encoder
that slow.

So: keep PyArrow as the Delta writer, give it the compression it always supported but
never exposed, and fix the correctness defects it does have.

## What survives from the abandoned branch

Two changes are independent of the writer choice and are real wins. They are
cherry-picked onto the new branch:

1. **Scan HEAD elimination** — commits `6c7807e9e`, `f8f38301b`, `929944da6`. A remote
   parquet open no longer costs an extra HEAD request when the scan task already knows
   the file size (Delta/Iceberg transaction logs, S3 LIST).
2. **tz-aware timestamp normalization** — commits `e21bf0b53`, `62d429875`. Daft infers
   timezone `+00:00` from Python `datetime` objects, and `deltalake >= 1.0`'s
   `Schema.from_arrow` rejects it, so **any tz-aware `write_deltalake` fails at commit
   time on `main` today**. The fix also unbreaks `mode="append"` against tz-aware tables.

Everything else native is dropped: `delta_stats.rs`, `delta_writer.rs`, the
`native_delta_writer` config flag, the `catalog.rs` seam, `make_native_add_action_batch`,
the native-vs-PyArrow parity suite, `test_native_writer.py`, and the writer benchmark.
`CatalogWriterFactory` stays as it is on `main` (`native: false`).

### The cherry-pick is not clean, and must not be

`a3b388627` (don't classify transient IO errors as corrupt on the known-size path)
depends on `a0505baff`, which also touches `delta_stats.rs`, `delta_writer.rs`, and both
native test files. Cherry-picking `a0505baff` would drag native code onto the new branch.

Instead: cherry-pick the three scan commits, then take the final content of
`src/daft-parquet/src/{lib.rs, read.rs, reader/chunk_source.rs}` from `f6079e4be` as one
commit. `daft-parquet` is the *reader*; it contains no native-writer code, so this is
safe and produces the identical end state.

The tz fix has **no tests of its own** — its coverage lived in the parity suite being
dropped. The new branch must add them, or it silently ships an untested fix for a bug
that breaks `main`.

This spec, and the implementation plan derived from it, are committed on
`perf/s3-delta-fast-path`. They must be cherry-picked onto the new branch as its first
commit, before any code, so the branch carries its own design record.

## Verified defect inventory

Everything below was measured against the PyArrow path, not assumed. Two widely-believed
defects turned out not to exist.

| Behavior | Status |
|---|---|
| DATE min/max stats | **Correct.** The `TODO` at `delta_lake_write.py:98` claiming otherwise is stale. |
| DECIMAL min/max stats | **Correct.** |
| TIMESTAMP min/max stats (tz-aware and naive) | **Correct.** |
| NaN / ±inf float bounds | Correct — bound dropped, which is safe. |
| All-null columns | Correct — `nullCount` only, no bounds. |
| **Unsigned integers** | **Broken.** Silent unreadable table. |
| **Binary stats** | **Broken.** Emits a bound that isn't true. |
| **Compression** | Hardcoded off. Never exposed. |

Do not "fix" the first five. They work.

## Fix 1: Compression

`DeltalakeWriter` inherits full `compression` support from `ParquetFileWriter`
(`daft/io/writer.py:144-165`), which forwards it to `pq.ParquetWriter`. Only the
hardcoded `compression=None` at `daft/io/writer.py:407` is in the way. `IcebergWriter`
has passed `compression="zstd"` all along.

### Accepted codecs

`none`, `uncompressed` (alias → `none`), `snappy`, `gzip`, `brotli`, `lz4`, `zstd` —
case-insensitive. All six verified to round-trip through `delta-rs`.

`lz4` is accepted here even though the abandoned branch rejected it. It was rejected
*only* because arrow-rs and PyArrow encode it as different parquet codecs (deprecated
Hadoop-framed LZ4, id 5, versus LZ4_RAW, id 7). With one writer there is no divergence.

Rejected, each with its own reason:

- **`lz4_raw`** — PyArrow's parquet writer cannot encode it.
- **`lzo`** — PyArrow's C++ implementation does not support LZO encoding. It *constructs*
  fine and raises `OSError: While LZO compression is supported by the Parquet format in
  general, it is currently not supported by the C++ implementation` on the first real
  write. A capability probe that writes zero rows wrongly reports it as supported.

Validation lives in `normalize_delta_compression(compression: str) -> str` in
`daft/io/delta_lake/delta_lake_write.py`, called as the first statement of each public
entry point so an invalid codec raises before anything touches storage.

### Default: `snappy`

This is a deliberate behavior change: every existing caller's **new** files become
snappy. It matches delta-rs and Spark, roughly halves bytes, and costs negligible CPU.

**Existing uncompressed files remain readable.** Parquet records the codec per column
chunk inside each file's own metadata, so a table containing a mix of codecs decodes
correctly. Verified: a table with `UNCOMPRESSED`, `SNAPPY`, and `ZSTD` files reads back
completely through both `deltalake.DeltaTable` and `daft.read_deltalake`.

### Three entry points, not one

| Entry point | Writes via | Change |
|---|---|---|
| `DataFrame.write_deltalake` | Daft's writer | `compression: str = "snappy"` |
| `distributed_merge_deltalake` → `DistributedDeltaMergeBuilder` | Daft's writer | `compression: str = "snappy"` |
| `merge_deltalake` | delta-rs's own engine | `compression: str \| None = None` |

`DistributedDeltaMergeBuilder` has **two** write branches and both need it:
`_write_partition_scoped` calls `write_df._builder.write_deltalake(...)` directly
(bypassing `DataFrame.write_deltalake` and its default), and `execute()`'s full-rewrite
branch calls `write_df.write_deltalake(...)`. The codec therefore has to be validated in
`distributed_merge_deltalake`, passed into `DistributedDeltaMergeBuilder.__init__`, stored
as `self._compression`, and read by both branches — the builder-level call has no default
to fall back on.

Because `DeltalakeWriter` is constructed from Rust, the codec is carried as one
`compression: String` field on `DeltaLakeCatalogInfo`, threaded through the `delta_write`
builder and its pyo3 wrapper, into `create_pyarrow_catalog_writer` →
`new_deltalake_writer` → the Python `DeltalakeWriter`.

### `merge_deltalake` and `WriterProperties`

`merge_deltalake` delegates to delta-rs's merge engine and already accepts
`writer_properties: deltalake.WriterProperties`, which carries its own `compression`.

**`dataclasses.replace` cannot be used to inject compression into a user's
`WriterProperties`.** That class is decorated `@dataclass(init=True)` but declares no
annotated class-level fields, so `__dataclass_fields__` is empty. `replace()` therefore
copies nothing and returns a fresh object with every other option reset to its default —
silently discarding `write_batch_size`, `max_row_group_size`, and the rest. (This is an
upstream `delta-rs` bug worth reporting: the class advertises the dataclass protocol and
does not implement it.)

So `compression` takes a `None` sentinel there, and the two parameters never merge:

| `compression` | `writer_properties` | Behavior |
|---|---|---|
| `None` | `None` | `WriterProperties(compression="SNAPPY")` |
| given | `None` | `WriterProperties(compression=<UPPERCASE>)` |
| `None` | given | passed through untouched — the expert escape hatch, and where `lz4_raw` remains reachable |
| given | given | `ValueError`: ambiguous |

Note delta-rs rejects `'NONE'`; `"none"` maps to `'UNCOMPRESSED'`.

`compression_level` is out of scope, matching `write_parquet`.

## Fix 2: Unsigned integers write unreadable tables

Delta Lake has no unsigned types. `delta-rs` maps `uint8→byte` (int8),
`uint16→short` (int16), `uint32→integer` (int32), `uint64→long` (int64). Any unsigned
value above the corresponding **signed** maximum commits successfully and then cannot be
read: `deltalake.DeltaTable(...).to_pyarrow_table()` raises
`ArrowInvalid: Integer value 3000000000 not in range: 0 to 2147483647`.

This affects `uint8` above 127 as readily as `uint32` above 2^31−1. It is silent.

**Fix:** widen unsigned types in `convert_pa_schema_to_delta`, recursing into nested
types exactly as the tz normalization already does:

| Arrow type | Widened to | Delta type | Lossless? |
|---|---|---|---|
| `uint8` | `int16` | `short` | yes, all 256 values |
| `uint16` | `int32` | `integer` | yes |
| `uint32` | `int64` | `long` | yes |
| `uint64` | `int64` | `long` | yes up to `i64::MAX`; raises beyond |

`sanitize_table_for_deltalake` already casts the table to that schema, so values follow
automatically. For `uint64` above `i64::MAX` the cast raises `ArrowInvalid`; wrap it in a
`ValueError` naming the column and suggesting an explicit cast. Failing loudly at write
time is strictly better than committing a table nobody can read.

Consequence, which must be documented: a `uint32` column reads back as `int64`. Delta
cannot represent it otherwise.

## Fix 3: Binary stats assert something untrue

`DeltaJSONEncoder` (`daft/io/delta_lake/delta_lake_write.py:120-121`) serializes `bytes`
bounds with `obj.decode("unicode_escape", "backslashreplace")`, so `b"\xff\xfe"` becomes
the string `"ÿþ"`. That is not what the data contains, and other engines read these
bounds for data skipping.

**Fix:** in `get_file_stats_from_metadata`, decode `bytes` bounds as strict UTF-8. If
either the min or the max fails to decode, omit **both** for that column while keeping
its `nullCount`. A missing bound is always safe; a wrong one is not. UTF-8-decodable
binary keeps its bounds — for well-formed UTF-8, byte order equals string order, so the
bound stays valid.

## Testing

All tests target the PyArrow path. There is no second writer to compare against, so
assertions anchor to **true values**, not to another implementation.

**Compression**
- Each of the six accepted codecs round-trips on `write_deltalake`: the codec lands in
  the file's column-chunk metadata and `deltalake.DeltaTable` reads the rows back.
- Default is `snappy` on all three entry points.
- `distributed_merge_deltalake` compresses on **both** write branches (partition-scoped
  and full-rewrite).
- `merge_deltalake`'s full conflict matrix, including that passing `writer_properties`
  through preserves its `write_batch_size` and `max_row_group_size` — the exact property
  a naive `dataclasses.replace` implementation would destroy.
- `lz4_raw` and `lzo` raise at call time, each naming its own reason, before any
  `_delta_log` exists. `lzo` must be exercised with **real rows**.
- `uncompressed` ≡ `none`; codec names are case-insensitive.

**Mixed-codec tables**
- Write uncompressed, append snappy, append zstd. Assert the three on-disk codecs differ
  and that both `deltalake.DeltaTable` and `daft.read_deltalake` return every row.

**Unsigned**
- `uint8`, `uint16`, `uint32` at their maximum values round-trip and read back exactly.
- `uint64` within `i64::MAX` works; above it raises a `ValueError` naming the column.
- Nested unsigned (inside a struct and inside a list) is widened too.

**Binary stats**
- Non-UTF-8 bytes → `minValues`/`maxValues` omit the column, `nullCount` retains it.
- UTF-8-decodable bytes → bounds present and correct.

**Stats correctness** (replacing the dropped parity suite; each asserts the true value)
- date, decimal, tz-aware timestamp, naive timestamp, int, string, NaN, ±inf, all-null,
  nested struct dotted keys, strings longer than parquet's 64-byte truncation limit.

**tz fix** (cherry-picked without tests)
- tz-aware `write_deltalake` commits successfully — this fails on `main`.
- `mode="append"` against a tz-aware table works.

## Risks

1. **The snappy default changes every existing caller's output.** Intentional and
   approved. Old files stay readable; tables become mixed-codec, which is normal.
2. **`uint32` now reads back as `int64`.** Unavoidable — Delta has no unsigned types.
   The alternative is the status quo, which is an unreadable table.
3. **Binary columns lose their bounds when not UTF-8-decodable.** Deliberate: a missing
   bound is safe, a false one is not.
4. **The cherry-picks may conflict.** `e21bf0b53` touches `delta_lake_write.py`, whose
   surrounding code differs on `main`. Resolve by keeping `main`'s context and applying
   only the tz-normalization functions.

## Out of scope

- Fixing the native parquet writer's encode throughput (~169 MB/s). Worth its own
  investigation; it affects `write_parquet`, whose default is already the native writer.
- `compression_level`, per-column `column_compression`.
- Iceberg writes (still `zstd`).
- Benchmarking against a real S3 bucket. Requires credentials unavailable here, and it is
  the only measurement that settles the writer question for object storage.
