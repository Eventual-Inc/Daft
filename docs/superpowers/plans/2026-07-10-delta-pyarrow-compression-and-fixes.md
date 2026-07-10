# PyArrow Delta Writer: Compression + Correctness Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On a fresh branch off `main`, keep PyArrow as the Delta data-file writer, give it a `compression` parameter across all three Delta write entry points, and fix four correctness defects it has today.

**Architecture:** The codec is validated once in Python and carried as one `compression: String` field on `DeltaLakeCatalogInfo`, threaded through the Rust `delta_write` builder into the Python `DeltalakeWriter`. The three correctness fixes all live in Python: unsigned widening and binary-stats sanitation in `daft/io/delta_lake/delta_lake_write.py`, and a data-dependent lossy-cast guard in `daft/io/delta_lake/_deltalake.py`.

**Tech Stack:** Python (daft/io/delta_lake, daft/dataframe, daft/io/writer.py), Rust (daft-logical-plan, daft-writers), pyo3, pytest, `deltalake==1.6.0`, PyArrow 22.

**Spec:** `docs/superpowers/specs/2026-07-10-delta-pyarrow-compression-and-fixes-design.md`

## Global Constraints

- **Branch:** `feat/delta-pyarrow-compression`, created off `main`. All work happens there.
- **No native Delta writer.** `delta_stats.rs`, `delta_writer.rs`, the `native_delta_writer` flag, and the `catalog.rs` native seam must never appear on this branch. `CatalogWriterFactory` stays as `main` has it (`native: false`).
- **Default compression is `"snappy"`** on `DataFrame.write_deltalake` and `distributed_merge_deltalake`. This intentionally changes existing callers' output. Old uncompressed files stay readable — parquet stores the codec per column chunk.
- **Accepted codecs** (case-insensitive): `none`, `uncompressed` (alias → `none`), `snappy`, `gzip`, `brotli`, `lz4`, `zstd`.
- **Rejected codecs**, each with its own reason: `lz4_raw` (PyArrow cannot encode it); `lzo` (PyArrow's C++ implementation does not support LZO encoding — it *constructs* fine and raises `OSError: While LZO compression is supported by the Parquet format in general, it is currently not supported by the C++ implementation` on the first real write).
- **Never call `dataclasses.replace` on `deltalake.WriterProperties`.** It is decorated `@dataclass` but declares no fields, so `__dataclass_fields__` is empty and `replace()` silently returns an object with every other option reset to its default.
- Validation runs as the **first statement** of each public entry point, so an invalid codec raises before anything touches storage.
- Scope is `compression` only. No `compression_level`, no per-column `column_compression`. Iceberg untouched.
- Commits must NOT contain a `Co-Authored-By` trailer.
- **NEVER run `cargo fmt --all`** — it reformats ~84 unrelated files of pre-existing drift. Format only files you touch (`cargo fmt -p <crate>`).
- `cargo clippy -p <crate> -- -D warnings` exits 101 on PRE-EXISTING lints in dependency crates. Use `--no-deps --all-features` for a signal on your own files.
- `make build` (~2 min) is required after any Rust change, before Python tests.
- Python tests: `DAFT_RUNNER=native make test EXTRA_ARGS="..."`.

---

### Task 1: Bootstrap the branch

**Files:**
- Create branch `feat/delta-pyarrow-compression` from `main`
- Port: `src/daft-parquet/src/{lib.rs, read.rs, reader/chunk_source.rs}`
- Cherry-pick: `6c7807e9e`, `f8f38301b`, `929944da6`, `e21bf0b53`, `62d429875`
- Cherry-pick: the spec commits from `perf/s3-delta-fast-path`

**Interfaces:**
- Produces: a branch whose `daft-parquet` reader skips the per-file HEAD request when a scan task knows the file size, and whose `convert_pa_schema_to_delta` normalizes tz-aware timestamps to `UTC`.

**Why the port instead of a cherry-pick.** `a3b388627` (don't classify transient IO errors as corrupt on the known-size path) depends on `a0505baff`, which *also* touches `delta_stats.rs`, `delta_writer.rs`, and both native test files. Cherry-picking it drags native code onto the branch. `daft-parquet` is the reader and contains no native-writer code, so taking its final file contents is safe and produces the identical end state.

- [ ] **Step 1: Create the branch and cherry-pick the scan work**

```bash
git checkout main
git checkout -b feat/delta-pyarrow-compression
git cherry-pick 6c7807e9e f8f38301b 929944da6
```
Expected: three commits applied cleanly (scan HEAD elimination).

- [ ] **Step 2: Port the hardened daft-parquet reader files**

```bash
git checkout f6079e4be -- src/daft-parquet/src/lib.rs src/daft-parquet/src/read.rs src/daft-parquet/src/reader/chunk_source.rs
git status --porcelain   # exactly those 3 files modified, nothing else
```

Verify no native code came along:
```bash
git diff --cached --name-only | grep -E 'delta_stats|delta_writer' && echo "NATIVE LEAKED - STOP" || echo "clean"
```
Expected: `clean`.

```bash
cargo test -p daft-parquet
```
Expected: 20 passed (3 hit `s3://daft-public-data` anonymously and need network).

```bash
git add src/daft-parquet
git commit -m "fix(parquet): harden known-size error classification on the scan fast path"
```

- [ ] **Step 3: Cherry-pick the tz-timestamp fix**

```bash
git cherry-pick e21bf0b53 62d429875
```

These touch only `daft/io/delta_lake/delta_lake_write.py`. If they conflict, keep `main`'s surrounding context and apply only the two new functions (`_normalize_delta_timestamp_type`, `_normalize_delta_schema_timestamps`) plus the one-line change at the end of `convert_pa_schema_to_delta` that calls `_normalize_delta_schema_timestamps(schema)` instead of returning `schema`. Do not import anything from the native writer.

- [ ] **Step 4: Bring the design record**

```bash
git cherry-pick c9efa11be 5018e216b 854303f62
```
Expected: the three spec commits for `docs/superpowers/specs/2026-07-10-delta-pyarrow-compression-and-fixes-design.md`. Also cherry-pick the commit containing this plan file once it exists on the other branch.

- [ ] **Step 5: Verify the branch is green and free of native code**

```bash
rg -l "delta_stats|delta_writer|native_delta_writer" src/ daft/ tests/ && echo "NATIVE FOUND - STOP" || echo "no native writer code"
make build
DAFT_RUNNER=native make test EXTRA_ARGS="tests/io/delta_lake"
cargo test -p daft-parquet
```
Expected: `no native writer code`; build succeeds; delta suite passes (21 checkpoint tests SKIP under the native runner — Ray-only, expected); 20 parquet tests pass.

---

### Task 2: Regression tests for the cherry-picked tz fix

**Files:**
- Test: `tests/io/delta_lake/test_delta_timestamps.py` (create)

**Interfaces:**
- Consumes: `convert_pa_schema_to_delta`'s tz normalization from Task 1.

**Why.** The tz fix arrived without tests — its coverage lived in a parity suite this branch does not have. Without this task the branch ships an untested fix for a bug that breaks `main`.

- [ ] **Step 1: Write the tests**

```python
from __future__ import annotations

import datetime

import pytest

deltalake = pytest.importorskip("deltalake")

import daft


def test_tz_aware_timestamp_commits(tmp_path):
    """Daft infers tz '+00:00' from python datetimes; deltalake>=1.0 rejects that
    spelling in Schema.from_arrow. Without the normalization this raises at commit."""
    ts = datetime.datetime(2021, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc)
    daft.from_pydict({"t": [ts]}).write_deltalake(str(tmp_path))

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table()
    got = table.column("t").to_pylist()[0]
    assert got.replace(tzinfo=datetime.timezone.utc) == ts


def test_tz_aware_append_against_existing_table(tmp_path):
    """Append re-derives the delta schema and compares it against the committed one.
    A '+00:00' vs 'UTC' mismatch used to raise ValueError here."""
    ts1 = datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc)
    ts2 = datetime.datetime(2021, 6, 1, tzinfo=datetime.timezone.utc)
    daft.from_pydict({"t": [ts1]}).write_deltalake(str(tmp_path))
    daft.from_pydict({"t": [ts2]}).write_deltalake(str(tmp_path), mode="append")

    rows = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("t").to_pylist()
    assert len(rows) == 2


def test_tz_aware_timestamp_nested_in_struct(tmp_path):
    """The normalization recurses into nested types."""
    import pyarrow as pa

    ts = datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc)
    struct_type = pa.struct([pa.field("at", pa.timestamp("us", tz="+00:00"))])
    tbl = pa.table({"s": pa.array([{"at": ts}], type=struct_type)})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    got = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("s").to_pylist()[0]
    assert got["at"].replace(tzinfo=datetime.timezone.utc) == ts


def test_naive_timestamp_is_timestamp_ntz(tmp_path):
    """Naive timestamps must not be relabelled as UTC-aware."""
    import json

    naive = datetime.datetime(2021, 1, 1, 0, 0, 1)
    daft.from_pydict({"t": [naive]}).write_deltalake(str(tmp_path))

    schema = json.loads(deltalake.DeltaTable(str(tmp_path)).schema().to_json())
    assert schema["fields"][0]["type"] == "timestampNtz"
```

- [ ] **Step 2: Prove the tests guard the fix**

Temporarily make `convert_pa_schema_to_delta`'s final branch `return schema` (undoing the normalization), run the suite, and confirm `test_tz_aware_timestamp_commits` FAILS with a `Schema error: Invalid data type for Delta Lake: Timestamp(µs, "+00:00")`. Quote that output. Then restore the normalization.

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_timestamps.py"`

- [ ] **Step 3: Run to verify all pass with the fix in place**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_timestamps.py"`
Expected: 4 passed.

- [ ] **Step 4: Commit**

```bash
git add tests/io/delta_lake/test_delta_timestamps.py
git commit -m "test(delta): cover tz-aware timestamp normalization"
```

---

### Task 3: The `normalize_delta_compression` validator

**Files:**
- Modify: `daft/io/delta_lake/delta_lake_write.py` (add after `make_deltalake_add_action`, ~line 152)
- Test: `tests/io/delta_lake/test_delta_compression.py` (create)

**Interfaces:**
- Produces: `normalize_delta_compression(compression: str) -> str` — returns a lowercase codec from `{none, snappy, gzip, brotli, lz4, zstd}`, mapping `uncompressed` → `none`. Raises `ValueError` for `lz4_raw`, `lzo`, and unknown codecs. Later tasks call it from `DataFrame.write_deltalake`, `distributed_merge_deltalake`, and `merge_deltalake`.

Nothing calls it yet. That is the intended end state of this task.

- [ ] **Step 1: Write the failing tests**

Create `tests/io/delta_lake/test_delta_compression.py`:

```python
from __future__ import annotations

import pytest

deltalake = pytest.importorskip("deltalake")

from daft.io.delta_lake.delta_lake_write import normalize_delta_compression

# Each rejected codec must explain its OWN reason, not a generic "unsupported".
_REJECTED_REASON_SUBSTRING = {
    "lz4_raw": "cannot encode",
    "lzo": "lzo",
}


@pytest.mark.parametrize("codec", ["none", "snappy", "gzip", "brotli", "lz4", "zstd"])
def test_accepted_codecs_pass_through(codec):
    assert normalize_delta_compression(codec) == codec


def test_uncompressed_is_an_alias_for_none():
    assert normalize_delta_compression("uncompressed") == "none"


@pytest.mark.parametrize("codec", ["SNAPPY", "Snappy", "ZSTD", "UNCOMPRESSED", " lz4 "])
def test_codec_names_are_case_insensitive_and_stripped(codec):
    cleaned = codec.strip().lower()
    expected = "none" if cleaned == "uncompressed" else cleaned
    assert normalize_delta_compression(codec) == expected


@pytest.mark.parametrize("codec", ["lz4_raw", "lzo", "LZO", "  lzo  "])
def test_pyarrow_unencodable_codecs_are_rejected_with_their_own_reason(codec):
    with pytest.raises(ValueError) as excinfo:
        normalize_delta_compression(codec)
    msg = str(excinfo.value)
    # Echoes the raw input so the user can find it in their code.
    assert codec in msg
    # Names the real reason, and did NOT fall through to the generic branch.
    assert "pyarrow" in msg.lower()
    assert "unsupported compression codec" not in msg.lower()
    assert _REJECTED_REASON_SUBSTRING[codec.strip().lower()] in msg.lower()


def test_lzo_really_is_unencodable_by_pyarrow():
    """Guard the reason we reject lzo: it CONSTRUCTS fine and fails on first write.

    A capability probe that writes zero rows wrongly reports lzo as supported.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    tbl = pa.table({"a": list(range(100))})
    buf = pa.BufferOutputStream()
    writer = pq.ParquetWriter(buf, tbl.schema, compression="lzo")  # succeeds
    with pytest.raises(OSError, match="not supported by the C\\+\\+ implementation"):
        writer.write_table(tbl)


def test_unknown_codec_is_rejected_and_lists_accepted():
    with pytest.raises(ValueError) as excinfo:
        normalize_delta_compression("bogus")
    msg = str(excinfo.value)
    assert "bogus" in msg
    assert "snappy" in msg and "zstd" in msg and "uncompressed" in msg
```

- [ ] **Step 2: Run to verify they fail**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_compression.py"`
Expected: FAIL — `ImportError: cannot import name 'normalize_delta_compression'`.

- [ ] **Step 3: Implement the validator**

In `daft/io/delta_lake/delta_lake_write.py`, after `make_deltalake_add_action`:

```python
# Codecs PyArrow's parquet writer can encode. `lz4` is included: with a single writer
# there is no ambiguity about which parquet codec it maps to.
_DELTA_COMPRESSION_CODECS = frozenset({"none", "snappy", "gzip", "brotli", "lz4", "zstd"})

# `uncompressed` is a spelling of `none`, not a distinct codec. PyArrow's writer rejects
# the string but accepts `none`; `write_parquet` documents `uncompressed`, so we keep it
# working here rather than gratuitously diverging.
_DELTA_COMPRESSION_ALIASES = {"uncompressed": "none"}

# Codecs the parquet format defines but PyArrow cannot encode. `lzo` is the treacherous
# one: `pq.ParquetWriter(compression="lzo")` constructs successfully and raises on the
# first `write_table`, so a zero-row capability probe reports it as supported.
_DELTA_COMPRESSION_REJECTED = {
    "lz4_raw": "PyArrow's parquet writer cannot encode it. Use 'lz4' instead.",
    "lzo": (
        "PyArrow's C++ Parquet implementation does not support LZO encoding "
        "(it fails on the first write, not at writer construction). "
        "Use 'snappy' or 'zstd' instead."
    ),
}


def normalize_delta_compression(compression: str) -> str:
    """Validate a Delta write compression codec and return its canonical name.

    Case-insensitive; surrounding whitespace is ignored. Maps ``uncompressed`` to
    ``none``. Raises ``ValueError`` both for codecs PyArrow cannot encode and for
    unrecognized codec names, so the failure surfaces at call time rather than mid-write.
    """
    codec = compression.strip().lower()
    codec = _DELTA_COMPRESSION_ALIASES.get(codec, codec)

    if codec in _DELTA_COMPRESSION_REJECTED:
        raise ValueError(
            f"compression={compression!r} is not supported for Delta writes: "
            f"{_DELTA_COMPRESSION_REJECTED[codec]}"
        )
    if codec not in _DELTA_COMPRESSION_CODECS:
        accepted = ", ".join(sorted(_DELTA_COMPRESSION_CODECS | {"uncompressed"}))
        raise ValueError(
            f"Unsupported compression codec {compression!r} for Delta writes. "
            f"Accepted codecs: {accepted}."
        )
    return codec
```

Note the rejection message must contain the substring `pyarrow` (case-insensitively) — both reasons do.

- [ ] **Step 4: Run to verify they pass**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_compression.py"`
Expected: all pass. No `make build` needed — pure Python.

- [ ] **Step 5: Prove the whitespace tests aren't vacuous**

Temporarily remove `.strip()` from `normalize_delta_compression`. Re-run. Both `test_codec_names_are_case_insensitive_and_stripped[ lz4 ]` and `test_pyarrow_unencodable_codecs_are_rejected_with_their_own_reason[  lzo  ]` must FAIL (the latter because `"  lzo  "` falls through to the generic branch, which the `"unsupported compression codec" not in msg` assertion catches). Restore `.strip()`.

- [ ] **Step 6: Commit**

```bash
git add daft/io/delta_lake/delta_lake_write.py tests/io/delta_lake/test_delta_compression.py
git commit -m "feat(delta): add compression codec validation for Delta writes"
```

---

### Task 4: Plumb `compression` into the PyArrow writer

**Files:**
- Modify: `src/daft-logical-plan/src/sink_info.rs` (struct field, `multiline_display`, `bind`)
- Modify: `src/daft-logical-plan/src/builder/mod.rs` (`delta_write` + its pyo3 wrapper)
- Modify: `daft/logical/builder.py:420-439`
- Modify: `daft/dataframe/dataframe.py` (`write_deltalake` signature, first-statement validation, builder call, `_write_deltalake_with_checkpoint`)
- Modify: `src/daft-writers/src/catalog.rs` (destructure + pass)
- Modify: `src/daft-writers/src/pyarrow.rs` (`new_deltalake_writer`)
- Modify: `daft/io/writer.py:393-414` (`DeltalakeWriter.__init__`)
- Modify: `daft/daft/__init__.pyi` (the `delta_write` stub)
- Test: `tests/io/delta_lake/test_delta_compression.py` (append)

**Interfaces:**
- Consumes: `normalize_delta_compression` (Task 3).
- Produces: `DeltaLakeCatalogInfo.compression: String`; `PyArrowWriter::new_deltalake_writer(root_dir, file_idx, version, large_dtypes, partition_values, io_config, compression: &str)`; Python `DeltalakeWriter(root_dir, file_idx, version, large_dtypes, partition_values=None, io_config=None, compression="none")`; `DataFrame.write_deltalake(..., compression: str = "snappy")`.

This must land as one commit: `delta_write` gains a parameter, so the Rust builder, its pyo3 wrapper, and `builder.py` change together or the tree does not compile.

- [ ] **Step 1: Write the failing tests**

Append to `tests/io/delta_lake/test_delta_compression.py`:

```python
import json

import daft


def _sole_add(tmp_path):
    log = tmp_path / "_delta_log" / "00000000000000000000.json"
    adds = [
        json.loads(line)["add"]
        for line in log.read_text().splitlines()
        if "add" in json.loads(line)
    ]
    assert len(adds) == 1
    return adds[0]


def _codec_of(tmp_path):
    import pyarrow.parquet as pq

    add = _sole_add(tmp_path)
    return pq.ParquetFile(tmp_path / add["path"]).metadata.row_group(0).column(0).compression


def test_default_is_snappy(tmp_path):
    daft.from_pydict({"a": [1, 2, 3]}).write_deltalake(str(tmp_path))
    assert _codec_of(tmp_path) == "SNAPPY"


@pytest.mark.parametrize("codec", ["none", "snappy", "gzip", "brotli", "lz4", "zstd"])
def test_codec_is_applied_and_table_is_readable(tmp_path, codec):
    daft.from_pydict({"a": [1, 2, 3], "s": ["x", "y", "z"]}).write_deltalake(
        str(tmp_path), compression=codec
    )
    expected = "UNCOMPRESSED" if codec == "none" else codec.upper()
    assert _codec_of(tmp_path) == expected

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().sort_by("a")
    assert table.column("a").to_pylist() == [1, 2, 3]
    assert table.column("s").to_pylist() == ["x", "y", "z"]


def test_uncompressed_matches_none(tmp_path):
    daft.from_pydict({"a": [1]}).write_deltalake(str(tmp_path), compression="uncompressed")
    assert _codec_of(tmp_path) == "UNCOMPRESSED"


@pytest.mark.parametrize("codec", ["lz4_raw", "lzo"])
def test_unencodable_codec_rejected_before_anything_is_written(tmp_path, codec):
    with pytest.raises(ValueError, match=codec):
        daft.from_pydict({"a": [1]}).write_deltalake(str(tmp_path), compression=codec)
    assert not (tmp_path / "_delta_log").exists()


def test_mixed_codec_table_reads_correctly(tmp_path):
    """Old uncompressed files stay readable after the snappy default lands.

    Parquet records the codec per column chunk inside each file, so a table containing a
    mix of codecs decodes correctly.
    """
    import glob

    import pyarrow.parquet as pq

    p = str(tmp_path)
    daft.from_pydict({"a": [1, 2], "s": ["x", "y"]}).write_deltalake(p, compression="none")
    daft.from_pydict({"a": [3, 4], "s": ["z", "w"]}).write_deltalake(
        p, mode="append", compression="snappy"
    )
    daft.from_pydict({"a": [5], "s": ["q"]}).write_deltalake(
        p, mode="append", compression="zstd"
    )

    codecs = {
        pq.ParquetFile(f).metadata.row_group(0).column(0).compression
        for f in glob.glob(f"{p}/*.parquet")
    }
    assert codecs == {"UNCOMPRESSED", "SNAPPY", "ZSTD"}

    from_delta_rs = deltalake.DeltaTable(p).to_pyarrow_table().sort_by("a")
    assert from_delta_rs.column("a").to_pylist() == [1, 2, 3, 4, 5]
    from_daft = daft.read_deltalake(p).sort("a").to_pydict()
    assert from_daft["a"] == [1, 2, 3, 4, 5]
    assert from_daft["s"] == ["x", "y", "z", "w", "q"]
```

- [ ] **Step 2: Run to verify they fail**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_compression.py -k 'default_is_snappy or codec_is_applied'"`
Expected: FAIL — `write_deltalake() got an unexpected keyword argument 'compression'`.

- [ ] **Step 3: Add the field to `DeltaLakeCatalogInfo`**

In `src/daft-logical-plan/src/sink_info.rs`, after `large_dtypes` in the struct:

```rust
    pub large_dtypes: bool,
    /// Canonical parquet compression codec name, already validated and normalized on the
    /// Python side by `normalize_delta_compression`.
    pub compression: String,
```

In `multiline_display`, after the `Large Dtypes` push:

```rust
        res.push(format!("Large Dtypes = {}", self.large_dtypes));
        res.push(format!("Compression = {}", self.compression));
```

The struct also has a `bind()` method (python-feature-gated) that reconstructs it field-by-field. Add `compression: self.compression` there. If you miss it, `cargo check` passes and `make build` fails — build before you trust it.

- [ ] **Step 4: Thread it through the Rust builder and its pyo3 wrapper**

In `src/daft-logical-plan/src/builder/mod.rs`, `delta_write` gains a parameter after `large_dtypes`:

```rust
    pub fn delta_write(
        &self,
        path: String,
        columns_name: Vec<String>,
        mode: String,
        version: i32,
        large_dtypes: bool,
        compression: String,
        partition_cols: Option<Vec<String>>,
        io_config: Option<IOConfig>,
    ) -> DaftResult<Self> {
```

and the struct literal gains `compression,` after `large_dtypes,`.

The pyo3 wrapper mirrors it. Insert a bare `compression` into its `#[pyo3(signature = (...))]` list, after `large_dtypes` and before `partition_cols=None`:

```rust
    pub fn delta_write(
        &self,
        path: String,
        columns_name: Vec<String>,
        mode: String,
        version: i32,
        large_dtypes: bool,
        compression: String,
        partition_cols: Option<Vec<String>>,
        io_config: Option<common_io_config::python::IOConfig>,
    ) -> PyResult<Self> {
        Ok(self
            .builder
            .delta_write(
                path,
                columns_name,
                mode,
                version,
                large_dtypes,
                compression,
                partition_cols,
                io_config.map(|cfg| cfg.config),
            )?
            .into())
    }
```

Mirror the new positional in the `delta_write` stub in `daft/daft/__init__.pyi`.

- [ ] **Step 5: Thread it through `builder.py`**

`daft/logical/builder.py:420`:

```python
    def write_deltalake(
        self,
        path: str | pathlib.Path,
        mode: str,
        version: int,
        large_dtypes: bool,
        io_config: IOConfig,
        compression: str = "none",
        partition_cols: list[str] | None = None,
    ) -> LogicalPlanBuilder:
        columns_name = self.schema().column_names()
        builder = self._builder.delta_write(
            str(path),
            columns_name,
            mode,
            version,
            large_dtypes,
            compression,
            partition_cols,
            io_config,
        )
        return LogicalPlanBuilder(builder)
```

The `"none"` default here is a safety net for callers that predate this change; every real caller passes a codec explicitly.

- [ ] **Step 6: Add the public parameter on `DataFrame.write_deltalake`**

In `daft/dataframe/dataframe.py`, add `compression` as the LAST parameter of `write_deltalake`, after `checkpoint`, so no positional caller breaks:

```python
        checkpoint: "IdempotentCommit | None" = None,
        compression: str = "snappy",
    ) -> "DataFrame":
```

Add to the docstring's Args section:

```
            compression (str, optional): compression codec applied to every column of the written Delta data files. Defaults to "snappy". Accepts "snappy", "gzip", "zstd", "lz4", "brotli", "uncompressed", or "none" (case-insensitive). "lz4_raw" and "lzo" are not supported, because PyArrow's parquet writer cannot encode them.
```

Normalize as the **very first statement of the function body**, after the docstring and before any table-URI resolution, schema work, or the `checkpoint` delegation:

```python
        from daft.io.delta_lake.delta_lake_write import normalize_delta_compression

        compression = normalize_delta_compression(compression)
```

This ordering is what makes `test_unencodable_codec_rejected_before_anything_is_written`'s `assert not (tmp_path / "_delta_log").exists()` true by construction, and it means the `_write_deltalake_with_checkpoint` delegation receives an already-canonical codec.

Pass it to the builder:

```python
        builder = self._builder.write_deltalake(
            table_uri,
            mode,
            version,
            large_dtypes,
            io_config=io_config,
            compression=compression,
            partition_cols=partition_cols,
        )
```

Then `_write_deltalake_with_checkpoint`: add `compression: str = "none"` as its last parameter, pass `compression=compression` in its own `self._builder.write_deltalake(...)` call, and forward `compression=compression` from `write_deltalake` at the site where it delegates (search for `_write_deltalake_with_checkpoint(` inside `write_deltalake`).

- [ ] **Step 7: Pass it to the PyArrow writer**

`src/daft-writers/src/catalog.rs` — the DeltaLake arm:

```rust
        CatalogType::DeltaLake(DeltaLakeCatalogInfo {
            path,
            version,
            large_dtypes,
            io_config,
            compression,
            ..
        }) => {
            let writer = PyArrowWriter::new_deltalake_writer(
                path,
                file_idx,
                *version,
                *large_dtypes,
                partition_values,
                io_config.as_ref(),
                compression,
            )?;
            Ok(Box::new(writer))
        }
```

`src/daft-writers/src/pyarrow.rs` — `new_deltalake_writer` gains a trailing `compression: &str`, passed as the 7th positional argument to the Python class:

```rust
    pub fn new_deltalake_writer(
        root_dir: &str,
        file_idx: usize,
        version: i32,
        large_dtypes: bool,
        partition_values: Option<&RecordBatch>,
        io_config: Option<&daft_io::IOConfig>,
        compression: &str,
    ) -> DaftResult<Self> {
```

and inside, the `call1` tuple:

```rust
            let py_writer = file_writer_class.call1((
                root_dir,
                file_idx,
                version,
                large_dtypes,
                partition_values,
                io_config.map(|cfg| daft_io::python::IOConfig {
                    config: cfg.clone(),
                }),
                compression,
            ))?;
```

**The `call1` tuple is positional.** `DeltalakeWriter.__init__`'s new parameter must sit at the matching position. An off-by-one silently passes the IOConfig as the codec.

- [ ] **Step 8: Accept it in the Python `DeltalakeWriter`**

`daft/io/writer.py:393` — add `compression` after `io_config`, and stop hardcoding `compression=None`:

```python
class DeltalakeWriter(ParquetFileWriter):
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        version: int,
        large_dtypes: bool,
        partition_values: RecordBatch | None = None,
        io_config: IOConfig | None = None,
        compression: str = "none",
    ):
        super().__init__(
            root_dir=root_dir,
            file_idx=file_idx,
            partition_values=partition_values,
            compression=compression,
            io_config=io_config,
            version=version,
            default_partition_fallback=None,
            metadata_collector=[],
        )

        self.large_dtypes = large_dtypes
```

`ParquetFileWriter` already forwards `compression` to `pq.ParquetWriter`; `FileWriterBase` normalizes `None` → `"none"`, so `"none"` yields an uncompressed file.

- [ ] **Step 9: Build and run**

```bash
cargo check -p daft-logical-plan -p daft-writers
make build
DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_compression.py"
DAFT_RUNNER=native make test EXTRA_ARGS="tests/io/delta_lake"
```
Expected: all pass. `test_mixed_codec_table_reads_correctly` is the answer to "can we still read existing uncompressed files" — it must pass.

- [ ] **Step 10: Commit**

```bash
git add src/daft-logical-plan/src/sink_info.rs src/daft-logical-plan/src/builder/mod.rs \
        src/daft-writers/src/catalog.rs src/daft-writers/src/pyarrow.rs \
        daft/logical/builder.py daft/dataframe/dataframe.py daft/io/writer.py \
        daft/daft/__init__.pyi tests/io/delta_lake/test_delta_compression.py
git commit -m "feat(delta): honor a compression parameter in the PyArrow Delta writer"
```

---

### Task 5: Compression for `distributed_merge_deltalake`

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` — `distributed_merge_deltalake` (~line 563), `DistributedDeltaMergeBuilder.__init__` (~line 925), `_write_partition_scoped` (~line 1226), `execute()`'s full-rewrite branch (~line 1711)
- Test: `tests/io/delta_lake/test_delta_merge_compression.py` (create)

**Interfaces:**
- Consumes: `normalize_delta_compression` (Task 3); `DataFrame.write_deltalake(..., compression=...)` and `LogicalPlanBuilder.write_deltalake(..., compression=...)` (Task 4).

**Why both branches.** `_write_partition_scoped` calls `write_df._builder.write_deltalake(...)` — the *builder*, which defaults to `"none"` — while `execute()`'s full-rewrite branch calls `write_df.write_deltalake(...)` — the *DataFrame method*, which defaults to `"snappy"`. Neither inherits the other's default. Both must be passed the codec explicitly, or the two merge strategies silently produce differently-compressed files.

- [ ] **Step 1: Write the failing tests**

Create `tests/io/delta_lake/test_delta_merge_compression.py`:

```python
from __future__ import annotations

import glob

import pytest

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa
import pyarrow.parquet as pq

import daft
from daft.io.delta_lake._deltalake import distributed_merge_deltalake


def _codecs(path) -> set[str]:
    return {
        pq.ParquetFile(f).metadata.row_group(0).column(0).compression
        for f in glob.glob(f"{path}/**/*.parquet", recursive=True)
    }


def _seed(path, partitioned: bool):
    df = daft.from_arrow(
        pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "p": pa.array(["a", "b"]),
                "v": pa.array([10, 20], pa.int64()),
            }
        )
    )
    kwargs = {"partition_cols": ["p"]} if partitioned else {}
    df.write_deltalake(str(path), compression="none", **kwargs)


def _merge(path, compression):
    src = daft.from_arrow(
        pa.table(
            {
                "k": pa.array([2, 3], pa.int64()),
                "p": pa.array(["b", "c"]),
                "v": pa.array([99, 30], pa.int64()),
            }
        )
    )
    builder = distributed_merge_deltalake(
        str(path), src, predicate="source.k = target.k", on="k", compression=compression
    )
    builder.when_matched_update_all().when_not_matched_insert_all().execute()


def test_distributed_merge_defaults_to_snappy(tmp_path):
    _seed(tmp_path, partitioned=False)
    _merge(tmp_path, compression="snappy")
    # Seed file is UNCOMPRESSED; every file the merge wrote must be SNAPPY.
    assert "SNAPPY" in _codecs(tmp_path)


@pytest.mark.parametrize("partitioned", [False, True], ids=["full_rewrite", "partition_scoped"])
def test_distributed_merge_honors_compression_on_both_branches(tmp_path, partitioned):
    """The partition-scoped branch calls the builder directly and would otherwise
    silently fall back to the builder's 'none' default."""
    _seed(tmp_path, partitioned=partitioned)
    _merge(tmp_path, compression="zstd")

    codecs = _codecs(tmp_path)
    assert "ZSTD" in codecs, codecs

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().sort_by("k")
    assert table.column("k").to_pylist() == [1, 2, 3]
    assert table.column("v").to_pylist() == [10, 99, 30]


def test_distributed_merge_rejects_unencodable_codec(tmp_path):
    _seed(tmp_path, partitioned=False)
    src = daft.from_arrow(pa.table({"k": pa.array([3], pa.int64()), "p": pa.array(["c"]), "v": pa.array([30], pa.int64())}))
    with pytest.raises(ValueError, match="lzo"):
        distributed_merge_deltalake(
            str(tmp_path), src, predicate="source.k = target.k", on="k", compression="lzo"
        )
```

- [ ] **Step 2: Run to verify they fail**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_merge_compression.py"`
Expected: FAIL — `distributed_merge_deltalake() got an unexpected keyword argument 'compression'`.

- [ ] **Step 3: Add the parameter and thread it**

In `daft/io/delta_lake/_deltalake.py`:

`distributed_merge_deltalake` gains `compression: str = "snappy"` as its last parameter. Validate it as the first statement of the body, before anything else:

```python
    from daft.io.delta_lake.delta_lake_write import normalize_delta_compression

    compression = normalize_delta_compression(compression)
```

Document it:

```
        compression: Compression codec for the parquet data files this merge writes.
            Defaults to "snappy". See :meth:`daft.DataFrame.write_deltalake`.
```

Pass `compression=compression` into the `DistributedDeltaMergeBuilder(...)` construction.

`DistributedDeltaMergeBuilder.__init__` gains `compression: str = "snappy"` as its last parameter and stores `self._compression = compression`.

`_write_partition_scoped`'s builder call:

```python
        builder = write_df._builder.write_deltalake(
            self._resolved_table.table_uri,
            "overwrite",
            pinned_version + 1,
            True,
            io_config=io_config,
            compression=self._compression,
            partition_cols=list(partition_cols),
        )
```

`execute()`'s full-rewrite call:

```python
                write_df.write_deltalake(
                    self._resolved_table.table_uri,
                    mode="overwrite",
                    schema_mode="overwrite",
                    partition_cols=partition_cols if partition_cols else None,
                    custom_metadata=merge_metadata,
                    io_config=self._io_config,
                    compression=self._compression,
                )
```

- [ ] **Step 4: Run to verify they pass**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_merge_compression.py"`
Expected: all pass. No `make build` needed — pure Python.

- [ ] **Step 5: Prove the partition-scoped branch is really covered**

Temporarily remove `compression=self._compression` from `_write_partition_scoped`'s builder call. Re-run. `test_distributed_merge_honors_compression_on_both_branches[partition_scoped]` must FAIL (the merge writes `UNCOMPRESSED`, since the builder's default is `"none"`). Restore it. Quote both outputs in your report — this is the assertion that proves the partition-scoped branch does not inherit the DataFrame default.

- [ ] **Step 6: Commit**

```bash
git add daft/io/delta_lake/_deltalake.py tests/io/delta_lake/test_delta_merge_compression.py
git commit -m "feat(delta): honor compression on both distributed-merge write branches"
```

---

### Task 6: Compression for `merge_deltalake`

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` — `merge_deltalake` (~line 234), and its `resolved_table.merge(...)` call (~line 388)
- Test: `tests/io/delta_lake/test_delta_merge_compression.py` (append)

**Interfaces:**
- Consumes: `normalize_delta_compression` (Task 3).

**Why a `None` sentinel here and not elsewhere.** `merge_deltalake` delegates to delta-rs's own merge engine and already accepts `writer_properties: deltalake.WriterProperties`, which carries its own `compression` field. The two must never be silently merged, because `dataclasses.replace` on `WriterProperties` **destroys every other option**: the class is decorated `@dataclass` but declares no annotated fields, so `__dataclass_fields__` is empty and `replace()` returns a fresh default object.

| `compression` | `writer_properties` | Behavior |
|---|---|---|
| `None` | `None` | `WriterProperties(compression="SNAPPY")` |
| given | `None` | `WriterProperties(compression=<UPPERCASE>)` |
| `None` | given | passed through untouched |
| given | given | `ValueError` |

delta-rs rejects `'NONE'`; `"none"` maps to `'UNCOMPRESSED'`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/io/delta_lake/test_delta_merge_compression.py`:

```python
from daft.io.delta_lake._deltalake import merge_deltalake


def _seed_simple(path):
    daft.from_arrow(
        pa.table({"k": pa.array([1, 2], pa.int64()), "v": pa.array([10, 20], pa.int64())})
    ).write_deltalake(str(path), compression="none")


def _source():
    return pa.table({"k": pa.array([2, 3], pa.int64()), "v": pa.array([99, 30], pa.int64())})


def test_merge_deltalake_defaults_to_snappy(tmp_path):
    _seed_simple(tmp_path)
    (
        merge_deltalake(str(tmp_path), _source(), predicate="s.k = t.k", source_alias="s", target_alias="t")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    assert "SNAPPY" in _codecs(tmp_path)


def test_merge_deltalake_honors_explicit_compression(tmp_path):
    _seed_simple(tmp_path)
    (
        merge_deltalake(
            str(tmp_path), _source(), predicate="s.k = t.k",
            source_alias="s", target_alias="t", compression="zstd",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    assert "ZSTD" in _codecs(tmp_path)


def test_merge_deltalake_rejects_both_compression_and_writer_properties(tmp_path):
    _seed_simple(tmp_path)
    wp = deltalake.WriterProperties(compression="ZSTD")
    with pytest.raises(ValueError, match="writer_properties"):
        merge_deltalake(
            str(tmp_path), _source(), predicate="s.k = t.k",
            source_alias="s", target_alias="t",
            compression="snappy", writer_properties=wp,
        )


def test_merge_deltalake_passes_writer_properties_through_untouched(tmp_path):
    """A `dataclasses.replace`-based implementation would silently reset these fields.

    `deltalake.WriterProperties` is decorated @dataclass but declares no fields, so
    `replace()` copies nothing and returns a default object.
    """
    _seed_simple(tmp_path)
    wp = deltalake.WriterProperties(
        compression="ZSTD", max_row_group_size=4096, write_batch_size=512
    )
    (
        merge_deltalake(
            str(tmp_path), _source(), predicate="s.k = t.k",
            source_alias="s", target_alias="t", writer_properties=wp,
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    # The object we handed in must be unmodified, and its other options intact.
    assert vars(wp)["max_row_group_size"] == 4096
    assert vars(wp)["write_batch_size"] == 512
    assert "ZSTD" in _codecs(tmp_path)


def test_writer_properties_replace_is_lossy_upstream_bug():
    """Pins WHY we never call dataclasses.replace on WriterProperties."""
    import dataclasses

    wp = deltalake.WriterProperties(compression="SNAPPY", max_row_group_size=4096)
    assert dataclasses.fields(wp) == ()  # decorated @dataclass, declares no fields
    replaced = dataclasses.replace(wp, compression="ZSTD")
    assert vars(replaced)["max_row_group_size"] is None  # silently destroyed
```

- [ ] **Step 2: Run to verify they fail**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_merge_compression.py -k merge_deltalake"`
Expected: FAIL — `merge_deltalake() got an unexpected keyword argument 'compression'`.

- [ ] **Step 3: Implement**

`merge_deltalake` gains `compression: str | None = None` as its last parameter, documented as defaulting to snappy when `writer_properties` is not supplied.

As the first statement of the body:

```python
    if compression is not None and writer_properties is not None:
        raise ValueError(
            "Pass either `compression` or `writer_properties`, not both. "
            "`writer_properties` already carries a `compression` field; setting both is "
            "ambiguous, and deltalake's WriterProperties cannot be safely copied "
            "(it is decorated @dataclass but declares no fields, so dataclasses.replace "
            "silently resets every other option)."
        )

    if writer_properties is None:
        from daft.io.delta_lake.delta_lake_write import normalize_delta_compression

        codec = normalize_delta_compression("snappy" if compression is None else compression)
        # delta-rs spells uncompressed as 'UNCOMPRESSED'; it rejects 'NONE'.
        wp_codec = "UNCOMPRESSED" if codec == "none" else codec.upper()
        writer_properties = deltalake.WriterProperties(compression=wp_codec)
```

Leave the `resolved_table.merge(..., writer_properties=writer_properties, ...)` call as it is — it now receives the constructed object.

Note `deltalake` is imported lazily elsewhere in this module; follow the file's existing import convention.

- [ ] **Step 4: Run to verify they pass**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_merge_compression.py"`
Expected: all pass, including the two `distributed_merge` tests from Task 5.

- [ ] **Step 5: Commit**

```bash
git add daft/io/delta_lake/_deltalake.py tests/io/delta_lake/test_delta_merge_compression.py
git commit -m "feat(delta): honor compression in merge_deltalake without mutating WriterProperties"
```

---

### Task 7: Fix 2 — widen unsigned integers

**Files:**
- Modify: `daft/io/delta_lake/delta_lake_write.py` — `convert_pa_schema_to_delta` and a new `_widen_unsigned_type` helper; `sanitize_table_for_deltalake`
- Test: `tests/io/delta_lake/test_delta_unsigned.py` (create)

**Interfaces:**
- Consumes: `_normalize_delta_timestamp_type` from Task 1 (the same recursion shape).

**The bug.** Delta Lake has no unsigned types. delta-rs maps `uint8→byte`(int8), `uint16→short`(int16), `uint32→integer`(int32), `uint64→long`(int64). Any unsigned value above the corresponding **signed** maximum commits successfully and then cannot be read: `ArrowInvalid: Integer value 3000000000 not in range: 0 to 2147483647`. This affects `uint8` above 127 as readily as `uint32`.

**The fix.** Widen in the schema conversion; `sanitize_table_for_deltalake` already casts the table to that schema, so values follow. `uint8→int16`, `uint16→int32`, `uint32→int64` are lossless for every representable value. `uint64→int64` is lossless up to `i64::MAX` and the cast **raises** beyond it, which we wrap in a clear error.

- [ ] **Step 1: Write the failing tests**

Create `tests/io/delta_lake/test_delta_unsigned.py`:

```python
from __future__ import annotations

import json

import pytest

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa

import daft


def _delta_type(path, field="u"):
    schema = json.loads(deltalake.DeltaTable(str(path)).schema().to_json())
    return {f["name"]: f["type"] for f in schema["fields"]}[field]


@pytest.mark.parametrize(
    ("arrow_type", "max_value", "expected_delta_type"),
    [
        (pa.uint8(), 255, "short"),
        (pa.uint16(), 65535, "integer"),
        (pa.uint32(), 4294967295, "long"),
        (pa.uint64(), (1 << 63) - 1, "long"),
    ],
)
def test_unsigned_max_value_round_trips(tmp_path, arrow_type, max_value, expected_delta_type):
    """Today uint8>127 and uint32>2^31-1 commit a table that raises on read."""
    tbl = pa.table({"u": pa.array([0, max_value], arrow_type)})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    assert _delta_type(tmp_path) == expected_delta_type
    got = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("u").to_pylist()
    assert sorted(got) == [0, max_value]


def test_uint64_above_i64_max_raises_naming_the_column(tmp_path):
    tbl = pa.table({"u": pa.array([1, (1 << 63) + 42], pa.uint64())})
    with pytest.raises(ValueError) as excinfo:
        daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    msg = str(excinfo.value)
    assert "u" in msg
    assert "uint64" in msg.lower()


def test_unsigned_nested_in_struct_is_widened(tmp_path):
    struct_type = pa.struct([pa.field("n", pa.uint32())])
    tbl = pa.table({"s": pa.array([{"n": 3_000_000_000}], type=struct_type)})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    got = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("s").to_pylist()[0]
    assert got["n"] == 3_000_000_000


def test_unsigned_nested_in_list_is_widened(tmp_path):
    tbl = pa.table({"l": pa.array([[3_000_000_000]], type=pa.list_(pa.uint32()))})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    got = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("l").to_pylist()[0]
    assert got == [3_000_000_000]


def test_signed_types_are_untouched(tmp_path):
    tbl = pa.table({"u": pa.array([-1, 5], pa.int32())})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    assert _delta_type(tmp_path) == "integer"
```

- [ ] **Step 2: Run to verify they fail**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_unsigned.py"`
Expected: `test_unsigned_max_value_round_trips[uint8]` and `[uint32]` FAIL with `ArrowInvalid: Integer value ... not in range`, proving the bug. `test_uint64_above_i64_max_raises_naming_the_column` fails because no `ValueError` is raised.

- [ ] **Step 3: Implement the widening**

In `daft/io/delta_lake/delta_lake_write.py`, beside `_normalize_delta_timestamp_type`:

```python
# Delta Lake has no unsigned types: delta-rs maps uint8->byte(int8), uint16->short(int16),
# uint32->integer(int32), uint64->long(int64). Any value above the corresponding SIGNED
# maximum commits and then cannot be read. Widen to the next signed type that holds every
# value. uint64 has no lossless target; values above i64::MAX raise at cast time.
_DELTA_UNSIGNED_WIDENING = [
    ("uint8", "int16"),
    ("uint16", "int32"),
    ("uint32", "int64"),
    ("uint64", "int64"),
]


def _widen_unsigned_type(dtype: pa.DataType) -> pa.DataType:
    """Rewrite unsigned integer types to a signed type Delta can represent.

    Recurses into nested types. uint8/uint16/uint32 widen losslessly. uint64 widens to
    int64, which is lossless up to i64::MAX; beyond that the table cast raises, which is
    strictly better than committing a table nobody can read.
    """
    from daft.dependencies import pa

    def widen_field(field: pa.Field) -> pa.Field:
        return field.with_type(_widen_unsigned_type(field.type))

    if pa.types.is_uint8(dtype):
        return pa.int16()
    if pa.types.is_uint16(dtype):
        return pa.int32()
    if pa.types.is_uint32(dtype) or pa.types.is_uint64(dtype):
        return pa.int64()
    if pa.types.is_struct(dtype):
        return pa.struct([widen_field(dtype.field(i)) for i in range(dtype.num_fields)])
    if pa.types.is_large_list(dtype):
        return pa.large_list(widen_field(dtype.value_field))
    if pa.types.is_fixed_size_list(dtype):
        return pa.list_(widen_field(dtype.value_field), dtype.list_size)
    if pa.types.is_list(dtype):
        return pa.list_(widen_field(dtype.value_field))
    if pa.types.is_map(dtype):
        return pa.map_(
            widen_field(dtype.key_field), widen_field(dtype.item_field),
            keys_sorted=dtype.keys_sorted,
        )
    return dtype


def _widen_unsigned_schema(schema: pa.Schema) -> pa.Schema:
    from daft.dependencies import pa

    widened = [field.with_type(_widen_unsigned_type(field.type)) for field in schema]
    return pa.schema(widened, metadata=schema.metadata)
```

In `convert_pa_schema_to_delta`, apply it in the `>= 1.0.0` branch alongside the tz normalization:

```python
    else:
        # deltalake>=1.0.0 passes Arrow data types through without modification, EXCEPT
        # that Schema.from_arrow rejects tz-aware timestamps whose timezone is not the
        # literal "UTC", and Delta has no unsigned integer types.
        return _widen_unsigned_schema(_normalize_delta_schema_timestamps(schema))
```

In `sanitize_table_for_deltalake`, wrap the existing `arrow_table.cast(arrow_batch)` so a `uint64` overflow becomes a clear error:

```python
    try:
        return arrow_table.cast(arrow_batch)
    except Exception as exc:  # pa.ArrowInvalid and friends
        unsigned = [
            f.name for f in arrow_table.schema
            if str(f.type).startswith("uint")
        ]
        if unsigned:
            raise ValueError(
                f"Failed to write column(s) {unsigned} to Delta Lake. Delta has no "
                f"unsigned integer types; uint64 values above 2**63-1 cannot be "
                f"represented. Cast the column explicitly (e.g. to decimal) before "
                f"writing. Original error: {exc}"
            ) from exc
        raise
```

Do not touch the `< 1.0.0` branches — older deltalake versions do their own conversion.

- [ ] **Step 4: Run to verify they pass**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_unsigned.py"`
Expected: all pass. Pure Python — no `make build`.

- [ ] **Step 5: Run the full delta suite for regressions**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="tests/io/delta_lake"`
Expected: no failures.

- [ ] **Step 6: Commit**

```bash
git add daft/io/delta_lake/delta_lake_write.py tests/io/delta_lake/test_delta_unsigned.py
git commit -m "fix(delta): widen unsigned integers so Delta tables stay readable"
```

---

### Task 8: Fix 3 — the distributed merge silently nulls values that don't fit

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` — add `_is_definitely_lossless_cast` and `_check_source_casts`, call the check early in `DistributedDeltaMergeBuilder.execute()`
- Test: `tests/io/delta_lake/test_delta_merge_cast_guard.py` (create)

**Interfaces:**
- Produces: `_check_source_casts(self) -> None`, raising `ValueError` when aligning the source to the target schema would turn a non-null value into `NULL`.

**The bug, measured.** `DistributedDeltaMergeBuilder._decomposed_outer_join` (~line 1142) aligns source rows to the target schema with `col(name).cast(f.dtype)`. **Daft's `cast` returns `NULL` on overflow** where PyArrow raises:

```
daft    uint32 -> int32 : [None]          <- silently loses the value
pyarrow uint32 -> int32 : ArrowInvalid    <- refuses
```

Seed a table with a `uint32` column (Delta declares it `integer`), merge a row whose value is `3_000_000_000`: the merge commits, the table reads back with the right row count, and the value is `None`. Nothing fails. This is strictly worse than the `write_deltalake` unsigned bug, which at least fails loudly on read.

**Why the guard is data-dependent.** Statically rejecting every narrowing cast would break merges that narrow correctly today because their values happen to fit. The classifier below is *sound for lossless only*: it returns `True` only for casts that can never lose a value. Everything else is guarded by an actual null-introduction check, which cannot produce a false error.

- [ ] **Step 1: Write the failing tests**

Create `tests/io/delta_lake/test_delta_merge_cast_guard.py`:

```python
from __future__ import annotations

import pytest

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa

import daft
from daft.io.delta_lake._deltalake import distributed_merge_deltalake


def _merge(path, source_table):
    builder = distributed_merge_deltalake(
        str(path), daft.from_arrow(source_table), predicate="source.k = target.k", on="k"
    )
    builder.when_matched_update_all().when_not_matched_insert_all().execute()


def test_merge_into_narrower_column_raises_instead_of_nulling(tmp_path):
    """A pre-existing `integer` column cannot hold 3e9. Today the value silently becomes
    NULL and the merge reports success."""
    # Seed a table whose Delta type is `integer` (int32), bypassing Fix 2's widening.
    daft.from_arrow(
        pa.table({"k": pa.array([1], pa.int64()), "v": pa.array([10], pa.int32())})
    ).write_deltalake(str(tmp_path))

    src = pa.table({"k": pa.array([2], pa.int64()), "v": pa.array([3_000_000_000], pa.int64())})
    with pytest.raises(ValueError) as excinfo:
        _merge(tmp_path, src)

    msg = str(excinfo.value)
    assert "v" in msg
    assert "Int64" in msg and "Int32" in msg

    # And nothing was committed beyond the seed.
    assert deltalake.DeltaTable(str(tmp_path)).version() == 0


def test_merge_that_narrows_within_range_still_succeeds(tmp_path):
    """The guard must be data-dependent, not type-dependent: an int64 source whose values
    all fit in an int32 target is a legitimate merge."""
    daft.from_arrow(
        pa.table({"k": pa.array([1], pa.int64()), "v": pa.array([10], pa.int32())})
    ).write_deltalake(str(tmp_path))

    src = pa.table({"k": pa.array([2], pa.int64()), "v": pa.array([42], pa.int64())})
    _merge(tmp_path, src)

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().sort_by("k")
    assert table.column("v").to_pylist() == [10, 42]


def test_null_source_values_do_not_trip_the_guard(tmp_path):
    daft.from_arrow(
        pa.table({"k": pa.array([1], pa.int64()), "v": pa.array([10], pa.int32())})
    ).write_deltalake(str(tmp_path))

    src = pa.table({"k": pa.array([2], pa.int64()), "v": pa.array([None], pa.int64())})
    _merge(tmp_path, src)

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().sort_by("k")
    assert table.column("v").to_pylist() == [10, None]


def test_merge_into_widened_column_preserves_the_value(tmp_path):
    """With Fix 2, a uint32 column is declared `long`, so 3e9 needs no narrowing cast."""
    daft.from_arrow(
        pa.table({"k": pa.array([1], pa.int64()), "v": pa.array([10], pa.uint32())})
    ).write_deltalake(str(tmp_path))

    src = pa.table({"k": pa.array([2], pa.int64()), "v": pa.array([3_000_000_000], pa.uint32())})
    _merge(tmp_path, src)

    table = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().sort_by("k")
    assert table.column("v").to_pylist() == [10, 3_000_000_000]
```

- [ ] **Step 2: Run to verify they fail**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_merge_cast_guard.py"`
Expected: `test_merge_into_narrower_column_raises_instead_of_nulling` FAILS — no `ValueError` is raised, the merge commits, and the value is `None`. That failure is the bug.

- [ ] **Step 3: Implement the guard**

In `daft/io/delta_lake/_deltalake.py`, at module level:

```python
# Casts that can never lose a value. Sound for lossless only: anything absent from this
# set is guarded by an actual null-introduction check, so a missing entry costs one extra
# pass but never a wrong error.
_LOSSLESS_NUMERIC_WIDENINGS = {
    ("Int8", "Int16"), ("Int8", "Int32"), ("Int8", "Int64"),
    ("Int16", "Int32"), ("Int16", "Int64"),
    ("Int32", "Int64"),
    ("UInt8", "UInt16"), ("UInt8", "UInt32"), ("UInt8", "UInt64"),
    ("UInt16", "UInt32"), ("UInt16", "UInt64"),
    ("UInt32", "UInt64"),
    ("UInt8", "Int16"), ("UInt8", "Int32"), ("UInt8", "Int64"),
    ("UInt16", "Int32"), ("UInt16", "Int64"),
    ("UInt32", "Int64"),
    ("Float32", "Float64"),
}


def _is_definitely_lossless_cast(src: "DataType", dst: "DataType") -> bool:
    """True only when every representable source value survives the cast unchanged."""
    if src == dst:
        return True
    return (str(src), str(dst)) in _LOSSLESS_NUMERIC_WIDENINGS
```

`DataType` is a forward reference; add `from daft.datatype import DataType` to this module's
`TYPE_CHECKING` block if it is not already imported. The keys are `str(dtype)` values —
`str(daft.DataType.uint32()) == "UInt32"`, `str(daft.DataType.int64()) == "Int64"`.

On `DistributedDeltaMergeBuilder`:

```python
    def _target_dtypes(self) -> "dict[str, DataType]":
        """Daft dtypes of the target table's columns, from the committed Delta schema.

        `deltalake.Schema.to_arrow()` returns an arro3 schema, not a PyArrow one;
        `pa.schema(...)` converts it via the Arrow C interface without reading any data.
        """
        import pyarrow as pa

        import daft

        arrow_schema = pa.schema(self._resolved_table.schema().to_arrow())
        return {f.name: f.dtype for f in daft.Schema.from_pyarrow_schema(arrow_schema)}

    def _check_source_casts(self) -> None:
        """Raise if aligning the source to the target schema would turn a value into NULL.

        Daft's `cast` returns NULL on overflow where PyArrow raises. The merge aligns the
        source to the target's dtypes, so an out-of-range value is silently discarded and
        the merge reports success. Guard only the casts that are not provably lossless;
        the check is data-dependent, so a merge that narrows within range still succeeds.
        """
        import daft
        from daft import col

        target_dtypes = self._target_dtypes()
        risky = [
            f
            for f in self._source.schema()
            if f.name in target_dtypes
            and not _is_definitely_lossless_cast(f.dtype, target_dtypes[f.name])
        ]
        if not risky:
            return

        # A row whose source value is non-null but whose cast result is null is a value
        # that did not fit. Count them per column in one pass.
        lost = (
            self._source.select(
                *[
                    (
                        (~col(f.name).is_null())
                        & col(f.name).cast(target_dtypes[f.name]).is_null()
                    )
                    .cast(daft.DataType.int64())
                    .alias(f.name)
                    for f in risky
                ]
            )
            .sum(*[f.name for f in risky])
            .to_pydict()
        )

        for f in risky:
            count = lost[f.name][0] or 0
            if count:
                raise ValueError(
                    f"Merging into column {f.name!r} would discard {count} value(s): "
                    f"casting {f.dtype} to the target's {target_dtypes[f.name]} produces "
                    f"NULL for values outside its range. Cast the column explicitly "
                    f"before merging if that is intended."
                )
```

Note `~expr` — Daft's `Expression` has no `.not_()` method.

Call it as the first statement of `DistributedDeltaMergeBuilder.execute()`, before any join, write, or version pin:

```python
    def execute(self) -> DataFrame:
        self._check_source_casts()
        ...
```

Verified behavior of this exact expression against a target whose `v` is `Int32`:

```
source v = [3_000_000_000] (Int64)  -> lost = {'v': [1]}  -> raises
source v = [42]            (Int64)  -> lost = {'v': [0]}  -> succeeds
source v = [None]          (Int64)  -> lost = {'v': [0]}  -> succeeds
```

- [ ] **Step 4: Run to verify they pass**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_merge_cast_guard.py"`
Expected: all 4 pass.

If `test_merge_that_narrows_within_range_still_succeeds` fails, the guard has become type-dependent — that is a defect, not a test to relax.

- [ ] **Step 5: Run the merge suites for regressions**

```bash
DAFT_RUNNER=native make test EXTRA_ARGS="tests/io/delta_lake"
```
Expected: no failures. `test_delta_merge_compression.py` from Tasks 5-6 must still pass — its merges use matching dtypes, so the guard should not fire.

- [ ] **Step 6: Commit**

```bash
git add daft/io/delta_lake/_deltalake.py tests/io/delta_lake/test_delta_merge_cast_guard.py
git commit -m "fix(delta): raise instead of silently nulling out-of-range merge casts"
```

---

### Task 9: Fix 4 — binary stats assert something untrue

**Files:**
- Modify: `daft/io/delta_lake/delta_lake_write.py` — `get_file_stats_from_metadata` (~line 95-110)
- Test: `tests/io/delta_lake/test_delta_stats.py` (create; Task 10 appends to it)

**Interfaces:** none consumed or produced.

**The bug.** `DeltaJSONEncoder` serializes `bytes` bounds with `obj.decode("unicode_escape", "backslashreplace")`, so `b"\xff\xfe"` becomes the string `"ÿþ"`. That is not what the data contains, and other engines read these bounds for data skipping.

**The fix.** Decode `bytes` bounds as strict UTF-8. If either the min or the max fails, omit **both** for that column while keeping its `nullCount`. A missing bound is always safe; a wrong one is not. UTF-8-decodable binary keeps its bounds — for well-formed UTF-8, byte order equals string order, so the bound stays valid.

- [ ] **Step 1: Write the failing tests**

Create `tests/io/delta_lake/test_delta_stats.py`:

```python
from __future__ import annotations

import json

import pytest

deltalake = pytest.importorskip("deltalake")

import pyarrow as pa

import daft


def stats_of(tmp_path):
    log = tmp_path / "_delta_log" / "00000000000000000000.json"
    adds = [
        json.loads(line)["add"]
        for line in log.read_text().splitlines()
        if "add" in json.loads(line)
    ]
    assert len(adds) == 1
    return json.loads(adds[0]["stats"])


def test_non_utf8_binary_bounds_are_omitted(tmp_path):
    """b"\\xff\\xfe" must not be reported as the string "ÿþ" — that is not the data."""
    tbl = pa.table({"b": pa.array([b"apple", b"\xff\xfe", None], pa.binary())})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    s = stats_of(tmp_path)
    assert "b" not in s["minValues"]
    assert "b" not in s["maxValues"]
    # nullCount is still reported — only the bounds are unsafe.
    assert s["nullCount"]["b"] == 1
    assert s["numRecords"] == 3


def test_utf8_decodable_binary_keeps_its_bounds(tmp_path):
    tbl = pa.table({"b": pa.array([b"apple", b"cherry"], pa.binary())})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    s = stats_of(tmp_path)
    assert s["minValues"]["b"] == "apple"
    assert s["maxValues"]["b"] == "cherry"


def test_binary_data_round_trips_regardless_of_stats(tmp_path):
    tbl = pa.table({"b": pa.array([b"apple", b"\xff\xfe"], pa.binary())})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))

    got = deltalake.DeltaTable(str(tmp_path)).to_pyarrow_table().column("b").to_pylist()
    assert sorted(got) == sorted([b"apple", b"\xff\xfe"])
```

- [ ] **Step 2: Run to verify they fail**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_stats.py"`
Expected: `test_non_utf8_binary_bounds_are_omitted` FAILS — `minValues["b"]` exists and `maxValues["b"] == "ÿþ"`.

- [ ] **Step 3: Implement**

In `daft/io/delta_lake/delta_lake_write.py`, add beside `get_file_stats_from_metadata`:

```python
def _json_safe_bound(value: Any) -> tuple[bool, Any]:
    """Return (is_safe, value) for a parquet min/max bound.

    Binary bounds that are not valid UTF-8 have no faithful JSON representation. The old
    `unicode_escape` decode produced a string that is not what the data contains, and other
    engines read these bounds for data skipping. Omitting a bound is always safe.
    """
    if isinstance(value, bytes):
        try:
            return True, value.decode("utf-8")
        except UnicodeDecodeError:
            return False, None
    return True, value
```

Then in `get_file_stats_from_metadata`, replace the min/max assignment block so both bounds are dropped together when either is unsafe:

```python
            if any(group.column(column_idx).statistics.has_min_max for group in iter_groups(metadata)):
                # Min and Max are recorded in physical type, not logical type.
                minimums = (group.column(column_idx).statistics.min for group in iter_groups(metadata))
                # If some row groups have all null values, their min and max will be null too.
                min_value = min(minimum for minimum in minimums if minimum is not None)
                maximums = (group.column(column_idx).statistics.max for group in iter_groups(metadata))
                max_value = max(maximum for maximum in maximums if maximum is not None)

                min_ok, min_json = _json_safe_bound(min_value)
                max_ok, max_json = _json_safe_bound(max_value)
                if min_ok and max_ok:
                    # Infinity cannot be serialized to JSON, so we skip it. Saying
                    # min/max is infinity is equivalent to saying it is null, anyways.
                    if min_json != -inf:
                        stats["minValues"][name] = min_json
                    if max_json != inf:
                        stats["maxValues"][name] = max_json
```

Delete the now-stale `TODO: Add logic to decode physical type for DATE, DECIMAL` comment and the StackOverflow link above it: DATE, DECIMAL, and TIMESTAMP bounds are all verified correct on this path.

Leave `DeltaJSONEncoder`'s `bytes` branch in place as defense — with this change it should no longer fire for stats.

- [ ] **Step 4: Run to verify they pass**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_stats.py"`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add daft/io/delta_lake/delta_lake_write.py tests/io/delta_lake/test_delta_stats.py
git commit -m "fix(delta): omit binary stats bounds that are not valid UTF-8"
```

---

### Task 10: Stats correctness suite

**Files:**
- Modify: `tests/io/delta_lake/test_delta_stats.py` (append)

**Interfaces:**
- Consumes: `stats_of(tmp_path)` from Task 9.

**Why.** These behaviors are correct today and have no test. The abandoned branch's parity suite covered them by comparing two writers; with one writer, each must be anchored to its **true value**. Without this, a future refactor of `get_file_stats_from_metadata` silently regresses data skipping for every engine that reads Daft's Delta tables.

- [ ] **Step 1: Write the tests**

Append to `tests/io/delta_lake/test_delta_stats.py`:

```python
import datetime
import decimal


def test_date_bounds(tmp_path):
    tbl = pa.table({"d": pa.array([datetime.date(2020, 12, 31), datetime.date(2021, 1, 2)])})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert s["minValues"]["d"] == "2020-12-31"
    assert s["maxValues"]["d"] == "2021-01-02"


def test_decimal_bounds(tmp_path):
    tbl = pa.table(
        {"x": pa.array([decimal.Decimal("-0.05"), decimal.Decimal("123.45")], pa.decimal128(10, 2))}
    )
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert decimal.Decimal(s["minValues"]["x"]) == decimal.Decimal("-0.05")
    assert decimal.Decimal(s["maxValues"]["x"]) == decimal.Decimal("123.45")


def test_tz_aware_timestamp_bounds(tmp_path):
    lo = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    hi = datetime.datetime(2021, 6, 1, 12, 30, tzinfo=datetime.timezone.utc)
    daft.from_pydict({"t": [hi, lo]}).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)

    def instant(v):
        return datetime.datetime.fromisoformat(v.replace("Z", "+00:00"))

    assert instant(s["minValues"]["t"]) == lo
    assert instant(s["maxValues"]["t"]) == hi


def test_naive_timestamp_bounds(tmp_path):
    lo = datetime.datetime(2021, 1, 1, 0, 0, 1)
    hi = datetime.datetime(2021, 6, 1)
    daft.from_pydict({"t": [hi, lo]}).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert datetime.datetime.fromisoformat(s["minValues"]["t"]) == lo
    assert datetime.datetime.fromisoformat(s["maxValues"]["t"]) == hi


def test_int_and_string_bounds(tmp_path):
    daft.from_pydict({"i": [3, -7, 20], "s": ["banana", "apple", "cherry"]}).write_deltalake(
        str(tmp_path)
    )
    s = stats_of(tmp_path)
    assert s["minValues"]["i"] == -7 and s["maxValues"]["i"] == 20
    assert s["minValues"]["s"] == "apple" and s["maxValues"]["s"] == "cherry"


def test_nan_bound_is_dropped(tmp_path):
    daft.from_pydict({"f": [1.0, float("nan")]}).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    # Parquet stats ignore NaN; the finite value survives as the bound.
    assert s["minValues"]["f"] == 1.0


def test_infinite_bounds_are_dropped(tmp_path):
    daft.from_pydict({"f": [float("-inf"), float("inf")]}).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    # Infinity cannot be serialized to JSON; a missing bound is safe.
    assert "f" not in s["minValues"]
    assert "f" not in s["maxValues"]
    # The stats blob must be strict JSON: no NaN/Infinity literals.
    log = (tmp_path / "_delta_log" / "00000000000000000000.json").read_text()
    add = [json.loads(l)["add"] for l in log.splitlines() if "add" in json.loads(l)][0]
    json.loads(add["stats"], parse_constant=lambda c: pytest.fail(f"invalid JSON constant {c}"))


def test_all_null_column_has_null_count_but_no_bounds(tmp_path):
    tbl = pa.table({"a": pa.array([None, None, None], pa.int64())})
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert s["nullCount"]["a"] == 3
    assert "a" not in s["minValues"]
    assert "a" not in s["maxValues"]


def test_nested_struct_uses_dotted_stat_keys(tmp_path):
    tbl = pa.table(
        {
            "point": pa.array(
                [{"x": 1, "y": "a"}, {"x": 9, "y": "b"}],
                type=pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.string())]),
            )
        }
    )
    daft.from_arrow(tbl).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert s["minValues"]["point.x"] == 1 and s["maxValues"]["point.x"] == 9
    assert s["minValues"]["point.y"] == "a" and s["maxValues"]["point.y"] == "b"


def test_long_string_bounds_are_valid(tmp_path):
    """PyArrow emits exact bounds for strings past parquet's 64-byte truncation limit."""
    lo, hi = "a" * 80, "z" * 80
    daft.from_pydict({"s": [lo, hi]}).write_deltalake(str(tmp_path))
    s = stats_of(tmp_path)
    assert s["minValues"]["s"] <= lo
    assert s["maxValues"]["s"] >= hi
```

- [ ] **Step 2: Run**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/delta_lake/test_delta_stats.py"`
Expected: all pass, including Task 9's three binary tests.

**If any of these fail, the assertion is wrong only if the writer emits a bound that is still VALID** (a min at or below the true min, a max at or above the true max). A bound on the wrong side of the true value is a Critical finding — report it rather than adjusting the test.

- [ ] **Step 3: Full regression across the branch**

```bash
DAFT_RUNNER=native make test EXTRA_ARGS="tests/io/delta_lake"
DAFT_RUNNER=native make test EXTRA_ARGS="tests/io/test_parquet.py"
cargo test -p daft-parquet
cargo test -p daft-writers
cargo fmt -p daft-logical-plan -p daft-writers -- --check
```
Expected: all pass. 21 checkpoint tests SKIP under the native runner (Ray-only, expected).

- [ ] **Step 4: Commit**

```bash
git add tests/io/delta_lake/test_delta_stats.py
git commit -m "test(delta): pin stats correctness for every supported column type"
```
