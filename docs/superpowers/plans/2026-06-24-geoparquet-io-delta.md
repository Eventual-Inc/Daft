# Spatial Sub-project B: GeoParquet I/O + Delta Round-trip — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Read and write GeoParquet (geometry columns as WKB + a `"geo"` file-metadata JSON) in Daft, and round-trip geometry through Delta tables via a persisted `daft.geo` table property.

**Architecture:** A Rust geo-metadata module (serde model + `build`/`detect`) backs the GeoParquet `"geo"` JSON for the Parquet read/write paths: the native Parquet writer injects it into the footer kv-metadata for `Geometry` columns; the Parquet reader parses it and re-types the declared Binary columns to `Geometry` after schema inference. The Delta path is pure Python (delta-rs), so it uses a small mirror Python helper (`daft/io/_geoparquet.py`) to build/parse the same `"geo"` JSON, persisting it as the Delta table `configuration` key `daft.geo`; a cross-consistency test keeps the Python and Rust JSON shapes aligned.

**Tech Stack:** Rust (`daft-parquet`, `daft-writers`, `daft-scan` crates; `serde_json`), arrow-rs `parquet` (WriterProperties / footer kv-metadata), Python (`daft/io/_parquet.py`, `daft/io/_geoparquet.py`, `daft/io/delta_lake/`, `daft/dataframe/dataframe.py`), the Python `deltalake` (delta-rs) package, `pytest`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-24-geoparquet-io-delta-design.md`. Every task inherits it.
- GeoParquet **version 1.1.0**; **encoding WKB only** (native GeoArrow encodings out of scope — leave such columns as their raw type).
- `DataType::Geometry` is logical-over-Binary; `to_arrow` → `LargeBinary` ([dtype.rs:346](../../../src/daft-schema/src/dtype.rs)). Re-typing a Binary/LargeBinary column to Geometry must not copy or re-encode data (WKB bytes are already correct); insert a physical cast only if the Binary width differs from Geometry's physical (`LargeBinary`).
- **Read auto-detects** geo metadata by default (no-op for non-geo files); opt-out `read_parquet(..., geometry=False)`.
- **Write auto-emits** `"geo"` metadata when the frame has `Geometry` columns; **native Parquet writer only** (the default). The PyArrow fallback writer does not emit geo metadata (documented, not an error).
- **CRS:** Daft has no CRS. Read ignores `crs`/`bbox`/`edges`/`orientation`. Write omits `crs` by default (GeoParquet treats absent crs as OGC:CRS84/lon-lat); a user-supplied `crs` string is embedded verbatim.
- **Delta:** geometry stored as `LargeBinary` WKB; the `"geo"` JSON persisted in the Delta table `configuration` under key `daft.geo` (an explicit Daft convention).
- **Never fail normal Parquet/Delta I/O over geo metadata** — malformed/missing → ignore + warn.
- Build after Rust changes exercised from Python: `make build`. Rust-only: `cargo build/test -p <crate>`. Python tests: `DAFT_RUNNER=native make test EXTRA_ARGS="-v <path>"`.
- Commit after each task. Conventional Commits titles.

---

## File map

| File | Responsibility | Tasks |
|---|---|---|
| `src/daft-parquet/src/geo_metadata.rs` (new) | `GeoMetadata` serde model; `build`/`detect` (Rust, Parquet paths) | 1 |
| `src/daft-parquet/src/lib.rs` | module declaration | 1 |
| `daft/io/_geoparquet.py` (new) | Python mirror geo-metadata build/detect for the Delta path | 4 |
| `src/daft-writers/src/parquet_writer.rs` | inject `"geo"` kv-metadata when Geometry cols | 2 |
| `src/daft-logical-plan/src/sink_info.rs`, `daft-writers/src/physical.rs`, `daft/dataframe/dataframe.py` | write_parquet `crs`/`geometry_columns` plumbing | 2 |
| `src/daft-parquet/src/metadata.rs` / `metadata_adapter.rs` | surface footer `"geo"` value | 3 |
| `src/daft-parquet/src/{read.rs,schema_inference.rs,reader/mod.rs}`, `daft-scan/src/{file_format_config.rs,glob.rs}`, `daft/io/_parquet.py` | re-type + `geometry` option (scan + direct) | 3 |
| `daft/io/delta_lake/delta_lake_write.py`, `daft/dataframe/dataframe.py` (write_deltalake), `daft/io/delta_lake/_deltalake.py` (read) | Delta persist/parse `daft.geo` | 4 |
| `tests/io/test_geoparquet.py` (new), `tests/io/delta_lake/`, Rust `#[cfg(test)]` | tests + foreign fixture | 1–5 |

---

## Task 1: Shared geo-metadata module (Rust + PyO3)

**Files:**
- Create: `src/daft-parquet/src/geo_metadata.rs`
- Modify: `src/daft-parquet/src/lib.rs` (`pub mod geo_metadata;`)
- Test: `src/daft-parquet/src/geo_metadata.rs` (`#[cfg(test)]`)

**Interfaces:**
- Produces (Rust, for the Parquet read/write paths — no PyO3; the Delta path uses a Python mirror added in Task 4):
  - `pub fn build_geo_metadata(schema: &daft_core::prelude::Schema, crs: Option<&str>, only_columns: Option<&[String]>) -> Option<String>` — returns the `"geo"` JSON, or `None` if no Geometry columns.
  - `pub fn detect_geo_columns(geo_json: &str, schema: &daft_core::prelude::Schema) -> Vec<String>` — geometry column names to re-type (WKB-encoded, present-and-Binary). Lenient: returns `vec![]` on parse failure.
  - `pub const GEO_METADATA_KEY: &str = "geo";`

- [ ] **Step 1: Write failing tests**

In `src/daft-parquet/src/geo_metadata.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use daft_core::prelude::{DataType, Field, Schema};

    fn schema_with_geo() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("geom", DataType::Geometry),
            Field::new("blob", DataType::Binary),
        ])
    }

    #[test]
    fn test_build_emits_conformant_geo_json() {
        let json = build_geo_metadata(&schema_with_geo(), None, None).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["version"], "1.1.0");
        assert_eq!(v["primary_column"], "geom");
        assert_eq!(v["columns"]["geom"]["encoding"], "WKB");
        // only the Geometry column is declared, not the plain Binary one
        assert!(v["columns"].get("blob").is_none());
        assert!(v["columns"].get("id").is_none());
    }

    #[test]
    fn test_build_none_when_no_geometry() {
        let s = Schema::new(vec![Field::new("id", DataType::Int64)]);
        assert!(build_geo_metadata(&s, None, None).is_none());
    }

    #[test]
    fn test_detect_roundtrips_build() {
        let json = build_geo_metadata(&schema_with_geo(), None, None).unwrap();
        // detect runs against a schema where geom arrived as Binary (pre-retype)
        let read_schema = Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("geom", DataType::Binary),
        ]);
        assert_eq!(detect_geo_columns(&json, &read_schema), vec!["geom".to_string()]);
    }

    #[test]
    fn test_detect_lenient_and_wkb_only() {
        let read_schema = Schema::new(vec![Field::new("g", DataType::Binary)]);
        // malformed JSON -> empty
        assert!(detect_geo_columns("{not json", &read_schema).is_empty());
        // non-WKB encoding -> skipped
        let arrow_geo = r#"{"version":"1.1.0","primary_column":"g","columns":{"g":{"encoding":"point"}}}"#;
        assert!(detect_geo_columns(arrow_geo, &read_schema).is_empty());
        // declared column absent from schema -> skipped
        let missing = r#"{"version":"1.1.0","primary_column":"x","columns":{"x":{"encoding":"WKB"}}}"#;
        assert!(detect_geo_columns(missing, &read_schema).is_empty());
    }
}
```

- [ ] **Step 2: Run them, verify they fail**

Run: `cargo test -p daft-parquet --lib geo_metadata`
Expected: FAIL — `build_geo_metadata`/`detect_geo_columns` not found.

- [ ] **Step 3: Implement the model + functions**

In `src/daft-parquet/src/geo_metadata.rs`:
```rust
use std::collections::BTreeMap;
use daft_core::prelude::{DataType, Schema};
use serde::{Deserialize, Serialize};

const GEOPARQUET_VERSION: &str = "1.1.0";
pub const GEO_METADATA_KEY: &str = "geo";

#[derive(Serialize, Deserialize, Debug)]
pub struct GeoMetadata {
    pub version: String,
    pub primary_column: String,
    pub columns: BTreeMap<String, GeoColumn>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GeoColumn {
    pub encoding: String,
    #[serde(default)]
    pub geometry_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crs: Option<serde_json::Value>,
    // Unknown spec fields (bbox/edges/orientation/...) are ignored on parse.
}

/// Build the GeoParquet `"geo"` metadata JSON for the Geometry columns of `schema`.
/// Returns None when there are no Geometry columns to declare.
pub fn build_geo_metadata(schema: &Schema, crs: Option<&str>, only_columns: Option<&[String]>) -> Option<String> {
    let geom_cols: Vec<String> = schema
        .fields()
        .iter()
        .filter(|f| f.dtype == DataType::Geometry)
        .map(|f| f.name.clone())
        .filter(|n| only_columns.map_or(true, |only| only.contains(n)))
        .collect();
    if geom_cols.is_empty() {
        return None;
    }
    let crs_value = crs.map(|c| serde_json::Value::String(c.to_string()));
    let columns = geom_cols
        .iter()
        .map(|name| (name.clone(), GeoColumn { encoding: "WKB".to_string(), geometry_types: vec![], crs: crs_value.clone() }))
        .collect();
    let meta = GeoMetadata { version: GEOPARQUET_VERSION.to_string(), primary_column: geom_cols[0].clone(), columns };
    serde_json::to_string(&meta).ok()
}

/// Parse a `"geo"` JSON and return the names of WKB-encoded geometry columns present as Binary in `schema`.
/// Lenient: returns empty on any parse failure (never panics).
pub fn detect_geo_columns(geo_json: &str, schema: &Schema) -> Vec<String> {
    let Ok(meta) = serde_json::from_str::<GeoMetadata>(geo_json) else {
        return vec![];
    };
    meta.columns
        .into_iter()
        .filter(|(_, c)| c.encoding.eq_ignore_ascii_case("WKB"))
        .map(|(name, _)| name)
        .filter(|name| matches!(schema.get_field(name).map(|f| &f.dtype), Ok(DataType::Binary) | Ok(DataType::LargeBinary) | Ok(DataType::Geometry)))
        .collect()
}
```
(Confirm the `Schema` accessor names against `daft-schema`: `fields()`/iteration, `get_field(name) -> DaftResult<&Field>` or equivalent; adapt to the actual API. `DataType::LargeBinary` exists in Daft's dtype enum — confirm; if Daft has no separate LargeBinary variant, match only `Binary`.)

- [ ] **Step 4: Run tests, verify pass**

Run: `cargo test -p daft-parquet --lib geo_metadata`
Expected: PASS.

- [ ] **Step 5: Register the module**

In `src/daft-parquet/src/lib.rs` add `pub mod geo_metadata;` alongside the other `pub mod` lines. No Python binding — the Parquet read/write tasks call this Rust module directly (Rust→Rust); the Delta path gets a Python mirror in Task 4.

- [ ] **Step 6: Commit**

```bash
git add src/daft-parquet/src/geo_metadata.rs src/daft-parquet/src/lib.rs
git commit -m "feat(geoparquet): shared Rust geo-metadata module (build/detect)"
```

---

## Task 2: Write GeoParquet (emit `"geo"` footer metadata)

**Files:**
- Modify: `src/daft-writers/src/parquet_writer.rs`, `src/daft-logical-plan/src/sink_info.rs`, `src/daft-writers/src/physical.rs`, `daft/dataframe/dataframe.py`
- Test: `tests/io/test_geoparquet.py` (new)

**Interfaces:**
- Consumes: `daft_parquet::geo_metadata::build_geo_metadata` (Task 1).
- Produces: `write_parquet(..., crs: str | None = None, geometry_columns: list[str] | None = None)`; geo metadata auto-emitted when the schema has Geometry columns.

- [ ] **Step 1: Write the failing test**

`tests/io/test_geoparquet.py`:
```python
import json
import daft
import pyarrow.parquet as pq
from daft.functions import st_geomfromtext

def _geo_df():
    return daft.from_pydict({"id": [1, 2], "w": ["POINT(1 2)", "POINT(3 4)"]}).select(
        daft.col("id"), st_geomfromtext(daft.col("w")).alias("geom")
    )

def test_write_parquet_emits_geo_metadata(tmp_path):
    _geo_df().write_parquet(str(tmp_path))
    files = list(tmp_path.rglob("*.parquet"))
    assert files
    meta = pq.read_metadata(files[0]).metadata  # bytes-keyed dict of footer kv-metadata
    geo = json.loads(meta[b"geo"])
    assert geo["version"] == "1.1.0"
    assert geo["primary_column"] == "geom"
    assert geo["columns"]["geom"]["encoding"] == "WKB"
```

- [ ] **Step 2: Run it, verify it fails**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k emits_geo_metadata tests/io/test_geoparquet.py"`
Expected: FAIL — `KeyError: b'geo'` (no geo metadata written).

- [ ] **Step 3: Inject geo metadata in the native writer**

In `src/daft-writers/src/parquet_writer.rs`, thread the Daft schema + crs/geometry_columns into `native_parquet_writer_properties` so it can emit the `"geo"` key. Change the signature to also take `geo_metadata: Option<String>` (computed by the caller) and inject it after the arrow-schema metadata (after line 69):
```rust
fn native_parquet_writer_properties(
    arrow_schema: &arrow_schema::Schema,
    default_compression: Compression,
    column_compression: &[(String, Compression)],
    geo_metadata: Option<&str>,
) -> WriterProperties {
    // ... existing builder ...
    let mut props = builder.build();
    add_encoded_arrow_schema_to_metadata(arrow_schema, &mut props);
    if let Some(geo) = geo_metadata {
        // append a "geo" KeyValue to the footer metadata (alongside ARROW:schema)
        let kv = parquet::format::KeyValue::new("geo".to_string(), geo.to_string());
        let mut existing = props.key_value_metadata().cloned().unwrap_or_default();
        existing.push(kv);
        props.set_key_value_metadata(Some(existing));
    }
    props
}
```
Confirm the arrow-rs `WriterProperties` API for appending kv-metadata: there may be a `WriterPropertiesBuilder::set_key_value_metadata(Vec<KeyValue>)` to set BEFORE `.build()` rather than mutating after. If `add_encoded_arrow_schema_to_metadata` mutates post-build, mirror that mechanism; otherwise set both arrow-schema and geo kv via the builder before `.build()`. Use whichever the installed `parquet` crate supports (the file already mutates post-build at line 69, so a post-build setter exists — match it).

In `create_native_parquet_writer` (line ~96), compute the geo metadata from `schema` before building properties and pass it through:
```rust
let geo_metadata = daft_parquet::geo_metadata::build_geo_metadata(schema.as_ref(), crs, geometry_columns);
let writer_properties = native_parquet_writer_properties(&arrow_schema, default_compression, &parsed_column_compression, geo_metadata.as_deref());
```
Add `crs: Option<&str>` and `geometry_columns: Option<&[String]>` params to `create_native_parquet_writer` (and update `native_parquet_writer_supported`'s call to pass `None` for geo — it only checks convertibility). (`daft-writers` must depend on `daft-parquet`; if that dependency edge doesn't exist or would cause a cycle, move `geo_metadata.rs` to a lower crate such as `daft-schema` or a small `daft-geo-meta` crate that both depend on — verify the dependency graph and place the module accordingly. Adjust Task 1's location if so.)

- [ ] **Step 4: Plumb crs/geometry_columns through the format option**

In `src/daft-logical-plan/src/sink_info.rs`, extend `ParquetFormatOption` (line ~364) with `pub crs: Option<String>` and `pub geometry_columns: Option<Vec<String>>`, and the `PyFormatSinkOption.parquet()` classmethod to accept them. In `src/daft-writers/src/physical.rs` `create_native_writer` (line ~190), extract them and pass to `create_native_parquet_writer`. In `daft/dataframe/dataframe.py` `write_parquet` (line ~1057), add `crs: str | None = None, geometry_columns: list[str] | None = None` params and pass into the parquet format option.

- [ ] **Step 5: Run the test, verify pass**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k emits_geo_metadata tests/io/test_geoparquet.py"`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/daft-writers/ src/daft-logical-plan/src/sink_info.rs daft/dataframe/dataframe.py tests/io/test_geoparquet.py
git commit -m "feat(geoparquet): emit geo footer metadata from the native parquet writer"
```

---

## Task 3: Read GeoParquet (re-type Binary → Geometry)

**Files:**
- Modify: `src/daft-parquet/src/metadata.rs` / `metadata_adapter.rs` (surface `"geo"`), `src/daft-parquet/src/read.rs` + `schema_inference.rs` + `reader/mod.rs`, `src/daft-scan/src/file_format_config.rs` + `glob.rs`, `daft/io/_parquet.py`
- Test: `tests/io/test_geoparquet.py`

**Interfaces:**
- Consumes: `daft_parquet::geo_metadata::detect_geo_columns` (Task 1); `build_geo_metadata` via Task 2's writer (for the round-trip test).
- Produces: `read_parquet(..., geometry: bool = True)`; auto re-types WKB columns to `Geometry`.

- [ ] **Step 1: Write the failing round-trip test**

Add to `tests/io/test_geoparquet.py`:
```python
from daft.functions import st_astext

def test_geoparquet_roundtrip(tmp_path):
    _geo_df().write_parquet(str(tmp_path))
    df = daft.read_parquet(str(tmp_path))
    assert df.schema()["geom"].dtype == daft.DataType.geometry()
    out = df.select(st_astext(daft.col("geom")).alias("wkt")).sort("wkt").to_pydict()
    assert [w.upper().startswith("POINT") for w in out["wkt"]] == [True, True]

def test_geometry_false_keeps_binary(tmp_path):
    _geo_df().write_parquet(str(tmp_path))
    df = daft.read_parquet(str(tmp_path), geometry=False)
    assert df.schema()["geom"].dtype == daft.DataType.binary()
```

- [ ] **Step 2: Run, verify fail**

Run: `DAFT_RUNNER=native make test EXTRA_ARGS="-v -k 'roundtrip or keeps_binary' tests/io/test_geoparquet.py"`
Expected: FAIL — geom comes back as Binary (no re-typing); `geometry=` kwarg unknown.

- [ ] **Step 3: Surface the `"geo"` footer value**

In `src/daft-parquet/src/metadata.rs` / `metadata_adapter.rs`, add a way to read the `"geo"` kv-metadata value (it is currently parsed but only `ARROW:schema` is used — see `rebuild_file_metadata` line 249). Add a helper on the metadata adapter, e.g. `pub fn geo_metadata(&self) -> Option<String>` returning the `"geo"` KeyValue's value from `self.as_arrowrs().file_metadata().key_value_metadata()`.

- [ ] **Step 4: Add the `geometry` option + re-type step**

Add `geometry: bool` (default true) to `ParquetSchemaInferenceOptions` ([read.rs:47](../../../src/daft-parquet/src/read.rs)) and `ParquetSourceConfig` ([file_format_config.rs:76](../../../src/daft-scan/src/file_format_config.rs)). After the Daft schema is built from Arrow (in `infer_schema_from_daft_metadata` / `reader/mod.rs:154` `prepare_metadata`), if `geometry` is true and `geo_metadata()` is present, compute `detect_geo_columns(geo_json, &daft_schema)` and replace those fields' dtype with `DataType::Geometry`, producing the final schema. Apply in BOTH the scan schema-inference path ([daft-scan/src/glob.rs](../../../src/daft-scan/src/glob.rs)) and the data-read path (`prepare_metadata`) so the schema and the materialized Series agree. (`Series::from_arrow` builds a `GeometryArray` when the Daft field says Geometry over a Binary/LargeBinary physical — confirm via a focused check; if it requires the physical to be `LargeBinary` specifically, cast the Binary physical first.)

- [ ] **Step 5: Plumb the Python option**

In `daft/io/_parquet.py` `read_parquet` (line ~24), add `geometry: bool = True` and thread it into `ParquetSourceConfig`.

- [ ] **Step 6: Run tests, verify pass**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k 'roundtrip or keeps_binary' tests/io/test_geoparquet.py"`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/daft-parquet/ src/daft-scan/ daft/io/_parquet.py tests/io/test_geoparquet.py
git commit -m "feat(geoparquet): auto-detect geo metadata on read; re-type WKB columns to Geometry"
```

---

## Task 4: GeoParquet→Delta round-trip

**Files:**
- Create: `daft/io/_geoparquet.py` (Python mirror geo helper)
- Modify: `daft/dataframe/dataframe.py` (`write_deltalake`), `daft/io/delta_lake/delta_lake_write.py`, `daft/io/delta_lake/_deltalake.py` (read path)
- Test: `tests/io/delta_lake/test_geo_deltalake.py` (new) or extend `tests/io/test_geoparquet.py`

**Interfaces:**
- Produces: `daft/io/_geoparquet.py` with `build_geo_metadata(schema: Schema, crs: str | None = None, only_columns: list[str] | None = None) -> str | None` and `detect_geo_columns(geo_json: str, schema: Schema) -> list[str]` — pure-Python mirror of the Rust module (same `"geo"` JSON shape, version 1.1.0, WKB-only, lenient parse).
- Produces: geometry columns persisted to Delta as `LargeBinary` + `daft.geo` table property; `read_deltalake` re-types from it.

- [ ] **Step 1: Write the failing test**

```python
import daft
from daft.functions import st_geomfromtext, st_astext

def test_deltalake_geo_roundtrip(tmp_path):
    df = daft.from_pydict({"id": [1], "w": ["POINT(5 6)"]}).select(
        daft.col("id"), st_geomfromtext(daft.col("w")).alias("geom")
    )
    df.write_deltalake(str(tmp_path))
    back = daft.read_deltalake(str(tmp_path))
    assert back.schema()["geom"].dtype == daft.DataType.geometry()
    out = back.select(st_astext(daft.col("geom")).alias("wkt")).to_pydict()
    assert out["wkt"][0].upper().startswith("POINT")
```

- [ ] **Step 2: Run, verify fail**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k deltalake_geo_roundtrip tests/io/"`
Expected: FAIL — geom returns as Binary (no `daft.geo`, no re-type).

- [ ] **Step 3: Create the Python geo helper**

Create `daft/io/_geoparquet.py` mirroring the Rust module's shape (GeoParquet 1.1.0, WKB-only, lenient):
```python
"""Geo metadata helpers for the GeoParquet/Delta path (Python mirror of daft-parquet's geo_metadata)."""
from __future__ import annotations
import json
from daft.schema import Schema
from daft.datatype import DataType

_GEOPARQUET_VERSION = "1.1.0"
GEO_METADATA_KEY = "geo"  # parquet footer key
GEO_DELTA_PROPERTY = "daft.geo"  # delta table-configuration key

def build_geo_metadata(schema: Schema, crs: str | None = None, only_columns: list[str] | None = None) -> str | None:
    geom = DataType.geometry()
    cols = [f.name for f in schema if f.dtype == geom and (only_columns is None or f.name in only_columns)]
    if not cols:
        return None
    col_meta = {"encoding": "WKB", "geometry_types": []}
    if crs is not None:
        col_meta["crs"] = crs
    return json.dumps({
        "version": _GEOPARQUET_VERSION,
        "primary_column": cols[0],
        "columns": {name: dict(col_meta) for name in cols},
    })

def detect_geo_columns(geo_json: str, schema: Schema) -> list[str]:
    try:
        meta = json.loads(geo_json)
        columns = meta["columns"]
    except (ValueError, KeyError, TypeError):
        return []
    binary_like = {DataType.binary(), DataType.geometry()}
    names = {f.name: f.dtype for f in schema}
    return [
        name for name, c in columns.items()
        if isinstance(c, dict) and str(c.get("encoding", "")).upper() == "WKB"
        and names.get(name) in binary_like
    ]
```
(Confirm `Schema` is iterable yielding fields with `.name`/`.dtype`, and `DataType.geometry()`/`.binary()` equality — both used in Task 3's tests. Adjust the iteration/accessors to the actual `daft.schema.Schema` API.)

- [ ] **Step 4: Persist `daft.geo` on write**

In the Delta write path (`daft/dataframe/dataframe.py` `write_deltalake` ~line 1782, where `configuration`/`custom_metadata` are assembled, and/or `daft/io/delta_lake/delta_lake_write.py`), before the write, build the geo JSON from the Daft schema and add it to the table configuration:
```python
from daft.io._geoparquet import build_geo_metadata, GEO_DELTA_PROPERTY
geo = build_geo_metadata(self.schema())
if geo is not None:
    configuration = {**(configuration or {}), GEO_DELTA_PROPERTY: geo}
```
Pass this `configuration` into the existing table-create/commit call (`create_table_with_add_actions(..., configuration=...)`). Geometry already writes as `LargeBinary` (no data change). Confirm delta-rs persists `configuration` as table properties on create AND that overwrite refreshes it; for append to an existing table, the property persists from creation (set it only at create/overwrite if append-time update is unsupported — see the spec risk).

- [ ] **Step 5: Re-type on read**

In the Delta read path (`daft/io/delta_lake/_deltalake.py`, where the Daft schema is constructed from the Delta table), read the table `configuration` (delta-rs `DeltaTable.metadata().configuration`), and if `GEO_DELTA_PROPERTY` (`daft.geo`) is present, call `detect_geo_columns(geo_json, schema)` (from `daft.io._geoparquet`) and cast those columns to `DataType.geometry()` in the resulting plan (apply a projection `col(name).cast(DataType.geometry())`, or set the scan schema's field types to Geometry so the binary WKB is wrapped). Use whichever mechanism matches how the Delta scan builds its schema; the cast from Binary(WKB)→Geometry must be zero-copy (logical re-wrap).

- [ ] **Step 6: Run test, verify pass**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k deltalake_geo_roundtrip tests/io/"`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add daft/io/_geoparquet.py daft/dataframe/dataframe.py daft/io/delta_lake/ tests/io/
git commit -m "feat(geoparquet): round-trip geometry through Delta via daft.geo table property"
```

---

## Task 5: Foreign-file fixture, robustness, docs, full suite

**Files:**
- Modify: `tests/io/test_geoparquet.py`; any spatial/IO docs under `docs/`
- Test: same

- [ ] **Step 1: Foreign-file read test (not written by Daft)**

Build a minimal valid GeoParquet with pyarrow + hand-authored `"geo"` metadata, then read it via Daft:
```python
import json, pyarrow as pa, pyarrow.parquet as pq, daft

def test_read_foreign_geoparquet(tmp_path):
    # WKB for POINT(1 2), little-endian: 01 01000000 + 8-byte x=1.0 + 8-byte y=2.0
    wkb = bytes.fromhex("0101000000000000000000F03F0000000000000040")
    tbl = pa.table({"geom": pa.array([wkb], type=pa.binary())})
    geo = {"version": "1.1.0", "primary_column": "geom",
           "columns": {"geom": {"encoding": "WKB", "geometry_types": []}}}
    tbl = tbl.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    path = tmp_path / "foreign.parquet"
    pq.write_table(tbl, path)
    df = daft.read_parquet(str(path))
    assert df.schema()["geom"].dtype == daft.DataType.geometry()

def test_plain_parquet_unaffected(tmp_path):
    daft.from_pydict({"a": [1, 2]}).write_parquet(str(tmp_path))
    df = daft.read_parquet(str(tmp_path))
    assert df.schema()["a"].dtype == daft.DataType.int64()

def test_python_helper_matches_rust_geo_json(tmp_path):
    # The Python Delta helper must emit the SAME "geo" JSON the Rust parquet writer emits,
    # so a GeoParquet→Delta→read round-trip stays consistent.
    import json, pyarrow.parquet as pq
    from daft.io._geoparquet import build_geo_metadata
    from daft.functions import st_geomfromtext
    df = daft.from_pydict({"id": [1], "w": ["POINT(1 2)"]}).select(
        daft.col("id"), st_geomfromtext(daft.col("w")).alias("geom")
    )
    df.write_parquet(str(tmp_path))
    rust_geo = json.loads(pq.read_metadata(next(tmp_path.rglob("*.parquet"))).metadata[b"geo"])
    py_geo = json.loads(build_geo_metadata(df.schema()))
    assert py_geo == rust_geo  # version, primary_column, columns{geom:{encoding:WKB,...}}
```
(Confirm pyarrow's `replace_schema_metadata` puts the `"geo"` key in the Parquet footer kv-metadata that Daft reads; if pyarrow nests it under the arrow schema instead of the file kv-metadata, write the kv via `pq.write_table(..., store_schema=...)` or set it through `ParquetWriter` metadata so it lands in the file-level `key_value_metadata`. The goal: the `"geo"` key is in the footer kv-metadata Daft reads at `metadata.rs:249`.)

- [ ] **Step 2: Run the foreign-file + plain tests**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v -k 'foreign or unaffected' tests/io/test_geoparquet.py"`
Expected: PASS. If the foreign fixture's geo key doesn't reach Daft's footer reader, fix the fixture-authoring approach (Step 1 note) until Daft detects it — this test is the real proof of external GeoParquet compatibility.

- [ ] **Step 3: Full suite**

Run: `make build && DAFT_RUNNER=native make test EXTRA_ARGS="-v tests/io/test_geoparquet.py tests/io/delta_lake/"`
Expected: all PASS, pristine output.

- [ ] **Step 4: Docs**

Add a short GeoParquet section to the relevant IO docs (find the existing parquet/connectors docs under `docs/`): `read_parquet` auto-detects GeoParquet and yields Geometry columns (`geometry=False` to disable); `write_parquet` emits GeoParquet 1.1 metadata for Geometry columns (`crs=` optional); the Delta round-trip via the `daft.geo` table property (a Daft convention); WKB-only, no CRS transforms.

- [ ] **Step 5: Commit**

```bash
git add tests/io/ docs/
git commit -m "test(geoparquet): foreign-file read fixture + docs; full suite"
```

---

## Self-review notes

- **Spec coverage:** shared Rust module → Task 1; Python mirror helper → Task 4; read auto-detect + `geometry` option + re-type → Task 3; write auto-emit + crs/geometry_columns → Task 2; Delta round-trip (`daft.geo` persist + read re-type) → Task 4; foreign-file robustness + WKB-only + plain-parquet-unaffected + Python↔Rust JSON consistency + docs → Task 5; error handling (lenient detect) → Task 1/Task 4 (`detect` returns empty on bad input) + Task 3/4 (apply only when present). All spec items mapped.
- **No PyO3:** per the decision to drop the binding, the Rust module serves the Parquet paths (Rust→Rust) and a pure-Python mirror (`daft/io/_geoparquet.py`) serves the Delta path; the Task 5 consistency test (`test_python_helper_matches_rust_geo_json`) guards against the two JSON shapes drifting.
- **Flagged confirmation points (external/unfamiliar APIs, not placeholders for our logic):** (a) the arrow-rs `WriterProperties` kv-metadata append mechanism (Task 2 Step 3 — mirror the existing post-build `add_encoded_arrow_schema_to_metadata`); (b) crate dependency edge `daft-writers → daft-parquet` for the geo module, with a documented fallback to relocate the module if it cycles (Task 2 Step 3); (c) `Series::from_arrow` wrapping Binary→Geometry without copy, else cast (Tasks 3/4); (d) delta-rs `configuration` persistence on create/overwrite/append (Task 4 Step 3, matches a spec risk); (e) pyarrow footer-metadata placement for the foreign fixture (Task 5 Step 1). Each names the concrete thing to verify and a fallback.
- **Type/name consistency:** `build_geo_metadata`/`detect_geo_columns` (+ `_py` bindings), `GEO_METADATA_KEY = "geo"`, the `daft.geo` Delta property key, and the `geometry`/`crs`/`geometry_columns` option names are used consistently across tasks.
- **DRY:** the Rust module backs Parquet read+write; a small Python mirror backs both Delta directions; the JSON shape is identical and pinned by the Task 5 consistency test — the only intentional duplication, kept honest by that test.
