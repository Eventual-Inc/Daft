# Spatial Sub-project B: GeoParquet I/O + Delta Round-trip

- **Date:** 2026-06-24
- **Status:** Approved design, ready for implementation planning
- **Scope:** `src/daft-parquet`, `src/daft-writers`, `src/daft-scan`, `daft/io/_parquet.py`, `daft/io/delta_lake/`, `daft/dataframe/dataframe.py`, a shared geo-metadata module (Rust + PyO3 binding), `tests/`
- **Part of:** the 3-sub-project geospatial effort. Sub-project A (geometry functions & engine) is complete. This is **Sub-project B**. Sub-project C (R-tree + native spatial join) follows.

## Summary

Daft can store geometry as `DataType::Geometry` (logical-over-Binary, WKB) and operate on it (Sub-project A), but it cannot **read or write GeoParquet** — the standard interchange format where geometry columns are WKB Binary and a file-level `"geo"` JSON metadata key declares them. Today a GeoParquet file reads as a plain Binary column (the `"geo"` metadata is dropped), and writing a Geometry column produces Parquet with no `"geo"` metadata.

This sub-project adds: (1) **read** GeoParquet (parse `"geo"` metadata → re-type declared Binary columns to `Geometry`), (2) **write** GeoParquet (emit `"geo"` metadata for Geometry columns), and (3) a **GeoParquet→Delta round-trip** (Delta write persists the `"geo"` metadata as a table property; Delta read re-types from it). All three share one geo-metadata model and the existing `Geometry` (WKB) type.

## Goals

- `daft.read_parquet` auto-detects GeoParquet `"geo"` metadata and produces `Geometry` columns (a no-op for non-geo Parquet); opt-out via `geometry=False`.
- `df.write_parquet` emits spec-conformant GeoParquet 1.1.0 `"geo"` metadata whenever the frame has `Geometry` columns (native writer).
- Read GeoParquet written by other tools (GeoPandas/pyarrow): robust to the full `"geo"` schema, using only the fields Daft needs.
- Round-trip geometry through Delta: `write_deltalake` persists `"geo"` info in the Delta table `configuration` (`daft.geo`); `read_deltalake` re-types geometry columns from it.
- Never fail normal Parquet/Delta I/O over geo metadata — degrade + warn.
- One geo-metadata code path shared by Parquet (Rust) and Delta (Python via PyO3).

## Non-goals

- **CRS / coordinate transforms.** Daft's `Geometry` carries no CRS. Read ignores the `crs` field; write emits a default/user-supplied `crs` string but does not transform coordinates. (Consistent with Sub-project A non-goals.)
- **GeoParquet native GeoArrow encodings** (1.1's point/linestring nested-array encodings). Only `encoding: "WKB"` columns are detected/produced; non-WKB-encoded geometry columns are left as their raw nested type (documented).
- **`bbox` and per-column `geometry_types` population.** Emitted as omitted / `[]` ("unknown/any" per spec) to avoid a full-column scan. `bbox`-based row-group pruning is out of scope (belongs with Sub-project C / a later optimization).
- **PyArrow fallback Parquet writer** geo-metadata emission. Geo metadata is emitted on the native writer (the default); the PyArrow path writes geometry as plain binary without `"geo"` metadata (documented).
- **A standard geometry-in-Delta format.** None exists; the `daft.geo` table property is an explicit Daft convention.

## Decisions (resolved during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Delta semantics | **Round-trip via table metadata** | Persist the `"geo"` JSON in the Delta table `configuration` so geometry re-types on read-back — a clean GeoParquet→Delta→Geometry round-trip. |
| Read API | **Auto-detect in `read_parquet`** (opt-out `geometry=False`) | GeoParquet *is* Parquet; auto-detect is a no-op for non-geo files. No separate `read_geoparquet`. |
| Write API | **Auto-emit** when Geometry columns present (via `write_parquet`) | Ergonomic; the schema already says which columns are geometry. |
| CRS | Read ignores; write defaults **OGC:CRS84**, user-overridable | Daft has no CRS concept; CRS84 is GeoParquet's lon/lat default. |
| Geo-metadata model | **One Rust module + PyO3 binding** | Parquet (Rust) and Delta (Python) reuse identical build/detect logic. |
| GeoParquet version | **1.1.0** | Current stable spec. |
| Encoding | **WKB only** | Daft's `Geometry` is WKB; native GeoArrow encodings out of scope. |

## Current state (baseline, from exploration)

- **Read:** Parquet footer kv-metadata is parsed but only `ARROW:schema` is kept ([metadata.rs:249](../../../src/daft-parquet/src/metadata.rs)); schema built at [reader/mod.rs:154](../../../src/daft-parquet/src/reader/mod.rs) / [schema_inference.rs](../../../src/daft-parquet/src/schema_inference.rs). No geo handling exists.
- **Write:** native writer ([parquet_writer.rs](../../../src/daft-writers/src/parquet_writer.rs)) already injects `ARROW:schema` kv-metadata via `add_encoded_arrow_schema_to_metadata` and has a `TODO: shove useful metadata before closing` hook. Geometry serializes as Binary/WKB automatically.
- **Delta:** write goes through the Python `deltalake` (delta-rs) package ([delta_lake_write.py](../../../daft/io/delta_lake/)); `Geometry` → `LargeBinary` survives (data preserved) but Arrow field metadata is dropped (`cast()` in `sanitize_table_for_deltalake`).
- `DataType::Geometry` is logical-over-Binary, `to_arrow` → `LargeBinary` ([dtype.rs:346](../../../src/daft-schema/src/dtype.rs)).

## Architecture

### Shared geo-metadata module (single source of truth)

A serde struct modeling the GeoParquet `"geo"` JSON (in `daft-parquet` or a small shared crate/module):
- `version: String`, `primary_column: String`, `columns: HashMap<String, GeoColumn>` where `GeoColumn { encoding: String, geometry_types: Vec<String>, crs: Option<serde_json::Value>, bbox: Option<Vec<f64>>, .. }` (extra spec fields tolerated/ignored on parse).
- **`build(schema, crs, geometry_columns) -> String`**: produce the `"geo"` JSON for a Daft schema's `Geometry` columns (version 1.1.0, primary = first, encoding WKB, `geometry_types: []`, crs default OGC:CRS84 or supplied).
- **`detect(geo_json, schema) -> Vec<String>`**: parse `"geo"` JSON, return the names of `encoding == "WKB"` columns present-and-Binary in `schema` (robust to malformed input → empty + warn).
- A thin **PyO3 binding** exposes `build`/`detect` so the Python Delta path reuses identical logic.

### Component 1 — Read GeoParquet

- Expose the footer `"geo"` kv-metadata from the Parquet metadata adapter (currently only `ARROW:schema` is surfaced).
- After Arrow→Daft schema inference, a **re-type step** sets each `detect()`-identified column's Daft type from Binary/LargeBinary to `Geometry`. Physical data is unchanged (already WKB); `Series::from_arrow` wraps the Binary physical as `GeometryArray` because the Daft field says Geometry.
- Applied on **both** the scan path (glob schema inference, [daft-scan/src/glob.rs](../../../src/daft-scan/src/glob.rs)) and the direct read path so schema and data agree.
- Plumbed via a `geometry: bool = True` option on `read_parquet` → `ParquetSourceConfig`/`ParquetSchemaInferenceOptions`. `False` skips re-typing.
- Foreign-file robust: parse the full `"geo"` schema, use only `version`/`primary_column`/`columns[].encoding`; ignore `crs`/`bbox`/`edges`/`orientation`; non-WKB encodings left as-is.

### Component 2 — Write GeoParquet

- In the native writer's properties builder (the `TODO` hook / `native_parquet_writer_properties`), if the schema has `Geometry` columns, call `build()` and inject the result as the `"geo"` footer kv-metadata alongside `ARROW:schema`.
- Geometry already serializes as Binary/WKB — no data-path change.
- Options on `write_parquet`: `crs: str | None` (default OGC:CRS84) and optional `geometry_columns: list[str] | None` (default = all `Geometry`-typed), plumbed via `ParquetFormatOption` → `physical.rs` → `create_native_parquet_writer`.
- Native writer only (default); PyArrow fallback writes geometry as plain binary (documented).

### Component 3 — GeoParquet→Delta round-trip

- **Write** ([delta_lake_write.py](../../../daft/io/delta_lake/) / `df.write_deltalake`): call the PyO3 `build()` on the Daft schema; persist the JSON in the Delta table `configuration` under key `daft.geo` (persistent table property), set on create/overwrite and preserved across appends. Geometry writes as `LargeBinary` WKB (already happens).
- **Read** (Daft Delta read): after schema construction, read the table `configuration`; if `daft.geo` present, re-type its declared Binary columns to `Geometry` via `detect()`.
- An explicit Daft convention (no geometry-in-Delta standard), documented.

## Error handling

- Read (Parquet/Delta): malformed/missing geo metadata → ignore + warn; a declared column absent or not Binary → skip that column + warn. Normal reads never fail over geo metadata.
- Write (Parquet): only the native writer emits `"geo"`; PyArrow fallback silently writes plain binary (documented).
- Delta: malformed `daft.geo` on read → ignore + warn.

## Testing

- **Rust unit:** the geo-metadata `build`/`detect` module — build a schema with Geometry columns → conformant JSON; detect with foreign/malformed inputs (missing keys, non-WKB encoding, absent column) → correct/empty.
- **Parquet round-trip (Python, native runner):** write a Geometry frame → assert the footer `"geo"` JSON is GeoParquet-1.1-conformant (`version`, `primary_column`, `columns[].encoding == "WKB"`) → read back → Geometry column with byte-identical WKB.
- **`geometry=False`** → column stays Binary.
- **Foreign-file read:** a minimal valid GeoParquet fixture built via pyarrow + hand-authored `"geo"` metadata → geometry detected; a plain non-geo Parquet → unaffected.
- **Delta round-trip:** `write_deltalake` a Geometry frame → table `configuration` contains `daft.geo` → `read_deltalake` → Geometry column; covered for append and overwrite modes.
- All Python tests native-runner; geometry data verified via Sub-project A functions (e.g. `st_astext`/`st_x`).

## Risks / open questions

- **Re-type wrapping:** confirm that constructing a Daft `Geometry` field over an Arrow Binary/LargeBinary array via `Series::from_arrow` yields a valid `GeometryArray` without a data copy/cast; if a column is `Binary` vs `LargeBinary` mismatched to Geometry's physical (`LargeBinary`), insert a cast.
- **Delta `configuration` update semantics:** confirm delta-rs allows setting/preserving a table property on create and overwrite (and that appends don't clobber it); if table-property update on append is unsupported, set it at create/overwrite only and document.
- **Native-writer dependence:** geo metadata requires `native_parquet_writer=true` (default); if the project intends GeoParquet to work under the PyArrow writer too, that's additional scope.
- **Fixture authoring:** the foreign-file test needs a hand-built GeoParquet; ensure the hand-authored `"geo"` JSON matches the spec so the test is meaningful.
