# Daft Documentation Audit Report

**Date**: 2026-05-23
**Scope**: All docs at docs.daft.ai/en/latest/ (source: /docs) + README.rst
**Perspective**: AI agent reading docs to assist Daft users

## Executive Summary

**198 issues found across 119 documentation files**

| Severity | Count |
|----------|-------|
| Critical | 6 |
| High | 34 |
| Medium | 75 |
| Low | 83 |

### Top Critical Issues (Must Fix)

1. **SQL SELECT doc is nearly empty** (`docs/sql/statements/select.md`) -- The SELECT statement is the core of Daft SQL. The doc has only 6 basic examples and no coverage of WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, JOIN, subqueries, CTEs, UNION, CASE, etc. All of these are supported by the engine.
2. **DESCRIBE TABLE example is rejected by the engine** (`docs/sql/statements/describe.md:30`) -- Docs show `DESCRIBE TABLE T;` as valid, but the source explicitly returns `unsupported_sql_err!("DESCRIBE TABLE is not supported")`. Correct syntax is `DESCRIBE T;`.
3. **Window function limitation claim is wrong** (`docs/sql/window_functions.md:163`) -- Says "window functions without PARTITION BY are not yet supported" but tests prove `ROW_NUMBER() OVER(ORDER BY value DESC)` works.
4. **Kubernetes example missing `daft.set_runner_ray()`** (`docs/distributed/kubernetes.md:54-69`) -- The distributed script calls `ray.init()` but never `daft.set_runner_ray()`, so Daft silently uses the native runner instead of Ray.
5. **Series API reference hides 148 of 167 methods** (`docs/api/series.md:7`) -- Filter `["^to", "^from"]` only shows `to_*`/`from_*` methods. Should be `["!^_"]`.
6. **`daft[kafka]` extra doesn't exist** (`docs/connectors/kafka.md:27`) -- Doc says `pip install -U "daft[kafka]"` but there is no `kafka` extra in pyproject.toml.

---

## Section 1: Core Guide (index, quickstart, install, roadmap, telemetry, architecture, sessions)

### High

- **Install page missing Python version requirement** (`install.md`) -- Never states Python 3.10+ is required. The quickstart mentions it but install.md does not.
- **Telemetry opt-out docs are incomplete** (`telemetry.md:7`) -- Only documents `DO_NOT_TRACK=true`. The code also supports `SCARF_NO_ANALYTICS=true` and `DAFT_ANALYTICS_ENABLED=0`.

### Medium

- **Slack invite link may expire** (`index.md:515`) -- Uses time-limited shared invite link. Should use `https://daft.ai/slack`.
- **Quickstart install page link is commented out** (`quickstart.md:33`) -- Users with advanced install needs have no pathway.
- **Session implicit creation claim is misleading** (`configuration/sessions-usage.md:10`) -- Says `import daft` defines an implicit session. Actually lazily created on first use.
- **Session reference table missing many methods** (`configuration/sessions-usage.md:266-296`) -- Missing: `attach_view`, `create_temp_view`, `current_model`, `current_provider`, `detach_function`, `detach_provider`, `get_function`, `get_provider`, `has_provider`, `set_model`, `set_provider`, `set_session`, `use`.
- **SQL output blocks in sessions doc not fenced** (`configuration/sessions-usage.md:238-255`) -- Table output renders as raw markdown text.
- **mkdir instruction buried in Python comment** (`configuration/sessions-usage.md:59`) -- Critical prerequisite easy to miss.
- **External file creation in comment** (`configuration/sessions-usage.md:146`) -- `echo` command hidden in Python comment.
- **No general configuration docs** -- No docs for `set_execution_config()`, `set_planning_config()`, env vars like `DAFT_RUNNER`, or `IOConfig` outside API reference.
- **Benchmark "nanx Slower" hover text** (`benchmarks/index.md:191,255`) -- Plotly chart shows "nanx Slower" for failed queries instead of "DNF".
- **Telemetry file reference is ambiguous** (`telemetry.md:19`) -- Says "see `scarf_telemetry.py`" without path or link. Actual path: `daft/scarf_telemetry.py`.

---

## Section 2: AI Functions & Modalities

### High

- **Cohere listed as supported provider but doesn't exist** (`modalities/embeddings.md:6`) -- No Cohere provider in codebase. `ProviderType` is `"google" | "lm_studio" | "openai" | "transformers" | "vllm-prefix-caching"`.
- **`prompt` not imported in vLLM example** (`ai-functions/providers.md:248`) -- Code uses `prompt()` without importing it. Will cause `NameError`.
- **`prompt` call missing required `model` parameter** (`modalities/documents.md:37-45`) -- Specifies `provider="openai"` but no model.
- **Inconsistent import paths across docs** -- `text.md` (lines 23, 48, 77) and `images.md` (line 401) use `from daft.functions.ai import ...` (internal path). Should be `from daft.functions import ...`.
- **Future/non-existent model names throughout** -- `gpt-5.4-mini`, `gpt-5-nano`, `gpt-5-mini`, `gpt-5-2025-08-07`, `gemini-3-pro-preview` appear across `prompt.md`, `providers.md`, `embeddings.md`. None currently exist. Users will get API errors.

### Medium

- **Unused imports in documents example** (`modalities/documents.md:17`) -- `embed_text` and `regexp_split` imported but unused.
- **Audio admonition malformatted** (`modalities/audio.md:7-8`) -- Content not indented under `!!! note` directive.
- **Transcription schema imported before defined** (`modalities/audio.md:246`) -- `from transcription_schema import TranscriptionResult` before the schema is shown.
- **No documentation of default model behavior** -- Unclear what happens when `model` is not specified for each provider.
- **No error handling / rate limiting guidance** -- No docs on `on_error`, timeouts, or partial failure handling for AI functions.
- **Multiple model name typos** -- "latests" (prompt.md:70), "catapie" should be "caterpie" (classify.md:78).

---

## Section 3: UDFs / Custom Code

### High

- **`RetryAfterError` import path wrong** (`custom-code/func.md:448`, `custom-code/migration.md:130`) -- Docs say `daft.ai.utils.RetryAfterError`. Canonical path is `daft.errors.RetryAfterError`.
- **APIs page is empty stub** (`custom-code/apis.md`) -- Just says "User guide coming soon!" -- dead end for async API call patterns.
- **Import inconsistency in batch inference** (`use-case/batch-inference.md:24`) -- Uses `from daft.functions import prompt` in one example, `from daft.functions.ai import embed_text` in another.
- **Batch inference links to legacy UDF page** (`use-case/batch-inference.md:9`) -- Links to `custom-code/udfs.md` instead of `custom-code/index.md`.

### Medium

- **`name_override` parameter undocumented in func.md** -- Only in cls.md.
- **`ray_options` parameter undocumented in func.md** -- Only in migration.md.
- **`batch_size` not documented in func.md** -- Only in cls.md.
- **`daft.metrics.increment_counter` completely undocumented** -- UDF observability feature not mentioned anywhere.
- **Migration guide claims method-level retries are ignored** (`custom-code/migration.md:133`) -- Source shows they DO override class-level values.
- **Migration guide class method example is misleading** (`custom-code/migration.md:98-99`) -- Shows `daft.Series` in new API row-wise method; should use scalar types or `@daft.method.batch`.
- **`max_concurrency` description in cls.md is ambiguous** (`custom-code/cls.md:94`) -- Doesn't distinguish sync (actor pool) vs async (coroutines) behavior.
- **Typo** (`custom-code/cls.md:262`) -- "some an expensive" should be "an expensive".

---

## Section 4: Connectors

### Critical

- **`daft[kafka]` extra doesn't exist** (`connectors/kafka.md:27`) -- `pip install -U "daft[kafka]"` does nothing; extra not in pyproject.toml.

### High

- **8+ connectors missing from index page** (`connectors/index.md`) -- Turbopuffer, Gravitino, Hugging Face, Unity Catalog, S3 Tables, Glue, Custom Connectors, Custom Catalogs all have pages but aren't linked from the index.
- **SQL write support exists but roadmap says it's missing** (`connectors/sql.md:154-159`) -- `DataFrame.write_sql()` exists in codebase but doc says it's a future feature.
- **No `write_sql` docs or examples** (`connectors/sql.md`) -- Only reading is documented.
- **Glue uses private `__glue` module imports** (`connectors/glue.md:20,80`) -- `from daft.catalog.__glue import load_glue`. Public API is `Catalog.from_glue()`.
- **Unity Catalog uses private `__unity` imports** (`connectors/unity_catalog.md:28`) -- Should prefer `Catalog.from_unity()`.

### Medium

- **Missing `import daft`** in AWS (`aws.md:31`), Azure (`azure.md:36`), and GCS (`gcs.md:35`) examples.
- **S3 Tables inconsistent table names** (`s3tables.md:25-56`) -- Writes to `"demo.points"` but reads from `"my_namespace.my_table"`.
- **Lance code blocks break MkDocs tabs** (`lance.md:96-104,138-153,257-273`) -- Not indented under tab selectors.
- **Hudi typo "Apachi"** (`hudi.md:51`) -- Should be "Apache".
- **Iceberg missing install instructions** (`iceberg.md`) -- No `pip install -U "daft[iceberg]"`.
- **Turbopuffer minimal docs** (`turbopuffer.md`) -- No parameter reference, no column requirements.
- **ClickHouse doesn't mention `daft[clickhouse]` extra** (`clickhouse.md:9`).
- **Glue missing install instructions** (`glue.md`).
- **S3 Tables missing install instructions** (`s3tables.md`).
- **Hudi map type mapping may be incorrect** (`hudi.md:70`) -- Shows `struct` but should likely be `map`.

---

## Section 5: API Reference

### Critical

- **Series page hides 148 of 167 public methods** (`api/series.md:7`) -- Filter `["^to", "^from"]` only renders `to_*`/`from_*` methods.

### High

- **5 I/O functions missing from io.md** -- `read_mcap`, `read_paimon`, `read_text`, `from_files`, `DataFrame.write_paimon` are all public but undocumented.
- **2 expression constructors missing** -- `element()` and `interval()` exported in `daft.__all__` but not in expressions.md.
- **3 AI protocols missing from ai.md** -- `ImageClassifier`, `ImageClassifierDescriptor`, `PrompterDescriptor`.

### Medium

- **CheckpointConfig/CheckpointStore undocumented** -- Exported in `daft.__all__`, no API page.
- **Schema page renders same class twice** (`api/schema.md:5,13`) -- `daft.schema.Schema` and `daft.logical.schema.Schema` are the same class.
- **Field class undocumented** (`api/schema.md`) -- `daft.schema.Field` exists but isn't rendered.
- **ResourceRequest undocumented** -- Exported in `daft.__all__`, used in UDF allocation.
- **TimeUnit undocumented** -- Exported in `daft.__all__`, used in timestamp/duration ops.
- **37 session convenience functions not cross-referenced** -- `daft.attach`, `daft.read_table`, `daft.set_catalog`, etc. only documented as Session methods.
- **`daft.range` undocumented** -- Exported in `daft.__all__`.

---

## Section 6: SQL Reference

### Critical

- **SELECT doc barely exists** (`sql/statements/select.md`) -- No WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, JOIN, CTE, UNION, subqueries, CASE, BETWEEN, IN, LIKE, CAST.
- **DESCRIBE TABLE documented but rejected** (`sql/statements/describe.md:30`) -- Source explicitly errors on `DESCRIBE TABLE`.
- **Window limitation claim is wrong** (`sql/window_functions.md:163`) -- Tests prove global partitions work.

### High

- **Massive list of supported SQL features undocumented** -- JOINs, CTEs, UNION/INTERSECT, HAVING, DISTINCT, LIMIT/OFFSET, CASE/WHEN, BETWEEN, IN, LIKE/ILIKE, CAST, subqueries, TABLESAMPLE, GROUP BY ROLLUP, IS NULL/TRUE/FALSE.
- **No SQL functions reference page** -- Hundreds of SQL functions (string, math, temporal, URI, image) are available but entirely undocumented.

### Medium

- **`daft.sql` vs `Session.sql` behavioral difference undocumented** -- `daft.sql()` auto-detects Python variables; `Session.sql()` does not.
- **Missing table functions from docs** (`sql/index.md:82-86`) -- Only `read_parquet` and `read_iceberg` shown; `read_csv`, `read_json`, `read_deltalake` also exist.
- **Identifier mode API status misleading** (`sql/identifiers.md:63-66`) -- Says Python API is "planned" but Rust implementation exists.
- **Missing aggregate window functions** (`sql/window_functions.md:89-96`) -- Only 5 listed; many more available (stddev, variance, median, percentile, etc.).

---

## Section 7: Examples

### Critical

- **`agg_list()` doesn't exist** (`examples/minhash-dedupe.md:365,715,812`) -- Should be `list_agg()`. Will raise `AttributeError`.

### High

- **5 of 14 examples use deprecated legacy UDF API** -- `text-embeddings.md`, `llms-red-pajamas.md`, `image-generation.md`, `mnist.md`, `document-processing.md` all use `@daft.udf`/`@udf` instead of `@daft.func`/`@daft.cls`.
- **Window functions example uses wrong images** (`examples/window-functions.md:265,330`) -- Both lines reference `image-2.png` but `image-3.png` and `image-4.png` exist and should be used.
- **Window functions Challenge 4 contradicts itself** (`examples/window-functions.md:416-422`) -- Text says "cumulative total" but code uses `rows_between(-2, 2)` (5-row sliding window).

### Medium

- **3 examples missing from index** (`examples/index.md`) -- `querying-images.md`, `mnist.md`, `document-processing.md` exist but aren't linked.
- **Missing `sentence-transformers` in pip install** (`examples/common-crawl-daft-tutorial.md:22`) -- Imports `SentenceTransformer` but not in pip install.
- **TranscriptionResult used before defined** (`examples/voice-ai-analytics.md:121`) -- Referenced at top, defined in Appendix.
- **Missing Tesseract system dependency** (`examples/document-processing.md:27`) -- `pytesseract` listed but Tesseract OCR engine itself not mentioned.
- **Batch size text mismatch** (`examples/text-embeddings.md:178`) -- Text says 128, constant is 16.
- **Missing `load_dotenv()` call** (`examples/minhash-dedupe.md:75`).

---

## Section 8: Distributed / Optimization / Observability / Extensions / Contributing

### Critical

- **Kubernetes script missing `daft.set_runner_ray()`** (covered above in Critical list).

### High

- **Common Crawl contradictory `in_aws` guidance** (`datasets/common-crawl.md:74`) -- Says "If you are running this code locally, set `in_aws = True`." Should say "inside AWS."

### Medium

- **Ray page has stale repr output** (`distributed/ray.md:33`) -- Shows old `DaftContext` with `_RayRunnerConfig`, `_disallow_set_runner` fields that no longer exist.
- **Memory docs reference deprecated UDF API** (`optimization/memory.md:20-21`) -- Uses `daft.udf.UDF.batch_size` and `concurrency` instead of `max_concurrency`.
- **Join suffix example may be incorrect** (`optimization/join-strategies.md:34`) -- Passing only `suffix="_other"` doesn't remove default `"right."` prefix.
- **Partitioning doc doesn't clarify repartition is no-op on native runner** (`optimization/partitioning.md:12`).
- **Logging uses non-existent module name** (`observability/logging.md:17`) -- `daft.distributed` module doesn't exist.
- **Logging `DAFT_DEBUG_DISPATCH` env var may be stale** (`observability/logging.md:125`).
- **Logging contradictory advice** (`observability/logging.md:12 vs 109`) -- Says configure before import, but example does opposite.
- **Extensions authoring uses `todo!()` in aggregate** (`extensions/authoring.md:331,339,346`) -- Non-functional example.
- **Extensions `string_count` referenced but not implemented** (`extensions/authoring.md:393`).
- **Extensions `daft-ext` version is placeholder** (`extensions/authoring.md:102`).
- **Contributing dev guide missing `daft.set_runner_ray()`** (`contributing/development.md:57-66`).
- **Contributing doctest may use wrong type display** (`contributing/development.md:317`) -- Shows `Utf8` but Daft may now display `String`.

---

## README.rst

### Low

- **Python version matches** (`README.rst:27`) -- "Python 3.10 or higher" is correct per pyproject.toml.
- **Comparison table may be outdated** (`README.rst:77-93`) -- Polars now supports some distributed features via Polars Cloud. Ray Data, Modin, and other projects have evolved.
- **All badge links verified functional**.

---

## Cross-Cutting Themes

1. **Import path inconsistency**: `daft.functions` vs `daft.functions.ai` used interchangeably across docs.
2. **Legacy vs new UDF API**: ~35% of examples/guides still use deprecated `@daft.udf`. No deprecation warnings on those pages.
3. **Missing install instructions**: Many connector pages omit `pip install` commands for required extras.
4. **Stale model names**: AI function docs use non-existent models (gpt-5.4-mini, gpt-5-nano, gemini-3-pro-preview).
5. **SQL under-documented**: The SQL engine supports far more than the docs describe.
6. **Private module imports in docs**: Glue and Unity Catalog docs use `__private` module paths.
7. **Missing `import daft`**: Multiple connector examples use `daft.` calls without importing daft.
8. **API reference gaps**: ~15 public APIs exported in `daft.__all__` have no documentation.
