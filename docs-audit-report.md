# Daft Documentation Audit Report

**Date:** 2026-05-21
**Scope:** All documentation at docs.daft.ai (source in /docs) + README.rst
**Perspective:** AI agent assisting Daft users
**Files audited:** 119 markdown files across 8 sections

---

## Executive Summary

| Severity | Count | Description |
|----------|-------|-------------|
| CRITICAL | 7 | Broken code, wrong APIs, missing 91% of Series docs |
| HIGH | 16 | Internal API usage in docs, stale examples, misleading claims |
| MEDIUM | 31 | Incomplete docs, inconsistencies, confusing patterns |
| LOW | 30+ | Typos, cosmetic issues, minor inconsistencies |

The documentation is broadly well-structured but has significant issues that would mislead an AI agent. The most impactful problems are: (1) the Series API page showing only 9% of methods, (2) multiple broken links to non-existent API function pages, (3) code examples using internal/private imports, and (4) widespread use of the legacy `@daft.udf` decorator in examples while the new `@daft.func`/`@daft.cls` API is the recommended path.

---

## CRITICAL Issues (Must Fix)

### C1. Series API page hides ~91% of methods
- **File:** `docs/api/series.md:7`
- **Problem:** Filter `["^to", "^from"]` restricts the page to only methods starting with `to` or `from` (~19 out of ~203 methods). All math operations, comparisons, string/datetime/list accessors, `cast`, `filter`, `sort`, `name`, `rename`, etc. are completely hidden.
- **Fix:** Change filter to `["!^_"]` to match the pattern used in `schema.md` and `aggregations.md`.

### C2. Broken API reference links in audio.md
- **File:** `docs/modalities/audio.md:18-22`
- **Problem:** Links to `../api/functions/audio_file.md`, `../api/functions/audio_metadata.md`, `../api/functions/resample.md` -- none of these files exist. The `/docs/api/functions/` directory does not exist.
- **Fix:** Remove broken links or create the referenced pages.

### C3. Broken API reference links in videos.md
- **File:** `docs/modalities/videos.md:10-15`
- **Problem:** Links to `../api/functions/video_file.md`, `../api/functions/video_metadata.md`, `../api/functions/video_frames.md`, `../api/functions/video_keyframes.md` -- none exist.
- **Fix:** Remove broken links or create the referenced pages.

### C4. `col().agg_list()` does not exist in Python API
- **File:** `docs/examples/minhash-dedupe.md:366`
- **Problem:** `.agg(col("node_id").agg_list().alias("nodes"))` -- `agg_list()` is NOT a method on the Expression class. This would cause an `AttributeError` at runtime.
- **Fix:** Use `from daft.functions import list_agg` then `.agg(list_agg(col("node_id")).alias("nodes"))`.

### C5. `Table.from_pydict` docstring missing required argument
- **File:** `daft/catalog/__init__.py:893-894` (rendered in `docs/api/catalogs_tables.md`)
- **Problem:** Docstring shows `Table.from_pydict({"foo": [1, 2]})` but the actual signature is `from_pydict(name: str, data: dict)` -- requires TWO arguments.
- **Fix:** Update docstring to `Table.from_pydict("my_table", {"foo": [1, 2]})`.

### C6. sessions-usage.md has broken code block
- **File:** `docs/configuration/sessions-usage.md:232-256`
- **Problem:** Output tables are inside a `python` code block but NOT wrapped in triple-quoted strings. Every other code block in this file wraps output correctly. An agent extracting this code would get syntax errors.
- **Fix:** Wrap output tables in `"""..."""` like the rest of the file.

### C7. `set_runner_ray()` output example is completely wrong
- **File:** `docs/distributed/ray.md:30-33`
- **Problem:** Output shows `DaftContext(_daft_execution_config=..., _runner_config=_RayRunnerConfig(...))` but `set_runner_ray()` now returns a `Runner[PartitionT]` object. `DaftContext`, `_RayRunnerConfig`, `_disallow_set_runner` no longer exist.
- **Fix:** Update the output example to reflect the current return type.

---

## HIGH Issues (Should Fix Soon)

### H1. Connector docs use private/internal imports
- **File:** `docs/connectors/glue.md:20-21, 80` -- imports from `daft.catalog.__glue` (double-underscore private module)
- **File:** `docs/connectors/unity_catalog.md:28-29, 48-50, 95-120` -- imports from `daft.catalog.__unity`
- **Fix:** Use public APIs (`Catalog.from_glue()`, `Catalog.from_unity()`) or document internal APIs as unstable.

### H2. Broken GitHub link in custom-catalogs.md
- **File:** `docs/connectors/custom-catalogs.md:190`
- **Problem:** Link to `daft/catalog/__unity.py` returns 404 -- actual path is `daft/catalog/__unity/` (a directory).
- **Fix:** Update URL to point to the directory.

### H3. sql.md roadmap claims write support doesn't exist
- **File:** `docs/connectors/sql.md:156-158`
- **Problem:** Roadmap says "Write support into SQL databases" is planned, but `DataFrame.write_sql()` already exists.
- **Fix:** Remove from roadmap or mark as completed.

### H4. migration.md falsely claims @daft.method ignores retry kwargs
- **File:** `docs/custom-code/migration.md:133`
- **Problem:** States `@daft.method`/`@daft.method.batch` "accept the kwargs but currently ignore them." Source code shows method-level `max_retries` and `on_error` take priority over class-level values when set.
- **Fix:** Remove the incorrect claim; document that per-method overrides work.

### H5. cpus/gpus descriptions contradict across pages
- **File:** `docs/custom-code/func.md:420-426` says cpus/gpus are "used for concurrency control and scheduling -- not placement"
- **File:** `docs/custom-code/cls.md:91` says cpus is a "placement hint used by the scheduler"
- **Fix:** Harmonize to one consistent description (func.md's "concurrency control" matches the implementation).

### H6. Empty apis.md page published with no content
- **File:** `docs/custom-code/apis.md`
- **Problem:** Entire content is "# Using External APIs / User guide coming soon!" -- zero value for navigation.
- **Fix:** Either populate with content or remove from nav until ready.

### H7. Inconsistent import paths for AI functions
- **File:** `docs/modalities/text.md:23-24,49,77` -- uses `from daft.functions.ai import embed_text`
- **File:** `docs/modalities/images.md:401` -- uses `from daft.functions.ai import embed_image`
- **Problem:** The canonical import is `from daft.functions import embed_text/embed_image`. Using the `.ai` submodule is an implementation detail.
- **Fix:** Standardize all imports to `from daft.functions import ...`.

### H8. Identifiers doc contradicts itself
- **File:** `docs/sql/identifiers.md:3` says "configure these modes via the `SessionOptions`"
- **File:** `docs/sql/identifiers.md:64-66` says "Python APIs for configuring the identifier mode are planned for a future release"
- **Fix:** Remove the line 3 claim until the Python API exists.

### H9. Partitioning doc claims partitions are "only available on distributed runners"
- **File:** `docs/optimization/partitioning.md:12`
- **Problem:** `repartition()` exists on all DataFrames (it's a no-op warning on native runner). Should say "only effective on distributed runners."
- **Fix:** Soften the language.

### H10. Multiple I/O functions missing from API reference
- **File:** `docs/api/io.md`
- **Problem:** `daft.from_files`, `daft.read_text`, `daft.read_paimon`, `daft.read_mcap`, `DataFrame.write_paimon` are all public APIs but completely absent.
- **Fix:** Add mkdocstrings directives for these functions.

### H11. Window `range_between` docstring says NotImplementedError but it's implemented
- **File:** `daft/window.py:222` (rendered in `docs/api/window.md`)
- **Problem:** Docstring says `Raises: NotImplementedError: This feature is not yet implemented` but the method has a complete implementation.
- **Fix:** Remove the stale NotImplementedError from the docstring.

### H12. 6+ example pages use legacy @daft.udf without migration notice
- **Files:** `docs/examples/text-embeddings.md`, `llms-red-pajamas.md`, `image-generation.md`, `querying-images.md`, `mnist.md`, `document-processing.md`
- **Problem:** Use `@daft.udf(return_dtype=...)` (legacy) while the project recommends `@daft.func`/`@daft.cls`. No inline migration notice.
- **Fix:** Either update examples to use new API, or add a visible admonition linking to the migration guide.

### H13. Window functions example reuses same image for 3 different charts
- **File:** `docs/examples/window-functions.md:173, 265, 331`
- **Problem:** All three reference `window-functions-image-2.png` but represent different visualizations.
- **Fix:** Create separate images for ranking and delta charts.

### H14. Join strategies doc: suffix example may produce wrong schema
- **File:** `docs/optimization/join-strategies.md:26-36`
- **Problem:** Example uses `suffix="_other"` and claims schema `key, value, value_other`, but if default prefix `"right."` is still applied, the result would be `right.value_other`.
- **Fix:** Verify actual behavior and update example accordingly.

### H15. Logging doc references non-existent env var and module
- **File:** `docs/observability/logging.md:126` -- `DAFT_DEBUG_DISPATCH` env var doesn't exist in codebase
- **File:** `docs/observability/logging.md:16` -- `daft.distributed` logger name references non-existent module
- **Fix:** Remove the fake env var; use a real logger name like `daft.runners`.

### H16. `attach_table` docstring example is wrong
- **File:** `daft/catalog/__init__.py:29-32` (rendered in `docs/api/catalogs_tables.md`)
- **Problem:** Shows `daft.attach_table(df, "my_table")` where `df` is a DataFrame, but `attach_table` doesn't accept DataFrames.
- **Fix:** Use `daft.attach(df, "my_table")` or `daft.attach_view(df, "my_table")`.

---

## MEDIUM Issues (Fix When Possible)

### M1. `daft.sql()` vs `Session.sql()` behavioral difference undocumented
- **File:** `docs/sql/index.md:50-61`
- `daft.sql()` auto-detects DataFrames from Python scope; `Session.sql()` does not. This critical difference is never explained.

### M2. 14 Session methods missing from reference table
- **File:** `docs/configuration/sessions-usage.md:266-296`
- Missing: `attach_provider`, `attach_view`, `create_temp_view`, `current_model`, `current_provider`, `detach_function`, `detach_provider`, `get_aggregate_function`, `get_function`, `get_provider`, `has_provider`, `load_extension`, `set_model`, `set_provider`

### M3. Install page missing extras
- **File:** `docs/install.md`
- Valid extras not listed: `google`, `video`, `audio`, `sql`, `postgres`, `pandas`, `numpy`, `gravitino`

### M4. README references "installing from source" but install.md has no such section
- **File:** `README.rst:29` + `docs/install.md`

### M5. Telemetry opt-out docs incomplete
- **File:** `docs/telemetry.md:7`
- Only documents `DO_NOT_TRACK=true`. Doesn't mention `SCARF_NO_ANALYTICS`, `DAFT_ANALYTICS_ENABLED`, or that `=1` also works.

### M6. TPC-H benchmarks lack date/version info
- **File:** `docs/benchmarks/index.md`
- TPC-H section doesn't state when benchmarks were run or which framework versions were used (appear to be very old: Daft 0.1.3).

### M7. `expressions.md` missing `interval` and `element` constructors
- **File:** `docs/api/expressions.md`
- Both are top-level exports (`daft.interval`, `daft.element`) but not documented.

### M8. Duplicate Schema documentation
- **File:** `docs/api/schema.md:13-15`
- Documents both `daft.logical.schema.Schema` and `daft.schema.Schema` (same class, rendered twice).

### M9. `misc.md` missing many types
- **File:** `docs/api/misc.md`
- Only has `ImageMode`/`ImageFormat`. Missing: `ImageProperty`, `ResourceRequest`, `UnionMode`, `TimeUnit`, `MediaType`, `IOConfig`.

### M10. `prompt.md` uses fabricated model name
- **File:** `docs/ai-functions/prompt.md:49`
- Uses `model="gpt-5.4-mini"` which doesn't appear to be real.

### M11. `documents.md` has unused imports in example
- **File:** `docs/modalities/documents.md:17`
- Imports `regexp_split` and `embed_text` but neither is used in the code.

### M12. `s3tables.md` inconsistent table names
- **File:** `docs/connectors/s3tables.md:26 vs 56`
- Reads from `"my_namespace.my_table"` but writes to `"demo.points"`, then reads from the first name again to verify.

### M13. `hudi.md` typo
- **File:** `docs/connectors/hudi.md:51`
- "Apachi Hudi" should be "Apache Hudi"

### M14. `cls.md` typo
- **File:** `docs/custom-code/cls.md:262`
- "Use `@daft.cls` when some an expensive initialization step" -- grammatically broken.

### M15. `cls.md` batch_size example misplaced
- **File:** `docs/custom-code/cls.md:293-301`
- The "Batch Sizing" section under `@daft.cls` page shows `@daft.func.batch` instead of `@daft.method.batch`.

### M16. `migration.md` uses Series in non-batch method
- **File:** `docs/custom-code/migration.md:98-99`
- Shows `def generate(self, value: daft.Series):` in new API, but non-batch methods are row-wise and receive individual Python values.

### M17. `migration.md` incomplete GPU description
- **File:** `docs/custom-code/migration.md:123`
- Says "Fractional values up to 1.0 are supported" but doesn't mention integer values above 1.0 are also valid.

### M18. `overview.md` advertises daft.ImageFile but images.md never covers it
- **File:** `docs/modalities/overview.md:14`

### M19. `embeddings.md` uses unexplained `enable_dynamic_batching`
- **File:** `docs/modalities/embeddings.md:115`

### M20. `logging.md` contradictory import ordering advice
- **File:** `docs/observability/logging.md:13-14 vs 28-29`
- Says "Configure logging BEFORE importing daft" but then "Import daft first to access the utility function."

### M21. `logging.md` grammar error
- **File:** `docs/observability/logging.md:50`
- "The `refresh_logger()` will been called" -- should be "will be called."

### M22. `memory.md` broken cross-references
- **File:** `docs/optimization/memory.md:20-21`
- References `[daft.udf.UDF.batch_size]` and `[daft.udf.UDF.concurrency]` but `UDF` is in `daft.udf.legacy`, not `daft.udf`.

### M23. `mm_structured_outputs.md` set_provider signature
- **File:** `docs/examples/mm_structured_outputs.md:73`
- `daft.set_provider("openai", api_key=..., base_url=...)` -- kwargs belong to `OpenAIProvider` constructor, not `set_provider`.

### M24. `window-functions.md` code/text mismatch
- **File:** `docs/examples/window-functions.md:416-422`
- Code uses `.rows_between(-2, 2)` (5-row window) but text says "Daft defaults to including all previous rows" (cumulative).

### M25. `create_temp_table` description inaccurate
- **File:** `docs/configuration/sessions-usage.md:276`
- Says "Creates a temp table from an existing view" but actually creates from Schema or DataFrame.

### M26. `Catalog.list_tables()` docstring shows wrong return type
- **File:** `daft/catalog/__init__.py:93-94`
- Shows returning `['users']` (list of strings) but actual return type is `list[Identifier]`.

### M27. `RetryAfterError` documented at fragile import path
- **File:** `docs/custom-code/func.md:448`
- Documents `daft.ai.utils.RetryAfterError` but canonical location is `daft.errors.RetryAfterError`.

### M28. `ray.md` misleading default behavior claim
- **File:** `docs/distributed/ray.md:55`
- "By default...Daft will spin up a Ray cluster locally" is misleading -- Daft defaults to native runner.

### M29. `bigtable.md` unusual concat pattern
- **File:** `docs/connectors/bigtable.md:155-158`
- Chains `.concat("#").concat(col(...))` which is unusual; `daft.functions.concat()` is more idiomatic.

### M30. `type_conversions.md` stale Rust file path
- **File:** `docs/api/datatypes/type_conversions.md:5`
- References `src/daft-core/src-lit/python.rs` but actual path is `src/daft-core/src/lit/python.rs`.

### M31. `common-crawl.md` invalid Python syntax
- **File:** `docs/datasets/common-crawl.md:75-76`
- `in_aws: bool = ...` with ellipsis is not valid Python if copied.

---

## Cross-Cutting Themes

### Theme 1: Legacy vs. New UDF API Confusion
The single biggest source of confusion. The project has migrated from `@daft.udf` to `@daft.func`/`@daft.cls`/`@daft.method`, but 6+ example pages still use the legacy API without inline migration notices. An AI agent would not know which to recommend.

**Action:** Add deprecation admonitions to all legacy examples, or better yet, update them to the new API.

### Theme 2: Internal Import Paths Exposed in Docs
Multiple connector docs import from double-underscore private modules (`daft.catalog.__glue`, `daft.catalog.__unity`). AI functions docs inconsistently use `daft.functions.ai` (internal) vs `daft.functions` (public).

**Action:** Standardize all docs to use only public API import paths.

### Theme 3: API Reference Gaps
The Series page is nearly empty due to an overly restrictive filter. The I/O page is missing several public functions. The misc page is missing many exported types. The expressions page is missing constructor functions.

**Action:** Fix Series filter; add missing functions to I/O page; expand misc.md and expressions.md.

### Theme 4: Stale Examples and Outdated Output
Ray runner output example is completely wrong. TPC-H benchmarks use very old versions. SQL connector roadmap is stale.

**Action:** Update all output examples to match current codebase behavior.

---

## Prioritized Fix List (For Another Agent)

### Batch 1: Quick Wins (< 1 hour total)
1. Fix `docs/api/series.md` filter: change `["^to", "^from"]` to `["!^_"]`
2. Fix `daft/catalog/__init__.py` `Table.from_pydict` docstring: add `name` parameter
3. Fix `daft/catalog/__init__.py` `attach_table` docstring example
4. Fix `daft/window.py:222` `range_between` docstring: remove stale NotImplementedError
5. Fix `docs/connectors/sql.md:156-158`: remove stale "write support" roadmap item
6. Fix `docs/custom-code/migration.md:133`: remove false claim about ignored kwargs
7. Fix `docs/custom-code/cls.md:262`: fix typo "some an expensive"
8. Fix `docs/connectors/hudi.md:51`: fix "Apachi" -> "Apache"
9. Fix `docs/observability/logging.md:50`: fix "will been called" grammar

### Batch 2: Content Fixes (2-4 hours total)
10. Fix `docs/distributed/ray.md:30-33`: update `set_runner_ray()` output example
11. Fix `docs/configuration/sessions-usage.md:232-256`: wrap output in triple-quotes
12. Fix `docs/configuration/sessions-usage.md:266-296`: add 14 missing Session methods
13. Fix `docs/api/io.md`: add `from_files`, `read_text`, `read_paimon`, `read_mcap`, `write_paimon`
14. Fix `docs/api/expressions.md`: add `interval` and `element` constructors
15. Fix `docs/api/misc.md`: add `ImageProperty`, `ResourceRequest`, `UnionMode`, `TimeUnit`, `MediaType`, `IOConfig`
16. Remove broken links from `docs/modalities/audio.md` and `docs/modalities/videos.md`
17. Fix `docs/sql/identifiers.md:3`: remove premature SessionOptions claim
18. Harmonize cpus/gpus descriptions across `func.md` and `cls.md`
19. Standardize AI function imports to `from daft.functions import ...`

### Batch 3: Example Updates (4-8 hours total)
20. Fix `docs/examples/minhash-dedupe.md:366`: replace `agg_list()` with `list_agg()`
21. Update legacy UDF examples or add migration admonitions (6 files)
22. Fix `docs/examples/window-functions.md`: create separate images, fix code/text mismatch
23. Update connector docs to use public APIs instead of internal imports (glue.md, unity_catalog.md)
24. Fix `docs/connectors/custom-catalogs.md:190`: update broken GitHub URL
25. Populate or remove `docs/custom-code/apis.md`

### Batch 4: Completeness Improvements (4-8 hours)
26. Add "install from source" section to `docs/install.md` (or remove README reference)
27. Add missing install extras to `docs/install.md`
28. Document `daft.sql()` vs `Session.sql()` behavioral difference
29. Update telemetry docs with all opt-out env vars
30. Add date/versions to TPC-H benchmarks section
31. Document `daft.ImageFile` in `docs/modalities/images.md`
