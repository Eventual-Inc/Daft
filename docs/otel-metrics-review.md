# Daft OTEL Metrics Review

Full audit of OpenTelemetry metric naming, attributes, and instrumentation patterns across the Daft codebase, cross-referenced against the [OTel Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/general/) and [Metrics API spec](https://opentelemetry.io/docs/specs/otel/metrics/api/).

## Current Inventory

### Instrumentation Scopes (2)

| Scope Name | Location | Scope Attributes |
|---|---|---|
| `daft.distributed.node_stats` | `src/daft-distributed/src/statistics/mod.rs:107` | `query_id_stats` |
| `daft.local.node_stats` | `src/daft-local-execution/src/pipeline.rs:204` | `query_id` |

### Metric Instruments (12 distinct names after normalization)

| Raw Literal | Normalized | Type | Unit Set? | Files |
|---|---|---|---|---|
| `"cpu us"` | `daft.cpu_us` | Counter\<u64\> | No | all stats structs |
| `"rows in"` | `daft.rows_in` | Counter\<u64\> | No | all stats structs |
| `"rows out"` | `daft.rows_out` | Counter\<u64\> | No | all stats structs |
| `"bytes read"` | `daft.bytes_read` | Counter\<u64\> | No | scan_source.rs |
| `"rows written"` | `daft.rows_written` | Counter\<u64\> | No | sink.rs, write.rs |
| `"bytes written"` | `daft.bytes_written` | Counter\<u64\> | No | sink.rs, write.rs |
| `"selectivity"` | `daft.selectivity` | Gauge\<f64\> | No | filter.rs (both paths) |
| `"amplification"` | `daft.amplification` | Gauge\<f64\> | No | explode.rs (both paths) |
| `"active_tasks"` | `active_tasks` **[BUG]** | UpDownCounter\<i64\> | No | stats.rs:43 |
| `"completed_tasks"` | `daft.completed_tasks` | Counter\<u64\> | No | stats.rs:44 |
| `"failed_tasks"` | `daft.failed_tasks` | Counter\<u64\> | No | stats.rs:45 |
| `"cancelled_tasks"` | `daft.cancelled_tasks` | Counter\<u64\> | No | stats.rs:46 |

### Attribute Keys (8 distinct keys)

| Key | Value | Where Used |
|---|---|---|
| `"node_id"` | stringified id | local-exec everywhere; distributed filter, explode, udf, sink |
| `"node_id_rnm"` | stringified id | distributed RuntimeNodeManager (`stats.rs:37`) |
| `"node_id_dfs"` | stringified id | distributed DefaultRuntimeStats (`stats.rs:98`) |
| `"node_id_scan"` | stringified id | distributed SourceStats (`scan_source.rs:37`) |
| `"node_id_limit"` | stringified id | distributed LimitStats (`limit.rs:43`) |
| `"operator"` | node_type Debug string | distributed DefaultRuntimeStats only (`stats.rs:99`) |
| `"query_id_stats"` | stringified query_id | distributed scope attribute (`mod.rs:109`) |
| `"query_id"` | stringified query_id | local scope attribute (`pipeline.rs:205`) |

### RuntimeStats Implementations

**Local execution** (`src/daft-local-execution/src/`):

| File | Struct | Counters | Gauges |
|---|---|---|---|
| `runtime_stats/values.rs` | DefaultRuntimeStats | cpu_us, rows_in, rows_out | - |
| `intermediate_ops/filter.rs` | FilterStats | cpu_us, rows_in, rows_out | selectivity |
| `intermediate_ops/explode.rs` | ExplodeStats | cpu_us, rows_in, rows_out | - |
| `intermediate_ops/udf.rs` | UdfRuntimeStats | cpu_us, rows_in, rows_out + dynamic | - |
| `streaming_sink/async_udf.rs` | AsyncUdfRuntimeStats | cpu_us, rows_in, rows_out + dynamic | - |
| `sources/source.rs` | SourceStats | cpu_us, rows_out | - |
| `sinks/write.rs` | WriteStats | cpu_us, rows_in, rows_written, bytes_written | - |

**Distributed execution** (`src/daft-distributed/src/`):

| File | Struct | Counters | Gauges |
|---|---|---|---|
| `statistics/stats.rs` | DefaultRuntimeStats | rows_in, rows_out, cpu_us | - |
| `statistics/stats.rs` | RuntimeNodeManager | active_tasks (UpDown), completed/failed/cancelled_tasks | - |
| `pipeline_node/scan_source.rs` | SourceStats | cpu_us, rows_out, bytes_read | - |
| `pipeline_node/limit.rs` | LimitStats | cpu_us, rows_in, rows_out | - |
| `pipeline_node/filter.rs` | FilterStats | cpu_us, rows_in, rows_out | selectivity |
| `pipeline_node/explode.rs` | ExplodeStats | cpu_us, rows_in, rows_out | amplification |
| `pipeline_node/udf.rs` | UdfStats | cpu_us, rows_in, rows_out + dynamic | - |
| `pipeline_node/sink.rs` | WriteStats | cpu_us, rows_in, rows_written, bytes_written | - |

### Shared Helpers

| File | Purpose |
|---|---|
| `src/common/metrics/src/meters.rs` | `Counter`, `Gauge` wrappers; `normalize_name()` function |
| `src/common/metrics/src/lib.rs` | Constants: `ROWS_IN_KEY`, `ROWS_OUT_KEY`, `CPU_US_KEY` |
| `src/common/metrics/src/snapshot.rs` | `StatSnapshot` enum with per-operator snapshot types |
| `src/common/metrics/src/operator_metrics.rs` | `OperatorMetrics` / `MetricsCollector` for UDF custom counters |
| `src/common/metrics/src/ops.rs` | `NodeType`, `NodeCategory`, `NodeInfo` definitions |
| `src/common/tracing/src/lib.rs` | Global OTel provider init (resource: `service.name = "daft"`) |
| `src/common/tracing/src/config.rs` | Env-var config for OTLP endpoints, protocol, export interval |

---

## Issues Found

### Issue 1 — Fragmented node-ID attribute keys (P0)

**Problem:** Five different keys (`node_id`, `node_id_rnm`, `node_id_dfs`, `node_id_scan`, `node_id_limit`) all represent the same concept: which plan operator produced this metric. This makes it impossible to write a single dashboard query that works across all metric types.

**OTel rule violated:** *"Common attributes SHOULD be consistently named"* and *"aggregations over all attributes of a given metric SHOULD be meaningful."*

**Fix:** Use a single key `daft.operator.id` everywhere. The suffixes (`_rnm`, `_dfs`, `_scan`, `_limit`) are debug-era disambiguators — the stats structs already provide type differentiation through the metric variants. The `daft.` namespace prefix avoids collision with OTel's infrastructure `node.*` attributes.

### Issue 2 — Inconsistent query-ID attribute key (P0)

**Problem:** `"query_id_stats"` (distributed, `mod.rs:109`) vs `"query_id"` (local, `pipeline.rs:205`). These are scope-level attributes, so cross-runtime joins are broken.

**Fix:** Standardize to `daft.query.id` in both paths.

### Issue 3 — `active_tasks` bypasses normalization (P0)

**Problem:** At `stats.rs:43`, `meter.i64_up_down_counter("active_tasks").build()` is called directly, skipping the `Counter::new()` wrapper and `normalize_name()`. The instrument is registered as `active_tasks` instead of `daft.active_tasks`. Every other metric goes through normalization.

**Fix:** Either create an `UpDownCounter` wrapper in `meters.rs` that calls `normalize_name()`, or pass the already-normalized name directly.

### Issue 4 — Split instrumentation scopes (P2, Optional)

**Current state:** Two scopes (`daft.distributed.node_stats`, `daft.local.node_stats`).

**Keeping separate scopes is defensible.** The OTel spec says scope identifies *"the instrumentation library, package, module or class name"* — and `daft-local-execution` and `daft-distributed` are genuinely different Rust crates with different `RuntimeStats` traits, different recording patterns (streaming vs snapshot-aggregate), and partially different metric sets (task metrics only exist in distributed). Separate scopes honestly represent this.

**Merging to one scope is also valid** if you want cross-engine queries to be trivial (no UNION across scope boundaries). A single scope `daft.execution` with attribute `daft.execution.engine = "local" | "distributed"` works, but is a convenience choice, not a correctness fix.

**What IS required regardless:** The scope attribute key for query ID must be consistent — `daft.query.id` in both scopes, not the current `query_id_stats` vs `query_id` mismatch (covered by Issue #2). If keeping separate scopes, also consider renaming them to follow the `daft.` namespace: `daft.local.execution` and `daft.distributed.execution`.

**Note on cardinality:** Both scopes put `query_id` as a scope attribute, creating a new `Meter` per query. For long-running services with many queries, consider moving `query_id` to per-recording attributes. For batch jobs this is fine.

### Issue 5 — `operator` type attribute only partially applied (P1)

**Problem:** Only distributed `DefaultRuntimeStats` (`stats.rs:98-99`) attaches `KeyValue::new("operator", ...)`. The other distributed stats structs and *all* local-execution stats structs don't. Dashboards can't filter by operator type for most metrics.

**Fix:** Add `daft.operator.type` and `daft.operator.category` to the KeyValue vec in all stats constructors. Both `NodeType` and `NodeCategory` are available at construction time in both paths.

### Issue 6 — Metric names embed units and use flat naming (P2)

**OTel rules violated:**
- *"Units do not need to be specified in the names since they are included during instrument creation"*
- *"Use namespacing. Delimit the namespaces using a dot character"*
- *"Metric names SHOULD NOT be pluralized"*
- *"Use `system.network.packet.dropped` instead of `system.network.dropped`"* (the `{object}.{property}` pattern)

| Current | Problem | Recommended |
|---|---|---|
| `daft.cpu_us` | Embeds unit (`us`), underscore where dot applies | `daft.cpu.time` |
| `daft.rows_in` | Plural, underscore where dot applies | `daft.row.in` |
| `daft.rows_out` | Same | `daft.row.out` |
| `daft.bytes_read` | Underscore where dot applies | `daft.byte.read` |
| `daft.bytes_written` | Same | `daft.byte.written` |
| `daft.rows_written` | Plural, underscore | `daft.row.written` |
| `daft.completed_tasks` | Underscore, plural, flat | `daft.task.completed` |
| `daft.failed_tasks` | Same | `daft.task.failed` |
| `daft.cancelled_tasks` | Same | `daft.task.cancelled` |
| `daft.active_tasks` | Same + missing normalization | `daft.task.active` |
| `daft.selectivity` | No namespace depth | `daft.filter.selectivity` |
| `daft.amplification` | No namespace depth | `daft.explode.amplification` |

### Issue 7 — No unit metadata on any instrument (P2)

**Problem:** Neither `Counter::new()` nor `Gauge::new()` calls `.with_unit()`. OTel exporters (Prometheus, Grafana OTLP) use unit metadata to auto-suffix metric names and label axes.

**Fix:** Add an optional `unit` parameter to both wrappers in `meters.rs`. Recommended units (UCUM):

| Metric | Unit |
|---|---|
| `daft.cpu.time` | `us` |
| `daft.row.*` | `{row}` |
| `daft.byte.*` | `By` |
| `daft.task.*` | `{task}` |
| `daft.filter.selectivity` | `1` (dimensionless) |
| `daft.explode.amplification` | `1` |

### Issue 8 — Spaces in metric-name source constants (P2)

**Problem:** Constants in `lib.rs:119-121` use spaces (`"rows in"`, `"rows out"`, `"cpu us"`) and rely on `normalize_name()` to fix them at runtime. Ad-hoc literals like `"bytes read"` are scattered across files. A new contributor may not realize spaces get normalized.

**Fix:** Define all metric names as their canonical dot-separated form. `normalize_name()` becomes a safety net rather than a required transformation.

### Issue 9 — Selectivity computed as 0–100 percentage (P3)

**Problem:** `FilterStats::selectivity()` returns `(rows_out / rows_in) * 100.0`. OTel convention for ratio metrics: *"Use dimensionless `1` for utilization fractions"* — meaning 0.0–1.0.

**Fix:** Record as 0.0–1.0 ratio. Display formatting can still show as percentage for human readability.

### Issue 10 — No descriptions on any metric (P3)

**Problem:** Every `Counter::new()` and `Gauge::new()` call passes `None` for description. OTel exporters surface descriptions (Prometheus `# HELP` lines, Grafana tooltips).

**Fix:** Add descriptions to at least the shared metrics. Example: `daft.cpu.time` → `"Cumulative CPU time consumed by this operator"`.

### Issue 11 — `normalize_name()` doesn't validate characters (P3)

**Problem:** `normalize_name()` replaces spaces and lowercases, but doesn't enforce the OTel name character set (alphanumeric + underscore, period, hyphen, forward slash). Dynamic UDF counter names from user code could include invalid characters.

**Fix:** Add a validation/sanitization pass when touching this function for other reasons.

---

## Final Recommendations

### Phase 1 — Fix broken semantics (do first, one PR)

These are correctness bugs. Metrics are emitted today with inconsistent keys, making cross-operator and cross-engine queries impossible.

**1a. One attribute key for operator identity.**

Replace all five `node_id*` variants with `daft.operator.id`:

| File | Line | Change |
|---|---|---|
| `statistics/stats.rs` | 37 | `"node_id_rnm"` → `"daft.operator.id"` |
| `statistics/stats.rs` | 98 | `"node_id_dfs"` → `"daft.operator.id"` |
| `pipeline_node/scan_source.rs` | 37 | `"node_id_scan"` → `"daft.operator.id"` |
| `pipeline_node/limit.rs` | 43 | `"node_id_limit"` → `"daft.operator.id"` |
| `pipeline_node/filter.rs` | 31 | `"node_id"` → `"daft.operator.id"` |
| `pipeline_node/explode.rs` | 31 | `"node_id"` → `"daft.operator.id"` |
| `pipeline_node/udf.rs` | 39 | `"node_id"` → `"daft.operator.id"` |
| `pipeline_node/sink.rs` | 41 | `"node_id"` → `"daft.operator.id"` |
| `runtime_stats/values.rs` | 38 | `"node_id"` → `"daft.operator.id"` |
| All local stats structs | — | `"node_id"` → `"daft.operator.id"` |

**1b. One attribute key for query identity.**

| File | Line | Change |
|---|---|---|
| `statistics/mod.rs` | 109 | `"query_id_stats"` → `"daft.query.id"` |
| `pipeline.rs` | 205 | `"query_id"` → `"daft.query.id"` |

**1c. Fix `active_tasks` normalization bypass.**

At `stats.rs:43`, `meter.i64_up_down_counter("active_tasks").build()` skips `normalize_name()`. Either:
- Add an `UpDownCounter` wrapper to `meters.rs` (keeps the pattern consistent), or
- Pass the literal `"daft.task.active"` directly

**1d. Rename `"operator"` attribute key to `daft.operator.type`.**

At `stats.rs:99`, `"operator"` → `"daft.operator.type"`. This follows the same `daft.*` namespace as the other attributes and avoids collision with infrastructure-level `operator` concepts.

### Phase 2 — Add operator context everywhere (one PR, additive)

Only distributed `DefaultRuntimeStats` attaches operator type today. Add `daft.operator.type` and `daft.operator.category` to every stats struct constructor, both local and distributed. These are additive attributes — no existing queries break, dashboards gain a new filter dimension.

The pattern in every stats struct becomes:

```rust
// All stats structs should build their KeyValue vec like this.
// NodeType and NodeCategory are already available at construction time in both paths.
let node_kv = vec![
    KeyValue::new("daft.operator.id", node_id.to_string()),
    KeyValue::new("daft.operator.type", node_type.to_string()),
    KeyValue::new("daft.operator.category", node_category.to_string()),
];
```

This touches every stats struct in both execution paths (15 files), but each change is mechanical — add two parameters to the constructor and two entries to the vec.

### Phase 3 — Clean up metric names and wrappers (one PR, coordinate with dashboards)

**3a. Rename metrics to dot-separated canonical form.**

| Current Constant/Literal | New Constant | New Value |
|---|---|---|
| `ROWS_IN_KEY = "rows in"` | `ROW_IN_KEY` | `"daft.row.in"` |
| `ROWS_OUT_KEY = "rows out"` | `ROW_OUT_KEY` | `"daft.row.out"` |
| `CPU_US_KEY = "cpu us"` | `CPU_TIME_KEY` | `"daft.cpu.time"` |
| `"bytes read"` | `BYTE_READ_KEY` | `"daft.byte.read"` |
| `"bytes written"` | `BYTE_WRITTEN_KEY` | `"daft.byte.written"` |
| `"rows written"` | `ROW_WRITTEN_KEY` | `"daft.row.written"` |
| `"completed_tasks"` | `TASK_COMPLETED_KEY` | `"daft.task.completed"` |
| `"failed_tasks"` | `TASK_FAILED_KEY` | `"daft.task.failed"` |
| `"cancelled_tasks"` | `TASK_CANCELLED_KEY` | `"daft.task.cancelled"` |
| `"active_tasks"` | `TASK_ACTIVE_KEY` | `"daft.task.active"` |
| `"selectivity"` | `FILTER_SELECTIVITY_KEY` | `"daft.filter.selectivity"` |
| `"amplification"` | `EXPLODE_AMPLIFICATION_KEY` | `"daft.explode.amplification"` |

Centralizing all names as constants eliminates ad-hoc string literals scattered across files. `normalize_name()` becomes a safety net, not a required transformation.

Also update the string keys in `snapshot.rs` `to_stats()` implementations (e.g. `"bytes_read"` at line 62, `"rows written"` at line 210) to match.

**3b. Add `unit` parameter to `Counter` and `Gauge` wrappers.**

```rust
// meters.rs
pub fn new(
    meter: &Meter,
    name: impl Into<Cow<'static, str>>,
    description: Option<Cow<'static, str>>,
    unit: Option<Cow<'static, str>>,  // NEW
) -> Self {
    let normalized_name = normalize_name(name.into());
    let mut builder = meter.u64_counter(normalized_name);
    if let Some(description) = description { builder = builder.with_description(description); }
    if let Some(unit) = unit { builder = builder.with_unit(unit); }
    // ...
}
```

Recommended units:

| Metric | Unit | Rationale |
|---|---|---|
| `daft.cpu.time` | `us` | Microseconds (UCUM) |
| `daft.row.*` | `{row}` | Annotation for dimensionless count |
| `daft.byte.*` | `By` | Bytes (UCUM, non-prefixed as spec recommends) |
| `daft.task.*` | `{task}` | Annotation for dimensionless count |
| `daft.filter.selectivity` | `1` | Dimensionless ratio |
| `daft.explode.amplification` | `1` | Dimensionless ratio |

**3c. Add descriptions to shared metrics.**

| Metric | Description |
|---|---|
| `daft.cpu.time` | `"Cumulative CPU time consumed by this operator"` |
| `daft.row.in` | `"Number of rows received by this operator"` |
| `daft.row.out` | `"Number of rows emitted by this operator"` |
| `daft.row.written` | `"Number of rows written to output sink"` |
| `daft.byte.read` | `"Bytes read from input source"` |
| `daft.byte.written` | `"Bytes written to output sink"` |
| `daft.task.active` | `"Number of tasks currently executing for this operator"` |
| `daft.task.completed` | `"Number of tasks completed successfully"` |
| `daft.task.failed` | `"Number of tasks that failed"` |
| `daft.task.cancelled` | `"Number of tasks that were cancelled"` |
| `daft.filter.selectivity` | `"Fraction of input rows that pass the filter predicate"` |
| `daft.explode.amplification` | `"Ratio of output rows to input rows after explode"` |

### Phase 4 — Polish (separate PRs, low urgency)

**4a. Selectivity scale.** Change `FilterStats::selectivity()` from `* 100.0` to raw ratio (0.0–1.0). Update display formatting in `snapshot.rs` to format as percentage for human readability. The OTel-emitted gauge value becomes spec-compliant.

**4b. Harden `normalize_name()`.** Add character validation/sanitization to strip characters outside the OTel instrument name character set (alphanumeric, underscore, period, hyphen, slash). Primarily matters for dynamic UDF counter names that originate from user code.

**4c. Scope rename (optional).** If keeping separate scopes, rename from `daft.distributed.node_stats` / `daft.local.node_stats` to `daft.distributed.execution` / `daft.local.execution`. The `node_stats` suffix is an implementation detail that doesn't belong in a scope name. If merging to a single scope, use `daft.execution` with `daft.execution.engine` attribute.

---

## Files Touched by Phase

### Phase 1 (P0 fixes)

| File | Changes |
|---|---|
| `src/common/metrics/src/meters.rs` | Add `UpDownCounter` wrapper (or inline fix) |
| `src/daft-distributed/src/statistics/mod.rs` | `"query_id_stats"` → `"daft.query.id"` |
| `src/daft-distributed/src/statistics/stats.rs` | `"node_id_rnm"` → `"daft.operator.id"`, `"node_id_dfs"` → same, `"operator"` → `"daft.operator.type"`, fix `active_tasks` |
| `src/daft-distributed/src/pipeline_node/scan_source.rs` | `"node_id_scan"` → `"daft.operator.id"` |
| `src/daft-distributed/src/pipeline_node/limit.rs` | `"node_id_limit"` → `"daft.operator.id"` |
| `src/daft-distributed/src/pipeline_node/filter.rs` | `"node_id"` → `"daft.operator.id"` |
| `src/daft-distributed/src/pipeline_node/explode.rs` | `"node_id"` → `"daft.operator.id"` |
| `src/daft-distributed/src/pipeline_node/udf.rs` | `"node_id"` → `"daft.operator.id"` |
| `src/daft-distributed/src/pipeline_node/sink.rs` | `"node_id"` → `"daft.operator.id"` |
| `src/daft-local-execution/src/pipeline.rs` | `"query_id"` → `"daft.query.id"` |
| `src/daft-local-execution/src/runtime_stats/values.rs` | `"node_id"` → `"daft.operator.id"` |
| `src/daft-local-execution/src/intermediate_ops/filter.rs` | `"node_id"` → `"daft.operator.id"` |
| `src/daft-local-execution/src/intermediate_ops/explode.rs` | `"node_id"` → `"daft.operator.id"` |
| `src/daft-local-execution/src/intermediate_ops/udf.rs` | `"node_id"` → `"daft.operator.id"` |
| `src/daft-local-execution/src/streaming_sink/async_udf.rs` | `"node_id"` → `"daft.operator.id"` |
| `src/daft-local-execution/src/sources/source.rs` | `"node_id"` → `"daft.operator.id"` |
| `src/daft-local-execution/src/sinks/write.rs` | `"node_id"` → `"daft.operator.id"` |

### Phase 2 (operator context)

Same files as Phase 1, plus constructor signature changes to accept `NodeType` and `NodeCategory`.

### Phase 3 (naming + wrappers)

| File | Changes |
|---|---|
| `src/common/metrics/src/lib.rs` | Rename and centralize all metric name constants |
| `src/common/metrics/src/meters.rs` | Add `unit` param to `Counter::new()` and `Gauge::new()` |
| `src/common/metrics/src/snapshot.rs` | Update string keys in `to_stats()` implementations |
| All stats struct files | Update `Counter::new()` / `Gauge::new()` calls with unit and description |

### Phase 4 (polish)

| File | Changes |
|---|---|
| `src/common/metrics/src/meters.rs` | Harden `normalize_name()` |
| `src/common/metrics/src/snapshot.rs` | Update selectivity display format |
| `src/daft-distributed/src/pipeline_node/filter.rs` | Remove `* 100.0` from selectivity |
| `src/daft-local-execution/src/intermediate_ops/filter.rs` | Same |
| `src/daft-distributed/src/statistics/mod.rs` | Optional scope rename |
| `src/daft-local-execution/src/pipeline.rs` | Optional scope rename |
