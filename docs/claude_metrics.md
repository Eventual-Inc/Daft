# Daft OTEL Metrics — Final Recommendations

Actionable recommendations for cleaning up OpenTelemetry metrics across the Daft codebase. Based on a full audit cross-referenced against the [OTel Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/general/) and [Metrics API spec](https://opentelemetry.io/docs/specs/otel/metrics/api/). See `docs/otel-metrics-review.md` for the complete audit with current inventory and issue analysis.

---

## Target Metric Names

OTel naming rules applied:
- Dot-separated namespacing: `daft.{object}.{property}`
- Pluralize when unit is a non-unit annotation (`{row}`, `{task}`); singular when unit is standard UCUM (`us`, `By`, `1`)
- Units in instrument metadata via `.with_unit()`, not embedded in metric names

| Current | Target | Type | Unit | Description |
|---|---|---|---|---|
| `daft.cpu_us` | `daft.cpu.time` | Counter\<u64\> | `us` | Cumulative CPU time consumed by this operator |
| `daft.rows_in` | `daft.rows.in` | Counter\<u64\> | `{row}` | Number of rows received by this operator |
| `daft.rows_out` | `daft.rows.out` | Counter\<u64\> | `{row}` | Number of rows emitted by this operator |
| `daft.rows_written` | `daft.rows.written` | Counter\<u64\> | `{row}` | Number of rows written to output sink |
| `daft.bytes_read` | `daft.source.io` | Counter\<u64\> | `By` | Bytes read from input source |
| `daft.bytes_written` | `daft.sink.io` | Counter\<u64\> | `By` | Bytes written to output sink |
| `active_tasks` [bug] | `daft.tasks.active` | UpDownCounter\<i64\> | `{task}` | Number of tasks currently executing for this operator |
| `daft.completed_tasks` | `daft.tasks.completed` | Counter\<u64\> | `{task}` | Number of tasks completed successfully |
| `daft.failed_tasks` | `daft.tasks.failed` | Counter\<u64\> | `{task}` | Number of tasks that failed |
| `daft.cancelled_tasks` | `daft.tasks.cancelled` | Counter\<u64\> | `{task}` | Number of tasks that were cancelled |
| `daft.selectivity` | `daft.filter.selectivity` | Gauge\<f64\> | `1` | Fraction of input rows that pass the filter predicate (0.0–1.0) |
| `daft.amplification` | `daft.explode.amplification` | Gauge\<f64\> | `1` | Ratio of output rows to input rows after explode |

### Constants (`src/common/metrics/src/lib.rs`)

```rust
// Core data-flow metrics
pub const CPU_TIME_KEY: &str = "daft.cpu.time";
pub const ROWS_IN_KEY: &str = "daft.rows.in";
pub const ROWS_OUT_KEY: &str = "daft.rows.out";
pub const ROWS_WRITTEN_KEY: &str = "daft.rows.written";
pub const SOURCE_IO_KEY: &str = "daft.source.io";
pub const SINK_IO_KEY: &str = "daft.sink.io";

// Task lifecycle metrics (distributed only)
pub const TASKS_ACTIVE_KEY: &str = "daft.tasks.active";
pub const TASKS_COMPLETED_KEY: &str = "daft.tasks.completed";
pub const TASKS_FAILED_KEY: &str = "daft.tasks.failed";
pub const TASKS_CANCELLED_KEY: &str = "daft.tasks.cancelled";

// Operator-specific metrics
pub const FILTER_SELECTIVITY_KEY: &str = "daft.filter.selectivity";
pub const EXPLODE_AMPLIFICATION_KEY: &str = "daft.explode.amplification";
```

---

## Target Attribute Keys

### Scope-level attributes

| Current | Target | Where |
|---|---|---|
| `"query_id_stats"` | `daft.query.id` | `statistics/mod.rs:109` |
| `"query_id"` | `daft.query.id` | `pipeline.rs:205` |

### Per-recording attributes

Replace 5 fragmented node-ID keys (`node_id`, `node_id_rnm`, `node_id_dfs`, `node_id_scan`, `node_id_limit`) and the bare `operator` key with a consistent set:

```rust
let node_kv = vec![
    KeyValue::new("daft.operator.id", node_id.to_string()),
    KeyValue::new("daft.operator.type", node_type.to_string()),
    KeyValue::new("daft.operator.category", node_category.to_string()),
];
```

| Key | Value | Purpose |
|---|---|---|
| `daft.operator.id` | Stringified node ID | Identifies the specific plan operator |
| `daft.operator.type` | `NodeType` Debug string (e.g. `Filter`, `ScanTask`, `Write`) | Enables filtering by operator kind |
| `daft.operator.category` | `NodeCategory` Debug string (`Intermediate`, `Source`, `StreamingSink`, `BlockingSink`) | Enables grouping by operator class |

---

## Target Instrumentation Scopes

Separate scopes are kept (they map to genuinely different Rust crates). Rename for consistency and fix the attribute key mismatch:

```rust
// Distributed — src/daft-distributed/src/statistics/mod.rs
let scope = InstrumentationScope::builder("daft.distributed.execution")
    .with_attributes(vec![
        KeyValue::new("daft.query.id", query_id.to_string()),
    ])
    .build();

// Local — src/daft-local-execution/src/pipeline.rs
let scope = InstrumentationScope::builder("daft.local.execution")
    .with_attributes(vec![
        KeyValue::new("daft.query.id", query_id.to_string()),
    ])
    .build();
```

---

## Wrapper API Changes (`src/common/metrics/src/meters.rs`)

### Add `unit` parameter to `Counter` and `Gauge`

```rust
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

Same change for `Gauge::new()`.

### Add `UpDownCounter` wrapper (or pass fully-qualified name)

`active_tasks` at `stats.rs:43` currently bypasses `normalize_name()` by calling `meter.i64_up_down_counter("active_tasks").build()` directly. Either add a wrapper or pass `"daft.tasks.active"` directly.

### Harden `normalize_name()`

Add character validation to strip characters outside the OTel instrument name character set (alphanumeric, underscore, period, hyphen, slash). Matters for dynamic UDF counter names from user code.

---

## Implementation Phases

### Phase 1 — Fix broken semantics (one PR)

Correctness bugs. Metrics are emitted today with inconsistent keys, making cross-operator and cross-engine queries impossible.

| Change | Files | Effort |
|---|---|---|
| All `node_id*` variants → `daft.operator.id` | 17 files (all stats structs) | Mechanical string replace |
| `query_id_stats` / `query_id` → `daft.query.id` | `statistics/mod.rs`, `pipeline.rs` | 2 lines |
| `"operator"` → `"daft.operator.type"` | `statistics/stats.rs` | 1 line |
| Fix `active_tasks` normalization bypass | `statistics/stats.rs` | 1 line |

### Phase 2 — Add operator context everywhere (one PR, additive)

Add `daft.operator.type` and `daft.operator.category` to every stats struct constructor. No existing queries break; dashboards gain a new filter dimension.

| Change | Files | Effort |
|---|---|---|
| Add `NodeType` + `NodeCategory` to all stats struct constructors and KeyValue vecs | 15 files | Mechanical — add params + 2 vec entries each |

### Phase 3 — Clean up metric names and wrappers (one PR, coordinate with dashboards)

| Change | Files | Effort |
|---|---|---|
| Rename and centralize all metric name constants | `lib.rs` | Define 12 constants |
| Add `unit` param to `Counter::new()` / `Gauge::new()` | `meters.rs` | Small API change |
| Update all `Counter::new()` / `Gauge::new()` call sites with new names, units, descriptions | All stats structs + `snapshot.rs` | Mechanical |
| Update `snapshot.rs` `to_stats()` string keys to match new names | `snapshot.rs` | 7 snapshot types |

### Phase 4 — Polish (separate PRs, low urgency)

| Change | Files | Effort |
|---|---|---|
| Selectivity: remove `* 100.0`, record as 0.0–1.0 ratio | `filter.rs` (both paths), `snapshot.rs` | Trivial |
| Harden `normalize_name()` with character validation | `meters.rs` | Small |
| Rename scopes to `daft.distributed.execution` / `daft.local.execution` | `statistics/mod.rs`, `pipeline.rs` | 2 lines |

---

## Quick Reference: Before/After

### Attribute keys

| Before | After |
|---|---|
| `node_id` / `node_id_rnm` / `node_id_dfs` / `node_id_scan` / `node_id_limit` | `daft.operator.id` |
| `operator` (distributed DefaultRuntimeStats only) | `daft.operator.type` (everywhere) |
| *(missing)* | `daft.operator.category` (everywhere) |
| `query_id_stats` / `query_id` | `daft.query.id` |

### Metric names

| Before | After |
|---|---|
| `daft.cpu_us` | `daft.cpu.time` |
| `daft.rows_in` | `daft.rows.in` |
| `daft.rows_out` | `daft.rows.out` |
| `daft.rows_written` | `daft.rows.written` |
| `daft.bytes_read` | `daft.source.io` |
| `daft.bytes_written` | `daft.sink.io` |
| `active_tasks` (no prefix) | `daft.tasks.active` |
| `daft.completed_tasks` | `daft.tasks.completed` |
| `daft.failed_tasks` | `daft.tasks.failed` |
| `daft.cancelled_tasks` | `daft.tasks.cancelled` |
| `daft.selectivity` | `daft.filter.selectivity` |
| `daft.amplification` | `daft.explode.amplification` |

### Scope names

| Before | After |
|---|---|
| `daft.distributed.node_stats` | `daft.distributed.execution` |
| `daft.local.node_stats` | `daft.local.execution` |
