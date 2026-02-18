# Gemini CLI: OpenTelemetry Metrics Recommendations

This document outlines the final recommendations for establishing a robust, standards-compliant, and maintainable OpenTelemetry framework for the Gemini CLI. These recommendations are based on a comprehensive review of existing metrics, OpenTelemetry Semantic Conventions, and best practices.

## Final Recommendations

The goal is to transition from the current state of inconsistent metrics to a fully compliant and descriptive observability system. This can be achieved by focusing on three core pillars: achieving full semantic compliance, hardening the implementation, and executing a coordinated migration.

### 1. Achieve Full Semantic Compliance

The highest priority is to make all telemetry data "speak OTel fluently." This ensures that Gemini CLI's metrics are immediately understandable and interoperable with standard observability tools.

*   **Unify Naming and Attributes:**
    *   Standardize all operator identification under a single attribute: **`daft.operator.id`**.
    *   Standardize query identification under a single attribute: **`daft.query.id`**.
    *   Refactor all metric names to the `namespace.object.property` format, using plural forms for counters of items (e.g., `daft.cpu.time`, **`daft.rows.in`**).

*   **Enrich with Standard Metadata:**
    *   **Remove Units from Names:** Units must be provided as metadata.
    *   **Provide UCUM Units:** Use the standard case-sensitive UCUM strings for all instruments (e.g., `us` for microseconds, `By` for bytes, `{row}` for a count of rows).
    *   **Add Descriptions and Context:** Enrich all metrics with human-readable descriptions and consistently attach contextual attributes like `daft.operator.type` and `daft.operator.category`.

### 2. Harden the Instrumentation Framework

Correcting implementation bugs and standardizing the code that produces metrics will prevent future inconsistencies.

*   **Fix Implementation Bugs:**
    *   Resolve the `active_tasks` normalization bug so it is correctly named **`daft.tasks.active`**.
    *   Adjust the `daft.filter.selectivity` gauge to report a standard 0.0–1.0 ratio instead of a percentage.

*   **Standardize Scopes and Helpers:**
    *   Adopt a consistent instrumentation scope strategy. The recommended approach is to use a single **`daft.execution`** scope and add a **`daft.execution.engine`** attribute (`"local"` or `"distributed"`) to differentiate runtimes.
    *   Strengthen the metric helper functions in `meters.rs` to enforce naming conventions and require unit/description metadata, making it easier to create compliant metrics.

### 3. Execute a Coordinated and Phased Migration

The required changes are significant and will break existing dashboards and alerts. A careful rollout is essential to avoid disrupting users.

*   **Prioritize Critical Changes:** Address the unification of attributes (`daft.operator.id`, `daft.query.id`) first, as this unlocks the most value.
*   **Implement with Dual Emission:** For all breaking changes to metric or attribute names, temporarily emit both the old and new formats simultaneously.
*   **Communicate a Deprecation Window:** Clearly communicate the migration plan and provide a reasonable timeframe for users to update their dashboards before the legacy formats are removed.

### Target State: Core Metrics Summary

This table represents the ideal, compliant state for Daft's core execution metrics, with corrected pluralization.

| Metric Name                   | Instrument    | Unit     | Description                                    |
| ----------------------------- | ------------- | -------- | ---------------------------------------------- |
| `daft.cpu.time`               | Counter       | `us`     | Cumulative CPU time consumed by this operator  |
| **`daft.rows.in`**            | Counter       | `{row}`  | Number of rows received by this operator       |
| **`daft.rows.out`**           | Counter       | `{row}`  | Number of rows emitted by this operator        |
| `daft.byte.read`              | Counter       | `By`     | Number of bytes read from a source             |
| `daft.byte.written`           | Counter       | `By`     | Number of bytes written to a sink              |
| **`daft.tasks.active`**       | UpDownCounter | `{task}` | Number of currently executing tasks            |
| **`daft.tasks.completed`**    | Counter       | `{task}` | Number of tasks that completed successfully    |
| `daft.filter.selectivity`     | Gauge         | `1`      | Fraction of rows passing a filter (0.0–1.0)    |
| `daft.explode.amplification`  | Gauge         | `1`      | Row multiplication factor from an explode op   |
