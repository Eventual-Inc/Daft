# Daft OTEL Metrics Naming Review (Agent Handoff)

## Scope
This note summarizes recommendations discussed for OpenTelemetry metric scope names, metric names, and attributes across local and distributed execution paths.

## Key Decisions
1. Use one shared instrumentation scope for execution metrics: `daft.execution.statistics`.
2. Do not split instrumentation scope by runtime (`daft.local.node_stats`, `daft.distributed.node_stats`), because scope should represent instrumentation/library identity, not runtime dimensions.
3. Put runtime/query/operator context in attributes, not in scope name.
4. Prefer operator-centric telemetry keys over ambiguous bare node keys.
5. Use namespaced custom attributes to avoid collisions with standard OTEL infra semantics.

## Recommended Attribute Model
1. `daft.execution.engine = local|distributed`
2. `daft.query.id`
3. `daft.operator.id`
4. `daft.operator.parent_id` (when local work is emitted on behalf of a distributed operator)
5. `daft.operator.type`
6. `daft.operator.name`
7. `daft.operator.category`
8. Optional for targeted diagnostics only: `daft.stage.id`, `daft.task.id`

## Why `operator` over `node`
1. In OTEL ecosystems, `node` is commonly used for infrastructure entities (for example Kubernetes node/host concepts).
2. Using `operator` for query-plan execution elements reduces ambiguity in dashboards and queries.
3. If internal structs still use `NodeInfo`, map those fields to `daft.operator.*` at emission boundaries.

## Cardinality Guidance
1. Keep always-on dimensions low-cardinality (`engine`, `operator.type`, `operator.category`).
2. Treat high-cardinality IDs (`query.id`, `task.id`) with care.
3. Consider gating/sampling high-cardinality dimensions in production if backend cost becomes an issue.

## Metric Naming Guidance
1. Keep a consistent canonical naming scheme such as `daft.<domain>.<metric>`.
2. Keep units in OTEL instrument unit metadata (where supported), not embedded in metric names.
3. Normalize names through shared helpers so all instruments (including direct OTel instruments) follow the same conventions.

## Inconsistencies Found (to be cleaned up)
1. Query attribute key mismatch (`query_id_stats` vs `query_id`).
2. Node attribute key variants (`node_id`, `node_id_scan`, `node_id_limit`, `node_id_rnm`, `node_id_dfs`).
3. Metric key spelling mismatch (`bytes read` vs `bytes_read`).
4. Mixed legacy names containing spaces and unit-in-name patterns (`rows in`, `rows out`, `cpu us`).
5. One direct instrument bypassing shared naming helper (`active_tasks`).

## Relevant Files
1. `src/daft-distributed/src/statistics/mod.rs`
2. `src/daft-distributed/src/statistics/stats.rs`
3. `src/daft-local-execution/src/pipeline.rs`
4. `src/common/metrics/src/meters.rs`
5. `src/common/metrics/src/lib.rs`
6. `src/common/metrics/src/snapshot.rs`
7. `src/common/metrics/src/operator_metrics.rs`
8. `src/common/tracing/src/lib.rs`
9. `src/common/tracing/src/config.rs`
10. `src/daft-distributed/src/pipeline_node/scan_source.rs`
11. `src/daft-distributed/src/pipeline_node/limit.rs`
12. `src/daft-distributed/src/pipeline_node/sink.rs`
13. `src/daft-distributed/src/pipeline_node/filter.rs`
14. `src/daft-distributed/src/pipeline_node/explode.rs`
15. `src/daft-distributed/src/pipeline_node/udf.rs`
16. `src/daft-local-execution/src/intermediate_ops/filter.rs`
17. `src/daft-local-execution/src/intermediate_ops/udf.rs`
18. `src/daft-local-execution/src/sources/source.rs`
19. `src/daft-local-execution/src/sinks/write.rs`
20. `src/parquet2/src/statistics/mod.rs` (reviewed for request context; does not contain `StatisticsManager`)

## Notes for Implementers
1. This review did not apply code changes to telemetry behavior.
2. For migration safety, consider temporary dual-emission (old + new keys/names), then remove legacy fields after dashboard/alert updates.
