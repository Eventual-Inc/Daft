# Codex OTEL Metrics Recommendations

## Final Decisions

1. Canonical scope:
   - Preferred: `daft.execution.statistics`
   - Valid alternative: `daft.execution.local` + `daft.execution.distributed`
   - Avoid new `*.node_stats` scope names.

2. Canonical attributes:
   - `daft.execution.engine`
   - `daft.query.id` (configurable if cardinality/cost concerns)
   - `daft.operator.id`
   - `daft.operator.parent_id` (for local metrics emitted on behalf of distributed operators)
   - `daft.operator.type`
   - `daft.operator.category`
   - Optional/debug: `daft.stage.id`, `daft.task.id`

3. Canonical metric names:
   - `daft.operator.cpu.time`
   - `daft.operator.rows.input`
   - `daft.operator.rows.output`
   - `daft.operator.bytes.read`
   - `daft.operator.bytes.written`
   - `daft.operator.tasks.active`
   - `daft.operator.tasks.completed`
   - `daft.operator.tasks.failed`
   - `daft.operator.tasks.cancelled`
   - `daft.operator.filter.selectivity`
   - `daft.operator.explode.amplification`

4. Units:
   - CPU time: `us` (or `s`, choose one globally)
   - Rows: `{row}`
   - Bytes: `By`
   - Tasks: `{task}`
   - Ratios (`selectivity`, `amplification`): `1`

5. Semantics:
   - `selectivity` should be recorded as `0..1` ratio (not `0..100` percent).
   - Keep percent conversion in presentation layers only.

6. Priority fixes:
   - Unify `query_id_stats` / `query_id` -> `daft.query.id`
   - Unify `node_id*` -> `daft.operator.id`
   - Add `daft.operator.parent_id` linkage for distributed->local attribution
   - Normalize `active_tasks` naming path to match the shared naming helper

7. Migration:
   - Dual-emit old and new metrics/attributes for one deprecation window.
   - Update dashboards/alerts during overlap.
   - Remove legacy schema after cutover.

## Related Review Docs

1. `docs/otel-metrics-review.md`
2. `docs/otel-metrics-review-for-agents.md`
