# Daft OTel Metrics Summary

Includes metrics emitted through the OTel paths, including distributed task/runtime metrics touched in PR `#6148`.

## Current Existing Metrics

| Metric name | Description | Unit | Instrumentation scope |
|---|---|---|---|
| `daft.cpu_us` | CPU time accumulated by a node/operator. | `microseconds` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.rows_in` | Input rows consumed by a node/operator. | `rows` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.rows_out` | Output rows produced by a node/operator. | `rows` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.bytes_read` | Bytes read by source/scan nodes. | `bytes` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.rows_written` | Rows written by sink/write nodes. | `rows` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.bytes_written` | Bytes written by sink/write nodes. | `bytes` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.selectivity` | Filter selectivity (`rows_out / rows_in * 100`). | `percent` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.amplification` | Explode amplification (`rows_out / rows_in`). | `ratio` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.distributed.node_stats.active_tasks` | Current in-flight task count per distributed node (up/down counter). | `tasks` | `daft.distributed.node_stats` |
| `daft.distributed.node_stats.completed_tasks` | Completed task count per distributed node. | `tasks` | `daft.distributed.node_stats` |
| `daft.distributed.node_stats.failed_tasks` | Failed task count per distributed node. | `tasks` | `daft.distributed.node_stats` |
| `daft.distributed.node_stats.cancelled_tasks` | Cancelled task count per distributed node. | `tasks` | `daft.distributed.node_stats` |
| `daft.distributed.node_stats.completed_rows_in` | Rows-in from completed tasks (distributed aggregate). | `rows` | `daft.distributed.node_stats` |
| `daft.distributed.node_stats.completed_rows_out` | Rows-out from completed tasks (distributed aggregate). | `rows` | `daft.distributed.node_stats` |
| `daft.distributed.node_stats.completed_cpu_us` | CPU time from completed tasks (distributed aggregate). | `microseconds` | `daft.distributed.node_stats` |
| `daft.<custom_udf_counter>` | User-defined UDF counter via `daft.udf.metrics.increment_counter(...)`; name is normalized to lowercase with spaces replaced by `_`. | `count` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.input_tokens` | AI helper counter for input tokens (emitted when used in UDF metrics context). | `tokens` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.output_tokens` | AI helper counter for output tokens (emitted when used in UDF metrics context). | `tokens` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.total_tokens` | AI helper counter for total tokens (emitted when used in UDF metrics context). | `tokens` | `daft.local.node_stats`, `daft.distributed.node_stats` |
| `daft.requests` | AI helper counter for request count (emitted when used in UDF metrics context). | `requests` | `daft.local.node_stats`, `daft.distributed.node_stats` |

## Recommended Renames

| Old metric | Recommended metric | Scope note |
|---|---|---|
| `daft.cpu_us` | `daft.operator.cpu.time` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.rows_in` | `daft.operator.input.rows` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.rows_out` | `daft.operator.output.rows` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.bytes_read` | `daft.operator.input.bytes` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.rows_written` | `daft.operator.output.rows_written` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.bytes_written` | `daft.operator.output.bytes` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.selectivity` | `daft.operator.filter.selectivity` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.amplification` | `daft.operator.explode.amplification` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.distributed.node_stats.active_tasks` | `daft.distributed.worker.tasks.active` | distributed-only runtime task metric |
| `daft.distributed.node_stats.completed_tasks` | `daft.distributed.worker.tasks.completed` | distributed-only runtime task metric |
| `daft.distributed.node_stats.failed_tasks` | `daft.distributed.worker.tasks.failed` | distributed-only runtime task metric |
| `daft.distributed.node_stats.cancelled_tasks` | `daft.distributed.worker.tasks.cancelled` | distributed-only runtime task metric |
| `daft.distributed.node_stats.completed_rows_in` | `daft.distributed.worker.completed.input.rows` | distributed-only runtime task metric |
| `daft.distributed.node_stats.completed_rows_out` | `daft.distributed.worker.completed.output.rows` | distributed-only runtime task metric |
| `daft.distributed.node_stats.completed_cpu_us` | `daft.distributed.worker.completed.cpu.time` | distributed-only runtime task metric |
| `daft.<custom_udf_counter>` | `daft.udf.<custom_counter_name>` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.input_tokens` | `daft.ai.tokens.input` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.output_tokens` | `daft.ai.tokens.output` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.total_tokens` | `daft.ai.tokens.total` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
| `daft.requests` | `daft.ai.requests` | applies in both `daft.local.node_stats` and `daft.distributed.node_stats` scopes |
