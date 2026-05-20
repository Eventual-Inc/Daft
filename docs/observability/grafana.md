# Grafana Dashboard

Daft ships an importable Grafana dashboard for visualizing query execution metrics alongside the rest of your infrastructure observability. The dashboard reads Daft's OTel-exported metrics via Prometheus and renders five panels covering throughput, bytes flow, task lifecycle, operator hot spots, and failed-task counts.

This is a complement to the [in-process Daft Dashboard](dashboard.md), not a replacement. The in-process dashboard is the front door for live per-query debugging — plan tree, tasks view, heatmap, event log replay. The Grafana dashboard is the on-call surface — fleet-level metrics, time-series, alerting, SLO tracking — aggregated alongside your other services.


## Prerequisites

Three pieces of infrastructure need to be wired up before any panel will render data:

1. **Daft with OTel export enabled.** Telemetry is off by default. Set an OTLP endpoint when launching your script. See [Telemetry](telemetry.md) for the full environment variable matrix.
   ```bash
   export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
   python my-daft-script.py
   ```
2. **A Prometheus instance scraping Daft's OTel metrics.** Either through the OTel Collector's Prometheus exporter, or by pointing Prometheus directly at the OTel `/metrics` endpoint.
3. **A Grafana instance with the Prometheus datasource configured.** The dashboard uses a `${DS_PROMETHEUS}` datasource variable so it imports cleanly against any Prometheus datasource.

!!! info
    The dashboard is designed for Grafana 10+ (schema version 39). Earlier Grafana versions may need adjustments to panel configurations.


## Importing the dashboard

Download [daft-dashboard.json](grafana/daft-dashboard.json) and import via the Grafana UI or API.

**Via the Grafana UI:**

1. Navigate to **Dashboards** → **New** → **Import**
2. Upload `daft-dashboard.json` or paste its contents into the import dialog
3. Select your Prometheus datasource when prompted
4. Click **Import**

**Via the Grafana HTTP API:**

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $GRAFANA_API_TOKEN" \
  -d @daft-dashboard.json \
  https://your-grafana-host/api/dashboards/db
```


## Panels

| Panel | What it shows |
| --- | --- |
| **Rows/sec by operator type** | Rate of rows emitted per operator type. Spikes show heavy producers; flat lines after a high rate often indicate a downstream bottleneck. |
| **Bytes flow — read vs written** | Bytes read from sources vs bytes written to sinks. The gap between the two is approximately the work the engine is doing in-memory. |
| **Task lifecycle** | Active, completed, failed, and cancelled task counters. Failed counter is highlighted in red when non-zero. |
| **Top 10 operators by cumulative duration** | Operators ranked by accumulated CPU time. Use this to find hot spots, then cross-reference with the [in-process dashboard's](dashboard.md) heatmap for the plan context. |
| **Failed tasks** | Single-stat panel with a red threshold above zero. The quick "is anything broken?" view for on-call dashboards. |


## Metric naming convention

Daft emits OTel metrics with dot-separated names. The Prometheus convention converts dots to underscores and adds `_total` to counters — the OTel Collector's Prometheus exporter handles this automatically. The dashboard's queries assume this convention.

| OTel name | Prometheus query name | Type |
| --- | --- | --- |
| `daft.rows.in` | `daft_rows_in_total` | counter |
| `daft.rows.out` | `daft_rows_out_total` | counter |
| `daft.bytes.read` | `daft_bytes_read_total` | counter |
| `daft.bytes.written` | `daft_bytes_written_total` | counter |
| `daft.duration` | `daft_duration_total` | counter (µs) |
| `daft.task.active` | `daft_task_active` | gauge |
| `daft.task.completed` | `daft_task_completed_total` | counter |
| `daft.task.failed` | `daft_task_failed_total` | counter |
| `daft.task.cancelled` | `daft_task_cancelled_total` | counter |

Labels: `node.id` and `node.type` become `node_id` and `node_type` in Prometheus queries.

!!! info
    Several metrics are emitted only in distributed (Ray) execution: `daft_bytes_read_total`, `daft_task_active`, `daft_task_cancelled_total`, `daft_task_completed_total`, `daft_task_failed_total`. Native-runner-only workloads will see empty panels for those — the rows, bytes-written, and duration panels work in both modes.


## What's not yet covered

The current dashboard focuses on metrics that are documented and stable as of Daft v0.7.x. Several additional metrics are landing in the observability work — when their Prometheus exposure stabilizes, panels can be added:

- **Process-level memory and CPU.** Per-process memory and CPU stats land as OTel metrics in distributed execution. Once the exposed metric names are confirmed against a real scrape, a "process resources" panel row makes sense.
- **Per-operator memory attribution.** `bytes_in` / `bytes_out` and inflation/deflation ratios are exposed in the in-process dashboard. Verifying their Prometheus name shape will let those panels land in Grafana too.
- **Peak resident state for stateful operators.** Coming in [#6883](https://github.com/Eventual-Inc/Daft/pull/6883). Will need a dedicated panel for accumulating-operator memory.

Panel additions are mechanical once the metric names are confirmed — same query shape, different metric name.


## Adapting the dashboard

The dashboard is a starting point. Real production setups often want:

- **Per-job filtering.** Add a `job_id` or `query_id` template variable if you're labeling metrics with one.
- **Aggregation windows.** The default `rate([1m])` is fine for interactive debugging; longer windows (`[5m]`, `[15m]`) give cleaner trend lines for SLO dashboards.
- **Alerts.** The `Failed tasks` panel is a natural alerting hook — set `sum(daft_task_failed_total) > 0` as a trigger.
- **Cluster topology.** If you're running multiple Daft jobs on shared infrastructure, group panels by `cluster` or `service` labels that your OTel collector adds.

Submit improvements via PRs — the dashboard belongs to the community.
