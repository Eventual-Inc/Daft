# Telemetry

Daft exports [OpenTelemetry](https://opentelemetry.io/) (OTLP) metrics for query execution. Export is off by default and is enabled by setting an OTLP endpoint environment variable. Metrics can be sent to any OTLP-compatible backend, such as Prometheus with OTLP receiver enabled.


## Configuration

Set OTLP environment variables to configure telemetry.
Daft enables OpenTelemetry when any OTLP endpoint is set.

### Examples

Configure telemetry with environment variables:

```bash
# 1) Set an OTLP endpoint (enables telemetry)
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317

# 2) Run your Daft pipeline
python my-daft-script.py
```

For metrics-only export to Prometheus (with the OTLP receiver enabled):

```bash
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://localhost:9090/api/v1/otlp/v1/metrics
python my-daft-script.py
```

### Configuration details

| Variable | Description | Default | Allowed values |
| --- | --- | --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Base OTLP endpoint used for metrics/logs/traces unless signal-specific endpoints are set. | Unset | OTLP endpoint URL, for example `http://localhost:4317` |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | Metrics-only OTLP endpoint. Overrides `OTEL_EXPORTER_OTLP_ENDPOINT` for metrics. | Unset | OTLP endpoint URL |
| `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` | Logs-only OTLP endpoint. Overrides `OTEL_EXPORTER_OTLP_ENDPOINT` for logs. | Unset | OTLP endpoint URL |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Traces-only OTLP endpoint. Overrides `OTEL_EXPORTER_OTLP_ENDPOINT` for traces. | Unset | OTLP endpoint URL |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | OTLP exporter protocol. | `grpc` | `grpc`, `http/protobuf`, `http/json` |
| `OTEL_METRIC_EXPORT_INTERVAL` | Metrics export interval in milliseconds. | `500` | Positive integer (milliseconds) |
| `DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT` | Deprecated alias for `OTEL_EXPORTER_OTLP_ENDPOINT`. | Unset | OTLP endpoint URL |

!!! info
    `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`, and `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` take precedence over `OTEL_EXPORTER_OTLP_ENDPOINT` for their respective signal.

!!! warning
    For metrics, `http/protobuf` is supported and `http/json` is not currently supported.
    For logs and traces, Daft currently exports via gRPC.
    Use HTTP-style endpoint URLs (`http://` or `https://`) for OTLP exporters; protocol is controlled by `OTEL_EXPORTER_OTLP_PROTOCOL`.

## Metrics

Daft exports the following metrics.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `daft.bytes.read` | Counter | `node.id`, `node.type` | Total bytes read by source/scan operators. |
| `daft.bytes.written` | Counter | `node.id`, `node.type` | Total bytes written by write sinks. |
| `daft.duration` | Counter | `node.id`, `node.type` | Accumulated operator CPU time in microseconds. |
| `daft.rows.in` | Counter | `node.id`, `node.type` | Total rows consumed by an operator. |
| `daft.rows.out` | Counter | `node.id`, `node.type` | Total rows emitted by an operator. |
| `daft.rows.written` | Counter | `node.id`, `node.type` | Total rows written by write sinks. |
| `daft.task.active` | UpDownCounter | `node.id`, `node.type` | Number of currently active tasks. |
| `daft.task.cancelled` | Counter | `node.id`, `node.type` | Total cancelled tasks. |
| `daft.task.completed` | Counter | `node.id`, `node.type` | Total completed tasks. |
| `daft.task.failed` | Counter | `node.id`, `node.type` | Total failed tasks. |

!!! info
    The following metrics are emitted only in distributed (Ray) execution: `daft.bytes.read`, `daft.task.active`, `daft.task.cancelled`, `daft.task.completed`, `daft.task.failed`.
