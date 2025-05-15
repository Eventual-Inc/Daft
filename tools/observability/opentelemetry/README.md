# OpenTelemetry

This directory contains the code for running the OpenTelemetry collector and exporter.

The docker compose file located in this directory will start the following services:

1. [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) - an agent for collecting and exporting opentelemetry data
2. [Jaeger](https://www.jaegertracing.io/) - an open source distributed tracing system
3. [Prometheus](https://prometheus.io/) - an open source metrics monitoring system

## Running the collector

To start the services, navigate to this directory and run the following command:

```bash
docker compose up
```

## Executing a job with opentelemetry enabled

The opentelemetry collector exposes an OTLP gRPC endpoint on port 4317.
To execute a job with opentelemetry enabled, set the `DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT` environment variable to `grpc://localhost:4317` and run the job.

Example:
```bash
DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT=grpc://localhost:4317 python tools/observability/opentelemetry/example.py
```

## Viewing the metrics

### Traces (Jaeger)

To view the traces on the Jaeger UI, navigate to `http://localhost:16686/` and select the `daft` service from the dropdown menu.

### Metrics (Prometheus)

To view the metrics on the Prometheus UI, navigate to `http://localhost:9090/`.

To query for all metrics provided by Daft, try the following [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/) query:

```promql
{service_name='daft'}
```
