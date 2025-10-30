use std::time::Duration;

use common_error::{DaftError, DaftResult};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;

pub fn init_otlp_meter_provider(
    otlp_endpoint: &str,
) -> DaftResult<opentelemetry_sdk::metrics::SdkMeterProvider> {
    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", "daft"))
        .build();

    let metrics_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| {
            DaftError::InternalError(format!(
                "Failed to build OTLP metric exporter for tracing: {}",
                e
            ))
        })?;

    // let stdout_exporter = opentelemetry_stdout::MetricExporter::builder()
    //     .with_temporality(Temporality::Cumulative)
    //     .build();

    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(metrics_exporter) // To customize the export interval, set the **"OTEL_METRIC_EXPORT_INTERVAL"** environment variable (in milliseconds).
        // .with_periodic_exporter(stdout_exporter)
        .with_resource(resource)
        .build();

    Ok(meter_provider)
}
