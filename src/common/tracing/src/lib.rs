use std::{
    sync::{LazyLock, Mutex},
    time::Duration,
};

use common_runtime::get_io_runtime;
use opentelemetry::{KeyValue, global, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    metrics::PeriodicReader,
    trace::{Sampler, SdkTracerProvider},
};
use tracing_subscriber::{layer::SubscriberExt, prelude::*};

static GLOBAL_TRACER_PROVIDER: LazyLock<
    Mutex<Option<opentelemetry_sdk::trace::SdkTracerProvider>>,
> = LazyLock::new(|| Mutex::new(None));

static GLOBAL_METER_PROVIDER: LazyLock<
    Mutex<Option<opentelemetry_sdk::metrics::SdkMeterProvider>>,
> = LazyLock::new(|| Mutex::new(None));

const OTEL_EXPORTER_OTLP_ENDPOINT: &str = "DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT";

pub fn should_enable_opentelemetry() -> bool {
    std::env::var(OTEL_EXPORTER_OTLP_ENDPOINT).is_ok()
}

pub fn init_opentelemetry_providers() {
    if !should_enable_opentelemetry() {
        return;
    }

    let otlp_endpoint = match std::env::var(OTEL_EXPORTER_OTLP_ENDPOINT) {
        Ok(endpoint) => endpoint,
        Err(_) => return,
    };

    let ioruntime = get_io_runtime(true);
    ioruntime.block_on_current_thread(async {
        init_otlp_metrics_provider(&otlp_endpoint).await;
        init_otlp_tracer_provider(&otlp_endpoint).await;
    });
}

pub fn flush_opentelemetry_providers() {
    flush_oltp_tracer_provider();
}

async fn init_otlp_metrics_provider(otlp_endpoint: &str) {
    let mut mg = GLOBAL_METER_PROVIDER.lock().unwrap();
    assert!(mg.is_none(), "Expected meter provider to be None on init");

    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", "daft"))
        .build();

    let metrics_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build OTLP metric exporter for tracing");

    let metrics_reader = PeriodicReader::builder(metrics_exporter)
        .with_interval(Duration::from_millis(500))
        .build();

    let metrics_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(metrics_reader)
        .with_resource(resource)
        .build();

    global::set_meter_provider(metrics_provider.clone());

    *mg = Some(metrics_provider);
}

pub fn flush_oltp_metrics_provider() {
    let mg = GLOBAL_METER_PROVIDER.lock().unwrap();
    if let Some(meter_provider) = mg.as_ref()
        && let Err(e) = meter_provider.force_flush()
    {
        println!("Failed to flush OTLP metrics provider: {}", e);
    }
}

async fn init_otlp_tracer_provider(otlp_endpoint: &str) {
    let mut mg = GLOBAL_TRACER_PROVIDER.lock().unwrap();
    assert!(mg.is_none(), "Expected tracer provider to be None on init");

    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", "daft"))
        .build();

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build OTLP span exporter for tracing");

    let tracer_provider: SdkTracerProvider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .with_sampler(Sampler::AlwaysOn)
        .build();

    let tracer = tracer_provider.tracer("daft-otel-tracer");
    let telemetry_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(tracing::level_filters::LevelFilter::INFO);

    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(telemetry_layer))
        .unwrap();

    *mg = Some(tracer_provider);
}

fn flush_oltp_tracer_provider() {
    let mg = GLOBAL_TRACER_PROVIDER.lock().unwrap();
    if let Some(tracer_provider) = mg.as_ref()
        && let Err(e) = tracer_provider.force_flush()
    {
        println!("Failed to flush OTLP tracer provider: {}", e);
    }
}
