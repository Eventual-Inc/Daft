use std::sync::{atomic::AtomicBool, Mutex};

use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::{layer::SubscriberExt, prelude::*};
static TRACING_INIT: AtomicBool = AtomicBool::new(false);
use std::{sync::LazyLock, time::Duration};

use common_runtime::get_io_runtime;
use opentelemetry::{global, trace::TracerProvider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace::{Sampler, SdkTracerProvider},
    Resource,
};

static CHROME_GUARD_HANDLE: LazyLock<Mutex<Option<tracing_chrome::FlushGuard>>> =
    LazyLock::new(|| Mutex::new(None));

pub fn init_otlp_metrics_provider() -> Option<opentelemetry_sdk::metrics::SdkMeterProvider> {
    let otlp_endpoint = match std::env::var("DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT") {
        Ok(endpoint) => endpoint,
        Err(_) => return None,
    };

    let ioruntime = get_io_runtime(true);
    ioruntime.block_on_current_thread(async {
        let resource = Resource::builder()
            .with_attribute(KeyValue::new("service.name", "daft"))
            .build();

        let metrics_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(otlp_endpoint)
            .with_timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to build OTLP metric exporter for tracing");

        let metrics_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_periodic_exporter(metrics_exporter) // To customize the export interval, set the **"OTEL_METRIC_EXPORT_INTERVAL"** environment variable (in milliseconds).
            .with_resource(resource)
            .build();

        global::set_meter_provider(metrics_provider.clone());

        Some(metrics_provider)
    })
}

pub fn init_otlp_trace_provider() -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
    let otlp_endpoint = match std::env::var("DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT") {
        Ok(endpoint) => endpoint,
        Err(_) => return None,
    };

    let ioruntime = get_io_runtime(true);
    ioruntime.block_on_current_thread(async {
        let resource = Resource::builder()
            .with_attribute(KeyValue::new("service.name", "daft"))
            .build();

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(otlp_endpoint)
            .with_timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to build OTLP span exporter for tracing");

        let tracer_provider: SdkTracerProvider =
            opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource)
                .with_sampler(Sampler::AlwaysOn)
                .build();

        let tracer = tracer_provider.tracer("daft-otel-tracer");
        let telemetry_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(tracing::level_filters::LevelFilter::INFO);

        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(telemetry_layer),
        )
        .unwrap();

        Some(tracer_provider)
    })
}

pub fn init_tracing(enable_chrome_trace: bool) {
    use std::sync::atomic::Ordering;

    assert!(
        !TRACING_INIT.swap(true, Ordering::Relaxed),
        "Cannot init tracing, already initialized!"
    );

    if !enable_chrome_trace {
        return; // Do nothing for now
    }

    let mut mg = CHROME_GUARD_HANDLE.lock().unwrap();
    assert!(
        mg.is_none(),
        "Expected chrome flush guard to be None on init"
    );

    let (chrome_layer, guard) = ChromeLayerBuilder::new()
        .trace_style(tracing_chrome::TraceStyle::Threaded)
        .name_fn(Box::new(|event_or_span| {
            match event_or_span {
                tracing_chrome::EventOrSpan::Event(ev) => ev.metadata().name().into(),
                tracing_chrome::EventOrSpan::Span(s) => {
                    // TODO: this is where we should extract out fields (such as node id to show the different pipelines)
                    s.name().into()
                }
            }
        }))
        .build();

    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(chrome_layer))
        .unwrap();

    *mg = Some(guard);
}

pub fn refresh_chrome_trace() -> bool {
    let mut mg = CHROME_GUARD_HANDLE.lock().unwrap();
    if let Some(fg) = mg.as_mut() {
        fg.start_new(None);
        true
    } else {
        false
    }
}
