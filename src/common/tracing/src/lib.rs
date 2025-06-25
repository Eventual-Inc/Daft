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

    let metrics_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(metrics_exporter) // To customize the export interval, set the **"OTEL_METRIC_EXPORT_INTERVAL"** environment variable (in milliseconds).
        .with_resource(resource)
        .build();

    global::set_meter_provider(metrics_provider.clone());

    *mg = Some(metrics_provider);
}

pub fn flush_oltp_metrics_provider() {
    let mg = GLOBAL_METER_PROVIDER.lock().unwrap();
    if let Some(meter_provider) = mg.as_ref() {
        if let Err(e) = meter_provider.force_flush() {
            println!("Failed to flush OTLP metrics provider: {}", e);
        }
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
    if let Some(tracer_provider) = mg.as_ref() {
        if let Err(e) = tracer_provider.force_flush() {
            println!("Failed to flush OTLP tracer provider: {}", e);
        }
    }
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
        // The initial writer to the chrome trace is a no-op sink, so we don't write anything
        // only on calls to start_chrome_trace() do we write traces.
        .writer(std::io::sink())
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

pub fn start_chrome_trace() -> bool {
    let mut mg = CHROME_GUARD_HANDLE.lock().unwrap();
    if let Some(fg) = mg.as_mut() {
        // start_new(None) will let tracing-chrome choose the file and file name.
        fg.start_new(None);
        true
    } else {
        false
    }
}

pub fn finish_chrome_trace() -> bool {
    let mut mg = CHROME_GUARD_HANDLE.lock().unwrap();
    if let Some(fg) = mg.as_mut() {
        // start_new(Some(Box::new(std::io::sink()))) will flush the current trace, and start a new one with a dummy writer.
        // The flush method doesn't actually close the file. The only way to do it is to drop the guard or call 'start_new'.
        // But we can't drop the guard because it's a static and we may have multiple traces per process, so we need to call start_new.
        fg.start_new(Some(Box::new(std::io::sink())));
        true
    } else {
        false
    }
}
