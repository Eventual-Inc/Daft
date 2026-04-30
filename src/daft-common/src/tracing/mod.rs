mod config;

use std::{
    sync::{LazyLock, Mutex},
    time::Duration,
};

use crate::runtime::get_io_runtime;
pub use config::Config;
use opentelemetry::{global, trace::TracerProvider};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    metrics::PeriodicReader,
    trace::{Sampler, SdkTracerProvider},
};
use tracing_subscriber::{
    EnvFilter, Registry, filter::LevelFilter, fmt::format::FmtSpan, layer::SubscriberExt,
    prelude::*,
};

static GLOBAL_TRACER_PROVIDER: LazyLock<Mutex<Option<SdkTracerProvider>>> =
    LazyLock::new(|| Mutex::new(None));

static GLOBAL_METER_PROVIDER: LazyLock<
    Mutex<Option<opentelemetry_sdk::metrics::SdkMeterProvider>>,
> = LazyLock::new(|| Mutex::new(None));

pub static GLOBAL_LOGGER_PROVIDER: LazyLock<Mutex<Option<SdkLoggerProvider>>> =
    LazyLock::new(|| Mutex::new(None));

#[derive(Debug, Clone, Copy)]
enum TraceFormat {
    Compact,
    Pretty,
    Json,
}

impl TraceFormat {
    fn from_env() -> Self {
        match std::env::var("DAFT_TRACE_FORMAT")
            .unwrap_or_else(|_| "compact".to_string())
            .to_lowercase()
            .as_str()
        {
            "pretty" => Self::Pretty,
            "json" => Self::Json,
            _ => Self::Compact,
        }
    }
}

pub fn init_tracing() {
    let config = Config::from_env();
    let resource = Resource::builder().with_service_name("daft").build();
    let tracer_provider = if config.enabled() {
        let runtime = get_io_runtime(true);
        runtime.block_on_current_thread(async {
            let tracer_provider = config
                .traces_endpoint()
                .map(|endpoint| init_otlp_tracer_provider(&config, endpoint, resource.clone()));

            if let Some(endpoint) = config.metrics_endpoint() {
                init_otlp_metrics_provider(&config, endpoint, resource.clone());
            }
            if let Some(endpoint) = config.logs_endpoint() {
                init_otlp_logger_provider(&config, endpoint, resource.clone());
            }

            tracer_provider
        })
    } else {
        None
    };

    let mut layers: Vec<Box<dyn tracing_subscriber::Layer<Registry> + Send + Sync>> = Vec::new();

    if let Ok(daft_trace) = std::env::var("DAFT_TRACE") {
        // A bare level enables Daft-owned targets only. Users can opt into additional
        // crates by passing an explicit EnvFilter directive, e.g.
        // `DAFT_TRACE=daft_=info,Daft=info,hyper=debug`.
        let filter_directive = if daft_trace.parse::<LevelFilter>().is_ok() {
            format!("daft_={daft_trace},Daft={daft_trace}")
        } else {
            daft_trace.clone()
        };
        match EnvFilter::try_new(&filter_directive) {
            Ok(filter) => {
                let layer = match TraceFormat::from_env() {
                    TraceFormat::Compact => tracing_subscriber::fmt::layer()
                        .compact()
                        .with_target(true)
                        .with_writer(std::io::stderr)
                        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                        .boxed(),
                    TraceFormat::Pretty => tracing_subscriber::fmt::layer()
                        .pretty()
                        .with_target(true)
                        .with_writer(std::io::stderr)
                        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                        .boxed(),
                    TraceFormat::Json => tracing_subscriber::fmt::layer()
                        .json()
                        .with_target(true)
                        .with_current_span(true)
                        .with_span_list(true)
                        .with_writer(std::io::stderr)
                        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                        .boxed(),
                };
                layers.push(Box::new(layer.with_filter(filter)));
            }
            Err(err) => {
                eprintln!(
                    "DAFT_TRACE: invalid filter {daft_trace:?}, console tracing disabled: {err}. \
                     Use a bare level like `info` for Daft logs only, or an explicit directive \
                     like `daft_=info,Daft=info,hyper=debug` to include other crates."
                );
            }
        }
    }

    if let Some(tracer_provider) = tracer_provider {
        layers.push(Box::new(
            tracing_opentelemetry::layer()
                .with_tracer(tracer_provider.tracer("daft-otel-tracer"))
                .with_filter(LevelFilter::INFO),
        ));
    }

    if !layers.is_empty() {
        let subscriber = tracing_subscriber::registry().with(layers);
        if let Err(err) = tracing::subscriber::set_global_default(subscriber) {
            eprintln!(
                "Daft could not install its tracing subscriber; OTLP trace export and Daft tracing output are disabled: {err}"
            );
        }
    }
}

pub fn flush_opentelemetry_providers() {
    flush_oltp_tracer_provider();
    flush_oltp_metrics_provider();
    flush_oltp_logger_provider();
}

fn init_otlp_logger_provider(config: &Config, otlp_endpoint: &str, resource: Resource) {
    let mut lg = GLOBAL_LOGGER_PROVIDER.lock().unwrap();
    assert!(lg.is_none(), "Expected logger provider to be None on init");

    if config.otlp_protocol != Protocol::Grpc {
        log::warn!(
            "`http/json` or `http/protobuf` protocol is currently not supported for the OTEL logs exporter. gRPC will be used instead."
        );
    }

    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build OTLP logger exporter.");

    let logger_provider: SdkLoggerProvider = SdkLoggerProvider::builder()
        .with_batch_exporter(log_exporter)
        .with_resource(resource)
        .build();

    *lg = Some(logger_provider);
}

pub fn flush_oltp_logger_provider() {
    let lg = GLOBAL_LOGGER_PROVIDER.lock().unwrap();
    if let Some(logger_provider) = lg.as_ref()
        && let Err(e) = logger_provider.force_flush()
    {
        eprintln!("Failed to flush OTLP logger provider: {}", e);
    }
}

fn init_otlp_metrics_provider(config: &Config, endpoint: &str, resource: Resource) {
    let mut mg = GLOBAL_METER_PROVIDER.lock().unwrap();
    assert!(mg.is_none(), "Expected meter provider to be None on init");

    let metrics_exporter = match config.otlp_protocol {
        Protocol::Grpc => {
            opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(10))
            .build()
        }
        Protocol::HttpBinary => {
            opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(10))
            .with_protocol(config.otlp_protocol)
            .build()
        }
        Protocol::HttpJson => {
            // TODO: Support by enabling the `http/json` feature of the opentelemetry-otlp crate
            panic!("`http/json` protocol is currently not supported for metrics exporter. Set `OTEL_EXPORTER_OTLP_PROTOCOL` to `grpc` or `http/protobuf` instead");
        }
    }.expect("Failed to build OTLP metric exporter for tracing");

    let metrics_reader = PeriodicReader::builder(metrics_exporter)
        .with_interval(config.metrics_export_interval())
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
        eprintln!("Failed to flush OTLP metrics provider: {}", e);
    }
}

fn init_otlp_tracer_provider(
    config: &Config,
    otlp_endpoint: &str,
    resource: Resource,
) -> SdkTracerProvider {
    if config.otlp_protocol != Protocol::Grpc {
        log::warn!(
            "`http/json` or `http/protobuf` protocol is currently not supported for the OTEL traces exporter. gRPC will be used instead."
        );
    }

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build OTLP span exporter for tracing");

    let tracer_provider: SdkTracerProvider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .with_sampler(Sampler::AlwaysOn)
        .build();

    {
        let mut mg = GLOBAL_TRACER_PROVIDER.lock().unwrap();
        assert!(mg.is_none(), "Expected tracer provider to be None on init");
        *mg = Some(tracer_provider.clone());
    }

    tracer_provider
}

fn flush_oltp_tracer_provider() {
    let mg = GLOBAL_TRACER_PROVIDER.lock().unwrap();
    if let Some(tracer_provider) = mg.as_ref()
        && let Err(e) = tracer_provider.force_flush()
    {
        eprintln!("Failed to flush OTLP tracer provider: {}", e);
    }
}
