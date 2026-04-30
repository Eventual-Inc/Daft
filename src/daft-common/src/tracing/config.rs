use std::time::Duration;

// These match the OTEL_EXPORTER_OTLP_* environment variables from:
// https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/#endpoint-configuration
use opentelemetry_otlp::{
    OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, Protocol,
};

/// Environment variable for the general OTLP endpoint.
pub const ENV_OLD_OTLP_ENDPOINT: &str = "DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT";
/// Environment variable for metrics export interval in milliseconds.
/// Mimics the official env since it's not exported out
pub const ENV_METRICS_EXPORT_INTERVAL_MS: &str = "OTEL_METRIC_EXPORT_INTERVAL";

fn parse_protocol(s: &str) -> Protocol {
    match s {
        "grpc" => Protocol::Grpc,
        "http/json" => Protocol::HttpJson,
        "http/protobuf" => Protocol::HttpBinary,
        _ => Protocol::Grpc,
    }
}

/// OpenTelemetry/OTLP configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    /// General OTLP endpoint (e.g. from `DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT`).
    pub otlp_endpoint: Option<String>,
    /// Communication protocol for the OTLP endpoint.
    pub otlp_protocol: Protocol,
    /// OTLP endpoint for metrics; overrides `otlp_endpoint` when set.
    pub otlp_metrics_endpoint: Option<String>,
    /// OTLP endpoint for logs; overrides `otlp_endpoint` when set.
    pub otlp_logs_endpoint: Option<String>,
    /// OTLP endpoint for traces; overrides `otlp_endpoint` when set.
    pub otlp_traces_endpoint: Option<String>,
    /// Metrics export interval in milliseconds.
    pub metrics_export_interval_ms: Option<u64>,
}

impl Config {
    /// Load config from environment variables.
    pub fn from_env() -> Self {
        let otlp_endpoint = if let Ok(endpoint) = std::env::var(OTEL_EXPORTER_OTLP_ENDPOINT) {
            Some(endpoint)
        } else if let Ok(endpoint) = std::env::var(ENV_OLD_OTLP_ENDPOINT) {
            log::warn!(
                "Using deprecated environment variable {} for OTLP endpoint. Use {} instead.",
                ENV_OLD_OTLP_ENDPOINT,
                OTEL_EXPORTER_OTLP_ENDPOINT
            );
            Some(endpoint)
        } else {
            None
        };

        // Note that even though the OTEL SDK can detect these automatically, we still need them
        // to enable the OTLP exporters
        Self {
            otlp_endpoint,
            otlp_protocol: parse_protocol(
                std::env::var(OTEL_EXPORTER_OTLP_PROTOCOL)
                    .ok()
                    .unwrap_or_else(|| "grpc".to_string())
                    .as_str(),
            ),
            otlp_metrics_endpoint: std::env::var(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT).ok(),
            otlp_logs_endpoint: std::env::var(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT).ok(),
            otlp_traces_endpoint: std::env::var(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT).ok(),
            metrics_export_interval_ms: std::env::var(ENV_METRICS_EXPORT_INTERVAL_MS)
                .ok()
                .and_then(|s| s.parse().ok()),
        }
    }

    /// Returns whether OpenTelemetry should be enabled (any endpoint is set).
    pub fn enabled(&self) -> bool {
        self.otlp_endpoint.is_some()
            || self.otlp_metrics_endpoint.is_some()
            || self.otlp_logs_endpoint.is_some()
            || self.otlp_traces_endpoint.is_some()
    }

    pub fn metrics_endpoint(&self) -> Option<&str> {
        self.otlp_metrics_endpoint
            .as_deref()
            .or(self.otlp_endpoint.as_deref())
    }

    pub fn logs_endpoint(&self) -> Option<&str> {
        self.otlp_logs_endpoint
            .as_deref()
            .or(self.otlp_endpoint.as_deref())
    }

    pub fn traces_endpoint(&self) -> Option<&str> {
        self.otlp_traces_endpoint
            .as_deref()
            .or(self.otlp_endpoint.as_deref())
    }

    /// Metrics export interval. Defaults to 500 ms when not set.
    pub fn metrics_export_interval(&self) -> Duration {
        self.metrics_export_interval_ms
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(500))
    }
}
