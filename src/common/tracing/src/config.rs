use std::time::Duration;

use opentelemetry::Key;
// These match the OTEL_EXPORTER_OTLP_* environment variables from:
// https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/#endpoint-configuration
use opentelemetry_otlp::{
    OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT, OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, Protocol,
};
use opentelemetry_sdk::{Resource, resource::EnvResourceDetector};

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

    pub fn resource(&self) -> Resource {
        let env_resource = Resource::builder_empty()
            .with_detector(Box::new(EnvResourceDetector::new()))
            .build();
        let service_name = std::env::var("OTEL_SERVICE_NAME")
            .ok()
            .filter(|name| !name.is_empty())
            .or_else(|| {
                env_resource
                    .get(&Key::new("service.name"))
                    .map(|value| value.to_string())
            })
            .unwrap_or_else(|| "daft".to_string());

        Resource::builder().with_service_name(service_name).build()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use opentelemetry::{Key, Value};

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env_vars<R>(vars: &[(&str, Option<&str>)], f: impl FnOnce() -> R) -> R {
        let _guard = ENV_LOCK.lock().unwrap();
        let previous_values: Vec<_> = vars
            .iter()
            .map(|(name, _)| (*name, std::env::var(name).ok()))
            .collect();

        for (name, value) in vars {
            match value {
                Some(value) => unsafe { std::env::set_var(name, value) },
                None => unsafe { std::env::remove_var(name) },
            }
        }

        let result = f();

        for (name, value) in previous_values {
            match value {
                Some(value) => unsafe { std::env::set_var(name, value) },
                None => unsafe { std::env::remove_var(name) },
            }
        }

        result
    }

    #[test]
    fn resource_defaults_to_daft_service_name() {
        with_env_vars(
            &[
                ("OTEL_SERVICE_NAME", None),
                ("OTEL_RESOURCE_ATTRIBUTES", None),
            ],
            || {
                let resource = Config::from_env().resource();
                assert_eq!(
                    resource.get(&Key::new("service.name")),
                    Some(Value::from("daft"))
                );
            },
        );
    }

    #[test]
    fn resource_uses_otel_service_name_env() {
        with_env_vars(
            &[
                ("OTEL_SERVICE_NAME", Some("daft-ray-worker")),
                ("OTEL_RESOURCE_ATTRIBUTES", None),
            ],
            || {
                let resource = Config::from_env().resource();
                assert_eq!(
                    resource.get(&Key::new("service.name")),
                    Some(Value::from("daft-ray-worker"))
                );
            },
        );
    }

    #[test]
    fn resource_includes_otel_resource_attributes() {
        with_env_vars(
            &[
                ("OTEL_SERVICE_NAME", None),
                (
                    "OTEL_RESOURCE_ATTRIBUTES",
                    Some("deployment.environment=staging,service.version=0.7.14"),
                ),
            ],
            || {
                let resource = Config::from_env().resource();
                assert_eq!(
                    resource.get(&Key::new("deployment.environment")),
                    Some(Value::from("staging"))
                );
                assert_eq!(
                    resource.get(&Key::new("service.version")),
                    Some(Value::from("0.7.14"))
                );
                assert_eq!(
                    resource.get(&Key::new("service.name")),
                    Some(Value::from("daft"))
                );
            },
        );
    }
}
