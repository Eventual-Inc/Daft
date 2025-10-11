use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

pub const DEFAULT_REGION: &str = "cn-beijing";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Display)]
#[display(
    "TosConfig
    region: {region:?}
    endpoint: {endpoint:?}
    access_key: {access_key:?}
    secret_key: ***
    security_token: ***
    anonymous: {anonymous}
    max_retries: {max_retries}
    retry_timeout_ms: {retry_timeout_ms}
    connect_timeout_ms: {connect_timeout_ms}
    read_timeout_ms: {read_timeout_ms}
    max_concurrent_requests: {max_concurrent_requests}
    max_connections_per_io_thread: {max_connections_per_io_thread}"
)]
pub struct TosConfig {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<ObfuscatedString>,
    pub security_token: Option<ObfuscatedString>,
    pub anonymous: bool,
    pub max_retries: u32,
    pub retry_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub max_concurrent_requests: u32,
    pub max_connections_per_io_thread: u32,
}

impl Default for TosConfig {
    fn default() -> Self {
        Self {
            region: None,
            endpoint: None,
            access_key: None,
            secret_key: None,
            security_token: None,
            anonymous: false,
            max_retries: 3,
            retry_timeout_ms: 30_000,
            connect_timeout_ms: 10_000,
            read_timeout_ms: 30_000,
            max_concurrent_requests: 50,
            max_connections_per_io_thread: 50,
        }
    }
}

impl TosConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(region) = &self.region {
            res.push(format!("Region name = {region}"));
        }
        if let Some(endpoint) = &self.endpoint {
            res.push(format!("Endpoint URL = {endpoint}"));
        }
        if let Some(access_key) = &self.access_key {
            res.push(format!("Access key id = {access_key}"));
        }
        if self.secret_key.is_some() {
            res.push("Secret access key = ***".to_string());
        }
        if self.security_token.is_some() {
            res.push("Security token = ***".to_string());
        }
        res.push(format!("Anonymous = {}", self.anonymous));
        res.push(format!("Max retries = {}", self.max_retries));
        res.push(format!("Retry timeout = {}ms", self.retry_timeout_ms));
        res.push(format!("Connect timeout = {}ms", self.connect_timeout_ms));
        res.push(format!("Read timeout = {}ms", self.read_timeout_ms));
        res.push(format!(
            "Max concurrent requests = {}",
            self.max_concurrent_requests
        ));
        res.push(format!(
            "Max connections = {}",
            self.max_connections_per_io_thread
        ));
        res
    }

    pub fn endpoint_and_region(&self) -> (String, String) {
        match (self.endpoint.clone(), self.region.clone()) {
            (Some(ep), Some(re)) => (ep, re),
            (Some(ep), None) => {
                let region = extract_region(&ep)
                    .unwrap_or_else(|| {
                        log::warn!("Cannot extract region from endpoint {ep}, use default region {DEFAULT_REGION}");
                        DEFAULT_REGION.to_string()
                    });
                (ep, region)
            }
            (None, Some(re)) => {
                log::warn!(
                    "Endpoint is not set but found region {re}, use default endpoint tos-{re}.volces.com"
                );
                (format!("tos-{re}.volces.com"), re)
            }
            (None, None) => {
                log::warn!(
                    "Both endpoint and region are not found, use default endpoint tos-{DEFAULT_REGION}.volces.com"
                );
                (
                    format!("tos-{DEFAULT_REGION}.volces.com"),
                    DEFAULT_REGION.to_string(),
                )
            }
        }
    }
}

pub fn extract_region(endpoint: &str) -> Option<String> {
    let host = endpoint
        .trim_start_matches(|c: char| !c.is_ascii_alphanumeric())
        .trim_start_matches("://")
        .split('/')
        .next()?;

    for part in host.split('.') {
        if let Some(region) = part.strip_prefix("tos-") {
            return Some(region.to_string());
        }
    }

    None
}
