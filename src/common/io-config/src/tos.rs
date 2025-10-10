use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

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
        if let Some(access_key_id) = &self.access_key {
            res.push(format!("Access key id = {access_key_id}"));
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
}
