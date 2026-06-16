use std::collections::BTreeMap;

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
    pub multipart_size: u64,
    pub multipart_max_concurrency: u32,
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
            multipart_size: 8 * 1024 * 1024,
            multipart_max_concurrency: 16,
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
        res.push(format!("Multipart size = {}", self.multipart_size));
        res.push(format!(
            "Multipart max concurrency = {}",
            self.multipart_max_concurrency
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

    /// Convert TosConfig into an OpenDAL-compatible configuration map.
    ///
    /// This allows the generic OpenDAL backend to be used instead of a dedicated TOS source,
    /// while preserving the friendly TosConfig API (region auto-derivation, env var scanning, etc.).
    pub fn to_opendal_config(&self, bucket: &str) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        config.insert("bucket".to_string(), bucket.to_string());

        let (endpoint, region) = self.endpoint_and_region();
        config.insert("endpoint".to_string(), endpoint);
        config.insert("region".to_string(), region);

        if self.anonymous {
            config.insert("disable_config_load".to_string(), "true".to_string());
            config.insert("skip_signature".to_string(), "true".to_string());
        } else {
            if let Some(access_key) = &self.access_key {
                config.insert("access_key_id".to_string(), access_key.clone());
            }
            if let Some(secret_key) = &self.secret_key {
                config.insert(
                    "secret_access_key".to_string(),
                    secret_key.as_string().clone(),
                );
            }
            if let Some(security_token) = &self.security_token {
                config.insert(
                    "security_token".to_string(),
                    security_token.as_string().clone(),
                );
            }

            config.insert("disable_config_load".to_string(), "false".to_string());
        }

        config
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_opendal_config_with_credentials() {
        let config = TosConfig {
            region: Some("cn-beijing".to_string()),
            endpoint: Some("https://tos-cn-beijing.volces.com".to_string()),
            access_key: Some("test-ak".to_string()),
            secret_key: Some("test-sk".to_string().into()),
            security_token: Some("test-token".to_string().into()),
            ..Default::default()
        };

        let opendal_config = config.to_opendal_config("my-bucket");
        assert_eq!(opendal_config.get("bucket"), Some(&"my-bucket".to_string()));
        assert_eq!(
            opendal_config.get("endpoint"),
            Some(&"https://tos-cn-beijing.volces.com".to_string())
        );
        assert_eq!(
            opendal_config.get("region"),
            Some(&"cn-beijing".to_string())
        );
        assert_eq!(
            opendal_config.get("access_key_id"),
            Some(&"test-ak".to_string())
        );
        assert_eq!(
            opendal_config.get("secret_access_key"),
            Some(&"test-sk".to_string())
        );
        assert_eq!(
            opendal_config.get("security_token"),
            Some(&"test-token".to_string())
        );
        assert_eq!(
            opendal_config.get("disable_config_load"),
            Some(&"false".to_string())
        );
    }

    #[test]
    fn test_to_opendal_config_anonymous() {
        let config = TosConfig {
            anonymous: true,
            ..Default::default()
        };

        let opendal_config = config.to_opendal_config("public-bucket");
        assert_eq!(
            opendal_config.get("disable_config_load"),
            Some(&"true".to_string())
        );
        assert_eq!(
            opendal_config.get("skip_signature"),
            Some(&"true".to_string())
        );
        assert!(!opendal_config.contains_key("access_key_id"));
    }
}
