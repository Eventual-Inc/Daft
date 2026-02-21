use std::collections::BTreeMap;

use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

/// Default region for Tencent Cloud COS
pub const DEFAULT_REGION: &str = "ap-guangzhou";

/// Configuration for Tencent Cloud COS (Cloud Object Storage)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Display)]
#[display(
    "CosConfig
    region: {region:?}
    endpoint: {endpoint:?}
    secret_id: {secret_id:?}
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
pub struct CosConfig {
    /// Region name, e.g., "ap-guangzhou", "ap-beijing", "ap-shanghai"
    pub region: Option<String>,
    /// Custom endpoint URL, e.g., "https://cos.ap-guangzhou.myqcloud.com"
    pub endpoint: Option<String>,
    /// Tencent Cloud SecretId
    pub secret_id: Option<String>,
    /// Tencent Cloud SecretKey
    pub secret_key: Option<ObfuscatedString>,
    /// Security token for temporary credentials (STS)
    pub security_token: Option<ObfuscatedString>,
    /// Whether to use anonymous access
    pub anonymous: bool,
    /// Maximum number of retries
    pub max_retries: u32,
    /// Retry timeout in milliseconds
    pub retry_timeout_ms: u64,
    /// Connection timeout in milliseconds
    pub connect_timeout_ms: u64,
    /// Read timeout in milliseconds
    pub read_timeout_ms: u64,
    /// Maximum concurrent requests
    pub max_concurrent_requests: u32,
    /// Maximum connections per IO thread
    pub max_connections_per_io_thread: u32,
}

impl Default for CosConfig {
    fn default() -> Self {
        Self {
            region: None,
            endpoint: None,
            secret_id: None,
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

impl CosConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(region) = &self.region {
            res.push(format!("Region name = {region}"));
        }
        if let Some(endpoint) = &self.endpoint {
            res.push(format!("Endpoint URL = {endpoint}"));
        }
        if let Some(secret_id) = &self.secret_id {
            res.push(format!("Secret ID = {secret_id}"));
        }
        if self.secret_key.is_some() {
            res.push("Secret key = ***".to_string());
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

    /// Get the endpoint and region, with automatic derivation if possible
    pub fn endpoint_and_region(&self) -> (Option<String>, String) {
        match (self.endpoint.clone(), self.region.clone()) {
            (Some(ep), Some(re)) => (Some(ep), re),
            (Some(ep), None) => {
                let region = extract_region(&ep).unwrap_or_else(|| {
                    log::warn!(
                        "Cannot extract region from endpoint {ep}, use default region {DEFAULT_REGION}"
                    );
                    DEFAULT_REGION.to_string()
                });
                (Some(ep), region)
            }
            (None, Some(re)) => {
                // Derive endpoint from region
                let endpoint = format!("https://cos.{re}.myqcloud.com");
                (Some(endpoint), re)
            }
            (None, None) => {
                log::warn!(
                    "Both endpoint and region are not found, use default region {DEFAULT_REGION}"
                );
                (None, DEFAULT_REGION.to_string())
            }
        }
    }

    /// Convert CosConfig into an OpenDAL-compatible configuration map.
    ///
    /// This allows the generic OpenDAL backend to be used instead of a dedicated COS source,
    /// while preserving the friendly CosConfig API (region auto-derivation, env var scanning, etc.).
    pub fn to_opendal_config(&self, bucket: &str) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        config.insert("bucket".to_string(), bucket.to_string());

        let (endpoint, region) = self.endpoint_and_region();
        if let Some(ep) = endpoint {
            config.insert("endpoint".to_string(), ep);
        }
        config.insert("region".to_string(), region);

        if let Some(secret_id) = &self.secret_id {
            config.insert("secret_id".to_string(), secret_id.clone());
        }
        if let Some(secret_key) = &self.secret_key {
            config.insert("secret_key".to_string(), secret_key.as_string().clone());
        }
        if let Some(security_token) = &self.security_token {
            config.insert(
                "security_token".to_string(),
                security_token.as_string().clone(),
            );
        }

        // Allow OpenDAL to also load from environment variables
        config.insert("disable_config_load".to_string(), "false".to_string());

        config
    }
}

/// Extract region from COS endpoint URL
/// e.g., "https://cos.ap-guangzhou.myqcloud.com" -> "ap-guangzhou"
pub fn extract_region(endpoint: &str) -> Option<String> {
    let host = endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()?;

    // COS endpoint format: cos.<region>.myqcloud.com
    // or <bucket>.cos.<region>.myqcloud.com
    for part in host.split('.') {
        if part.starts_with("ap-")
            || part.starts_with("na-")
            || part.starts_with("eu-")
            || part.starts_with("sa-")
        {
            return Some(part.to_string());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_region() {
        assert_eq!(
            extract_region("https://cos.ap-guangzhou.myqcloud.com"),
            Some("ap-guangzhou".to_string())
        );
        assert_eq!(
            extract_region("https://cos.ap-beijing.myqcloud.com"),
            Some("ap-beijing".to_string())
        );
        assert_eq!(
            extract_region("https://bucket.cos.ap-shanghai.myqcloud.com"),
            Some("ap-shanghai".to_string())
        );
        assert_eq!(
            extract_region("https://cos.na-siliconvalley.myqcloud.com"),
            Some("na-siliconvalley".to_string())
        );
        assert_eq!(extract_region("https://invalid-endpoint.com"), None);
    }

    #[test]
    fn test_extract_region_http() {
        // Test HTTP (non-HTTPS) endpoints
        assert_eq!(
            extract_region("http://cos.ap-guangzhou.myqcloud.com"),
            Some("ap-guangzhou".to_string())
        );
    }

    #[test]
    fn test_extract_region_eu_sa() {
        // Test EU and SA regions
        assert_eq!(
            extract_region("https://cos.eu-frankfurt.myqcloud.com"),
            Some("eu-frankfurt".to_string())
        );
        assert_eq!(
            extract_region("https://cos.sa-saopaulo.myqcloud.com"),
            Some("sa-saopaulo".to_string())
        );
    }

    #[test]
    fn test_endpoint_and_region_both_provided() {
        // Both endpoint and region provided
        let config = CosConfig {
            endpoint: Some("https://cos.ap-shanghai.myqcloud.com".to_string()),
            region: Some("ap-shanghai".to_string()),
            ..Default::default()
        };
        let (endpoint, region) = config.endpoint_and_region();
        assert_eq!(
            endpoint,
            Some("https://cos.ap-shanghai.myqcloud.com".to_string())
        );
        assert_eq!(region, "ap-shanghai");
    }

    #[test]
    fn test_endpoint_and_region_only_region() {
        // Only region provided
        let config = CosConfig {
            region: Some("ap-beijing".to_string()),
            ..Default::default()
        };
        let (endpoint, region) = config.endpoint_and_region();
        assert_eq!(
            endpoint,
            Some("https://cos.ap-beijing.myqcloud.com".to_string())
        );
        assert_eq!(region, "ap-beijing");
    }

    #[test]
    fn test_endpoint_and_region_only_endpoint() {
        // Only endpoint provided (region extracted from endpoint)
        let config = CosConfig {
            endpoint: Some("https://cos.ap-nanjing.myqcloud.com".to_string()),
            ..Default::default()
        };
        let (endpoint, region) = config.endpoint_and_region();
        assert_eq!(
            endpoint,
            Some("https://cos.ap-nanjing.myqcloud.com".to_string())
        );
        assert_eq!(region, "ap-nanjing");
    }

    #[test]
    fn test_endpoint_and_region_endpoint_without_region() {
        // Endpoint without extractable region (use default)
        let config = CosConfig {
            endpoint: Some("https://custom-endpoint.example.com".to_string()),
            ..Default::default()
        };
        let (endpoint, region) = config.endpoint_and_region();
        assert_eq!(
            endpoint,
            Some("https://custom-endpoint.example.com".to_string())
        );
        assert_eq!(region, DEFAULT_REGION);
    }

    #[test]
    fn test_endpoint_and_region_neither_provided() {
        // Neither endpoint nor region provided (use default)
        let config = CosConfig::default();
        let (endpoint, region) = config.endpoint_and_region();
        assert_eq!(endpoint, None);
        assert_eq!(region, DEFAULT_REGION);
    }

    #[test]
    fn test_cos_config_default() {
        let config = CosConfig::default();
        assert_eq!(config.region, None);
        assert_eq!(config.endpoint, None);
        assert_eq!(config.secret_id, None);
        assert_eq!(config.secret_key, None);
        assert_eq!(config.security_token, None);
        assert!(!config.anonymous);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_timeout_ms, 30_000);
        assert_eq!(config.connect_timeout_ms, 10_000);
        assert_eq!(config.read_timeout_ms, 30_000);
        assert_eq!(config.max_concurrent_requests, 50);
        assert_eq!(config.max_connections_per_io_thread, 50);
    }

    #[test]
    fn test_cos_config_display() {
        let config = CosConfig {
            region: Some("ap-guangzhou".to_string()),
            endpoint: Some("https://cos.ap-guangzhou.myqcloud.com".to_string()),
            secret_id: Some("test-id".to_string()),
            secret_key: Some("test-key".to_string().into()),
            security_token: Some("test-token".to_string().into()),
            anonymous: false,
            ..Default::default()
        };
        let display = format!("{}", config);
        assert!(display.contains("CosConfig"));
        assert!(display.contains("ap-guangzhou"));
        assert!(display.contains("***")); // secret_key and security_token should be masked
    }

    #[test]
    fn test_cos_config_multiline_display() {
        let config = CosConfig {
            region: Some("ap-guangzhou".to_string()),
            endpoint: Some("https://cos.ap-guangzhou.myqcloud.com".to_string()),
            secret_id: Some("test-id".to_string()),
            secret_key: Some("test-key".to_string().into()),
            security_token: Some("test-token".to_string().into()),
            anonymous: false,
            max_retries: 5,
            retry_timeout_ms: 60000,
            connect_timeout_ms: 20000,
            read_timeout_ms: 60000,
            max_concurrent_requests: 100,
            max_connections_per_io_thread: 100,
        };
        let lines = config.multiline_display();
        assert!(
            lines
                .iter()
                .any(|l| l.contains("Region name = ap-guangzhou"))
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("Endpoint URL = https://cos.ap-guangzhou.myqcloud.com"))
        );
        assert!(lines.iter().any(|l| l.contains("Secret ID = test-id")));
        assert!(lines.iter().any(|l| l.contains("Secret key = ***")));
        assert!(lines.iter().any(|l| l.contains("Security token = ***")));
        assert!(lines.iter().any(|l| l.contains("Anonymous = false")));
        assert!(lines.iter().any(|l| l.contains("Max retries = 5")));
        assert!(lines.iter().any(|l| l.contains("Retry timeout = 60000ms")));
        assert!(
            lines
                .iter()
                .any(|l| l.contains("Connect timeout = 20000ms"))
        );
        assert!(lines.iter().any(|l| l.contains("Read timeout = 60000ms")));
        assert!(
            lines
                .iter()
                .any(|l| l.contains("Max concurrent requests = 100"))
        );
        assert!(lines.iter().any(|l| l.contains("Max connections = 100")));
    }

    #[test]
    fn test_cos_config_multiline_display_minimal() {
        // Test with minimal config (no secrets)
        let config = CosConfig::default();
        let lines = config.multiline_display();
        assert!(lines.iter().any(|l| l.contains("Anonymous = false")));
        assert!(lines.iter().any(|l| l.contains("Max retries = 3")));
        // Should not contain secret lines when not set
        assert!(!lines.iter().any(|l| l.contains("Region name")));
        assert!(!lines.iter().any(|l| l.contains("Endpoint URL")));
        assert!(!lines.iter().any(|l| l.contains("Secret ID")));
    }

    #[test]
    fn test_cos_config_clone() {
        let config = CosConfig {
            region: Some("ap-guangzhou".to_string()),
            secret_id: Some("test-id".to_string()),
            ..Default::default()
        };
        let cloned = config.clone();
        assert_eq!(config.region, cloned.region);
        assert_eq!(config.secret_id, cloned.secret_id);
    }

    #[test]
    fn test_cos_config_equality() {
        let config1 = CosConfig {
            region: Some("ap-guangzhou".to_string()),
            ..Default::default()
        };
        let config2 = CosConfig {
            region: Some("ap-guangzhou".to_string()),
            ..Default::default()
        };
        let config3 = CosConfig {
            region: Some("ap-beijing".to_string()),
            ..Default::default()
        };
        assert_eq!(config1, config2);
        assert_ne!(config1, config3);
    }

    #[test]
    fn test_cos_config_hash() {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };

        let config1 = CosConfig {
            region: Some("ap-guangzhou".to_string()),
            ..Default::default()
        };
        let config2 = CosConfig {
            region: Some("ap-guangzhou".to_string()),
            ..Default::default()
        };

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        config1.hash(&mut hasher1);
        config2.hash(&mut hasher2);
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_default_region_constant() {
        assert_eq!(DEFAULT_REGION, "ap-guangzhou");
    }
}
