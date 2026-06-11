use std::collections::BTreeMap;

use derive_more::Display;
use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

/// Configuration for GooseFS (distributed caching file system)
///
/// GooseFS is a distributed caching file system accessed via native gRPC protocol.
/// This configuration is forwarded to OpenDAL's `services-goosefs` backend.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Display)]
#[display(
    "GoosefsConfig
    root: {root:?}
    master_addr: {master_addr:?}
    block_size: {block_size:?}
    chunk_size: {chunk_size:?}
    write_type: {write_type:?}
    auth_type: {auth_type:?}
    auth_username: {auth_username:?}
    auth_password: ***
    anonymous: {anonymous}
    max_retries: {max_retries}
    retry_timeout_ms: {retry_timeout_ms}
    connect_timeout_ms: {connect_timeout_ms}
    read_timeout_ms: {read_timeout_ms}
    max_concurrent_requests: {max_concurrent_requests}
    max_connections_per_io_thread: {max_connections_per_io_thread}"
)]
pub struct GoosefsConfig {
    /// Root path of the backend. All operations happen under this root.
    /// Defaults to "/" if not set.
    pub root: Option<String>,
    /// Master address(es) in `host:port` format.
    ///
    /// - Single master: `"10.0.0.1:9200"`
    /// - HA (comma-separated): `"10.0.0.1:9200,10.0.0.2:9200,10.0.0.3:9200"`
    pub master_addr: Option<String>,
    /// Block size in bytes for new files (default: 64 MiB).
    pub block_size: Option<u64>,
    /// Chunk size in bytes for streaming RPCs (default: 1 MiB).
    pub chunk_size: Option<u64>,
    /// Default write type for new files.
    ///
    /// Supported values: "must_cache", "cache_through", "through", "async_through".
    pub write_type: Option<String>,
    /// Authentication type.
    ///
    /// Supported values: "nosasl", "simple". Defaults to "simple".
    pub auth_type: Option<String>,
    /// Authentication username (used in SIMPLE mode).
    /// Defaults to current OS user.
    pub auth_username: Option<String>,
    /// Optional authentication password (kept secret).
    pub auth_password: Option<ObfuscatedString>,
    /// Whether to use anonymous access (skip credential forwarding)
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

impl Default for GoosefsConfig {
    fn default() -> Self {
        Self {
            root: None,
            master_addr: None,
            block_size: None,
            chunk_size: None,
            write_type: None,
            auth_type: None,
            auth_username: None,
            auth_password: None,
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

impl GoosefsConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(root) = &self.root {
            res.push(format!("Root = {root}"));
        }
        if let Some(master_addr) = &self.master_addr {
            res.push(format!("Master addr = {master_addr}"));
        }
        if let Some(block_size) = self.block_size {
            res.push(format!("Block size = {block_size}"));
        }
        if let Some(chunk_size) = self.chunk_size {
            res.push(format!("Chunk size = {chunk_size}"));
        }
        if let Some(write_type) = &self.write_type {
            res.push(format!("Write type = {write_type}"));
        }
        if let Some(auth_type) = &self.auth_type {
            res.push(format!("Auth type = {auth_type}"));
        }
        if let Some(auth_username) = &self.auth_username {
            res.push(format!("Auth username = {auth_username}"));
        }
        if self.auth_password.is_some() {
            res.push("Auth password = ***".to_string());
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

    /// Convert GoosefsConfig into an OpenDAL-compatible configuration map.
    ///
    /// `authority` is the URL host[:port] component (e.g. parsed from
    /// `goosefs://host:port/path`). When provided and non-empty, it is used as
    /// `master_addr` unless the user already configured `master_addr` explicitly.
    pub fn to_opendal_config(&self, authority: &str) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        // Resolve master address: prefer explicit config, fall back to URL authority.
        let master_addr = self
            .master_addr
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                if authority.is_empty() {
                    None
                } else {
                    Some(authority.to_string())
                }
            });
        if let Some(ma) = master_addr {
            config.insert("master_addr".to_string(), ma);
        }

        if let Some(root) = &self.root {
            config.insert("root".to_string(), root.clone());
        }
        if let Some(block_size) = self.block_size {
            config.insert("block_size".to_string(), block_size.to_string());
        }
        if let Some(chunk_size) = self.chunk_size {
            config.insert("chunk_size".to_string(), chunk_size.to_string());
        }
        if let Some(write_type) = &self.write_type {
            config.insert("write_type".to_string(), write_type.clone());
        }

        if self.anonymous {
            // Anonymous mode: force nosasl, ignore credential fields
            config.insert("auth_type".to_string(), "nosasl".to_string());
        } else {
            if let Some(auth_type) = &self.auth_type {
                config.insert("auth_type".to_string(), auth_type.clone());
            }
            if let Some(auth_username) = &self.auth_username {
                config.insert("auth_username".to_string(), auth_username.clone());
            }
            if let Some(auth_password) = &self.auth_password {
                // Forward as a generic option; the underlying backend may or may not
                // consume it, but this preserves user intent.
                config.insert(
                    "auth_password".to_string(),
                    auth_password.as_string().clone(),
                );
            }
        }

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_goosefs_config_default() {
        let config = GoosefsConfig::default();
        assert_eq!(config.root, None);
        assert_eq!(config.master_addr, None);
        assert_eq!(config.block_size, None);
        assert_eq!(config.chunk_size, None);
        assert_eq!(config.write_type, None);
        assert_eq!(config.auth_type, None);
        assert_eq!(config.auth_username, None);
        assert_eq!(config.auth_password, None);
        assert!(!config.anonymous);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_timeout_ms, 30_000);
        assert_eq!(config.connect_timeout_ms, 10_000);
        assert_eq!(config.read_timeout_ms, 30_000);
        assert_eq!(config.max_concurrent_requests, 50);
        assert_eq!(config.max_connections_per_io_thread, 50);
    }

    #[test]
    fn test_to_opendal_config_uses_authority_when_no_master_addr() {
        let config = GoosefsConfig::default();
        let map = config.to_opendal_config("10.0.0.1:9200");
        assert_eq!(map.get("master_addr"), Some(&"10.0.0.1:9200".to_string()));
    }

    #[test]
    fn test_to_opendal_config_master_addr_overrides_authority() {
        let config = GoosefsConfig {
            master_addr: Some("primary:9200,secondary:9200".to_string()),
            ..Default::default()
        };
        let map = config.to_opendal_config("ignored:9999");
        assert_eq!(
            map.get("master_addr"),
            Some(&"primary:9200,secondary:9200".to_string())
        );
    }

    #[test]
    fn test_to_opendal_config_no_master_addr_and_no_authority() {
        let config = GoosefsConfig::default();
        let map = config.to_opendal_config("");
        assert!(!map.contains_key("master_addr"));
    }

    #[test]
    fn test_to_opendal_config_full_fields() {
        let config = GoosefsConfig {
            root: Some("/data".to_string()),
            master_addr: Some("m:9200".to_string()),
            block_size: Some(1024),
            chunk_size: Some(256),
            write_type: Some("cache_through".to_string()),
            auth_type: Some("simple".to_string()),
            auth_username: Some("alice".to_string()),
            auth_password: Some("secret".to_string().into()),
            ..Default::default()
        };
        let map = config.to_opendal_config("");
        assert_eq!(map.get("root"), Some(&"/data".to_string()));
        assert_eq!(map.get("master_addr"), Some(&"m:9200".to_string()));
        assert_eq!(map.get("block_size"), Some(&"1024".to_string()));
        assert_eq!(map.get("chunk_size"), Some(&"256".to_string()));
        assert_eq!(map.get("write_type"), Some(&"cache_through".to_string()));
        assert_eq!(map.get("auth_type"), Some(&"simple".to_string()));
        assert_eq!(map.get("auth_username"), Some(&"alice".to_string()));
        assert_eq!(map.get("auth_password"), Some(&"secret".to_string()));
    }

    #[test]
    fn test_to_opendal_config_anonymous_forces_nosasl() {
        let config = GoosefsConfig {
            anonymous: true,
            auth_type: Some("simple".to_string()),
            auth_username: Some("alice".to_string()),
            auth_password: Some("secret".to_string().into()),
            ..Default::default()
        };
        let map = config.to_opendal_config("m:9200");
        assert_eq!(map.get("auth_type"), Some(&"nosasl".to_string()));
        assert!(!map.contains_key("auth_username"));
        assert!(!map.contains_key("auth_password"));
    }

    #[test]
    fn test_goosefs_config_display_masks_password() {
        let config = GoosefsConfig {
            master_addr: Some("m:9200".to_string()),
            auth_username: Some("alice".to_string()),
            auth_password: Some("super-secret".to_string().into()),
            ..Default::default()
        };
        let s = format!("{}", config);
        assert!(s.contains("GoosefsConfig"));
        assert!(s.contains("alice"));
        assert!(!s.contains("super-secret"));
        assert!(s.contains("***"));
    }

    #[test]
    fn test_goosefs_config_multiline_display() {
        let config = GoosefsConfig {
            root: Some("/data".to_string()),
            master_addr: Some("m:9200".to_string()),
            block_size: Some(1024),
            chunk_size: Some(256),
            write_type: Some("cache_through".to_string()),
            auth_type: Some("simple".to_string()),
            auth_username: Some("alice".to_string()),
            auth_password: Some("secret".to_string().into()),
            ..Default::default()
        };
        let lines = config.multiline_display();
        assert!(lines.iter().any(|l| l.contains("Root = /data")));
        assert!(lines.iter().any(|l| l.contains("Master addr = m:9200")));
        assert!(lines.iter().any(|l| l.contains("Block size = 1024")));
        assert!(lines.iter().any(|l| l.contains("Chunk size = 256")));
        assert!(
            lines
                .iter()
                .any(|l| l.contains("Write type = cache_through"))
        );
        assert!(lines.iter().any(|l| l.contains("Auth type = simple")));
        assert!(lines.iter().any(|l| l.contains("Auth username = alice")));
        assert!(lines.iter().any(|l| l.contains("Auth password = ***")));
    }

    #[test]
    fn test_goosefs_config_equality_and_hash() {
        use std::{
            collections::hash_map::DefaultHasher,
            hash::{Hash, Hasher},
        };

        let c1 = GoosefsConfig {
            master_addr: Some("m:9200".to_string()),
            ..Default::default()
        };
        let c2 = GoosefsConfig {
            master_addr: Some("m:9200".to_string()),
            ..Default::default()
        };
        let c3 = GoosefsConfig {
            master_addr: Some("other:9200".to_string()),
            ..Default::default()
        };
        assert_eq!(c1, c2);
        assert_ne!(c1, c3);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        c1.hash(&mut h1);
        c2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }
}
