use std::collections::BTreeMap;

use derive_more::Display;
use serde::{Deserialize, Serialize};

/// Configuration for HDFS (Hadoop Distributed File System).
///
/// HDFS is accessed via the JNI-based `services-hdfs` OpenDAL backend.
/// This configuration is forwarded to OpenDAL's HDFS operator.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Display, Default)]
#[display(
    "HdfsConfig
    name_node: {name_node:?}
    root: {root:?}"
)]
pub struct HdfsConfig {
    /// HDFS name node address (e.g. `hdfs://namenode:9000`).
    pub name_node: Option<String>,
    /// Root path inside HDFS (defaults to "/").
    pub root: Option<String>,
}

impl HdfsConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(name_node) = &self.name_node {
            res.push(format!("Name node = {name_node}"));
        }
        if let Some(root) = &self.root {
            res.push(format!("Root = {root}"));
        }
        res
    }

    /// Convert HdfsConfig into an OpenDAL-compatible configuration map.
    ///
    /// `name_node_from_url` is the name node extracted from the URL (e.g. from
    /// `hdfs://host:port/path`). It is used as a fallback when `self.name_node`
    /// is not set — this makes `hdfs://host:port/path` work without requiring
    /// an explicit `IOConfig`.
    pub fn to_opendal_config(&self, name_node_from_url: &str) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        // Resolve name_node: prefer explicit config, fall back to URL.
        let name_node = self
            .name_node
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                if name_node_from_url.is_empty() {
                    None
                } else {
                    Some(name_node_from_url.to_string())
                }
            });
        if let Some(nn) = name_node {
            config.insert("name_node".to_string(), nn);
        }

        if let Some(root) = &self.root {
            config.insert("root".to_string(), root.clone());
        }

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hdfs_config_default() {
        let config = HdfsConfig::default();
        assert_eq!(config.name_node, None);
        assert_eq!(config.root, None);
    }

    #[test]
    fn test_to_opendal_config_uses_url_when_no_name_node() {
        let config = HdfsConfig::default();
        let map = config.to_opendal_config("hdfs://namenode:9000");
        assert_eq!(
            map.get("name_node"),
            Some(&"hdfs://namenode:9000".to_string())
        );
    }

    #[test]
    fn test_to_opendal_config_name_node_overrides_url() {
        let config = HdfsConfig {
            name_node: Some("hdfs://primary:9000".to_string()),
            ..Default::default()
        };
        let map = config.to_opendal_config("hdfs://fallback:8020");
        assert_eq!(
            map.get("name_node"),
            Some(&"hdfs://primary:9000".to_string())
        );
    }

    #[test]
    fn test_to_opendal_config_no_name_node_and_no_url() {
        let config = HdfsConfig::default();
        let map = config.to_opendal_config("");
        assert!(!map.contains_key("name_node"));
    }

    #[test]
    fn test_to_opendal_config_full_fields() {
        let config = HdfsConfig {
            name_node: Some("hdfs://nn:9000".to_string()),
            root: Some("/data".to_string()),
        };
        let map = config.to_opendal_config("");
        assert_eq!(map.get("name_node"), Some(&"hdfs://nn:9000".to_string()));
        assert_eq!(map.get("root"), Some(&"/data".to_string()));
    }

    #[test]
    fn test_hdfs_config_multiline_display() {
        let config = HdfsConfig {
            name_node: Some("hdfs://nn:9000".to_string()),
            root: Some("/data".to_string()),
        };
        let lines = config.multiline_display();
        assert!(
            lines
                .iter()
                .any(|l| l.contains("Name node = hdfs://nn:9000"))
        );
        assert!(lines.iter().any(|l| l.contains("Root = /data")));
    }

    #[test]
    fn test_hdfs_config_multiline_display_empty_for_defaults() {
        let lines = HdfsConfig::default().multiline_display();
        assert!(lines.is_empty(), "expected empty, got {lines:?}");
    }
}
