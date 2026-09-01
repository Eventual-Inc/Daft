use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
};

use serde::{Deserialize, Serialize};

use crate::{
    AzureConfig, CosConfig, GCSConfig, GooseFSConfig, HTTPConfig, HdfsConfig, S3Config,
    gravitino::GravitinoConfig, huggingface::HuggingFaceConfig, tos::TosConfig, unity::UnityConfig,
};
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IOConfig {
    pub s3: S3Config,
    pub azure: AzureConfig,
    pub gcs: GCSConfig,
    pub http: HTTPConfig,
    pub unity: UnityConfig,
    pub gravitino: GravitinoConfig,
    pub hf: HuggingFaceConfig,
    /// disable suffix range requests, please use range with offset
    pub disable_suffix_range: bool,
    pub tos: TosConfig,
    pub cos: CosConfig,
    pub goosefs: GooseFSConfig,
    pub hdfs: HdfsConfig,
    /// Additional backends configured via OpenDAL.
    /// Keys are scheme names (e.g. "oss", "cos"), values are key-value config maps.
    pub opendal_backends: BTreeMap<String, BTreeMap<String, String>>,
    /// Protocol aliases: maps custom scheme names to existing scheme names.
    /// For example, {"my-s3": "s3"} rewrites "my-s3://bucket/path" to "s3://bucket/path".
    pub protocol_aliases: BTreeMap<String, String>,
}

/// Identifies which IOConfig backend section is relevant for a URI scheme.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IoBackendKind {
    S3,
    Azure,
    Gcs,
    Http,
    Unity,
    Gravitino,
    HuggingFace,
    Tos,
    Cos,
    GooseFs,
    Hdfs,
}

impl IoBackendKind {
    /// Map a URI scheme (lowercase, without `://`) to the matching backend config section.
    #[must_use]
    pub fn from_scheme(scheme: &str) -> Option<Self> {
        match scheme {
            "s3" | "s3a" | "s3n" => Some(Self::S3),
            "az" | "abfs" | "abfss" => Some(Self::Azure),
            "gcs" | "gs" => Some(Self::Gcs),
            "http" | "https" => Some(Self::Http),
            "vol+dbfs" | "dbfs" => Some(Self::Unity),
            "gvfs" => Some(Self::Gravitino),
            "hf" => Some(Self::HuggingFace),
            "tos" => Some(Self::Tos),
            "cos" | "cosn" => Some(Self::Cos),
            "goosefs" => Some(Self::GooseFs),
            "hdfs" => Some(Self::Hdfs),
            _ => None,
        }
    }

    /// Infer backends from a list of paths/URIs.
    ///
    /// Returns `None` (show all non-default backends) when:
    /// - no remote scheme is present (local paths), or
    /// - any URI has a scheme we don't recognize (protocol aliases / OpenDAL
    ///   custom schemes), so we don't accidentally hide active config.
    #[must_use]
    pub fn from_uris<'a, I>(uris: I) -> Option<Vec<Self>>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut backends = Vec::new();
        let mut saw_scheme = false;
        for uri in uris {
            if let Some((scheme, _)) = uri.split_once("://") {
                saw_scheme = true;
                let scheme = scheme.to_ascii_lowercase();
                // `file://` is local — skip it for filtering purposes.
                if scheme == "file" {
                    continue;
                }
                match Self::from_scheme(&scheme) {
                    Some(backend) if !backends.contains(&backend) => backends.push(backend),
                    Some(_) => {}
                    // Unrecognized scheme (alias / OpenDAL): don't filter.
                    None => return None,
                }
            }
        }
        if saw_scheme && !backends.is_empty() {
            Some(backends)
        } else {
            None
        }
    }
}

impl IOConfig {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        self.multiline_display_for_backends(None)
    }

    /// Sparse IOConfig display.
    ///
    /// When `backends` is `Some`, only those backend sections are considered
    /// (scheme-aware explain). When `None`, all backends are considered.
    /// In both cases only non-default fields are emitted.
    #[must_use]
    pub fn multiline_display_for_backends(
        &self,
        backends: Option<&[IoBackendKind]>,
    ) -> Vec<String> {
        let include =
            |kind: IoBackendKind| backends.is_none_or(|selected| selected.contains(&kind));

        let mut res = vec![];
        let push_backend = |res: &mut Vec<String>, label: &str, lines: Vec<String>| {
            if !lines.is_empty() {
                res.push(format!("{label} = {{ {} }}", lines.join(", ")));
            }
        };

        if include(IoBackendKind::S3) {
            push_backend(&mut res, "S3 config", self.s3.multiline_display());
        }
        if include(IoBackendKind::Azure) {
            push_backend(&mut res, "Azure config", self.azure.multiline_display());
        }
        if include(IoBackendKind::Gcs) {
            push_backend(&mut res, "GCS config", self.gcs.multiline_display());
        }
        if include(IoBackendKind::Http) {
            push_backend(&mut res, "HTTP config", self.http.multiline_display());
        }
        if include(IoBackendKind::Unity) {
            push_backend(&mut res, "Unity config", self.unity.multiline_display());
        }
        if include(IoBackendKind::Gravitino) {
            push_backend(
                &mut res,
                "Gravitino config",
                self.gravitino.multiline_display(),
            );
        }
        if include(IoBackendKind::HuggingFace) {
            push_backend(&mut res, "Hugging Face config", self.hf.multiline_display());
        }
        if self.disable_suffix_range {
            res.push(format!(
                "Disable suffix range = {}",
                self.disable_suffix_range
            ));
        }
        if include(IoBackendKind::Tos) {
            push_backend(&mut res, "TOS config", self.tos.multiline_display());
        }
        if include(IoBackendKind::Cos) {
            push_backend(&mut res, "COS config", self.cos.multiline_display());
        }
        if include(IoBackendKind::GooseFs) {
            push_backend(&mut res, "GooseFS config", self.goosefs.multiline_display());
        }
        if include(IoBackendKind::Hdfs) {
            push_backend(&mut res, "HDFS config", self.hdfs.multiline_display());
        }
        if !self.opendal_backends.is_empty() {
            res.push(format!("OpenDAL backends = {:?}", self.opendal_backends));
        }
        if !self.protocol_aliases.is_empty() {
            res.push(format!("Protocol aliases = {:?}", self.protocol_aliases));
        }
        res
    }

    /// Validates that no protocol alias key shadows a built-in scheme.
    pub fn validate_protocol_aliases(&self) -> std::result::Result<(), String> {
        const BUILTIN_SCHEMES: &[&str] = &[
            "file", "http", "https", "s3", "s3a", "s3n", "az", "abfs", "abfss", "gcs", "gs", "hf",
            "tos", "cos", "cosn", "goosefs", "hdfs", "vol+dbfs", "dbfs", "gvfs",
        ];
        for key in self.protocol_aliases.keys() {
            if BUILTIN_SCHEMES.contains(&key.as_str()) {
                return Err(format!(
                    "Protocol alias key '{key}' conflicts with built-in scheme. \
                     Aliases can only map new custom scheme names to existing schemes."
                ));
            }
        }
        Ok(())
    }
}

impl Display for IOConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let lines = self.multiline_display();
        if lines.is_empty() {
            write!(f, "IOConfig {{}}")
        } else {
            write!(f, "IOConfig:\n{}", lines.join("\n"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_ioconfig_multiline_display_is_empty() {
        assert!(IOConfig::default().multiline_display().is_empty());
    }

    #[test]
    fn default_ioconfig_display_is_compact() {
        assert_eq!(format!("{}", IOConfig::default()), "IOConfig {}");
    }

    #[test]
    fn only_non_default_s3_fields_are_shown() {
        let mut config = IOConfig::default();
        config.s3.region_name = Some("us-west-2".to_string());
        config.s3.anonymous = true;
        let lines = config.multiline_display();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("S3 config"));
        assert!(lines[0].contains("Region name = us-west-2"));
        assert!(lines[0].contains("Anonymous = true"));
        assert!(!lines[0].contains("Azure config"));
        assert!(!lines[0].contains("Max connections"));
    }

    #[test]
    fn scheme_aware_display_filters_to_relevant_backend() {
        let mut config = IOConfig::default();
        config.s3.region_name = Some("us-east-1".to_string());
        config.gcs.project_id = Some("my-project".to_string());

        let s3_only = config.multiline_display_for_backends(Some(&[IoBackendKind::S3]));
        assert_eq!(s3_only.len(), 1);
        assert!(s3_only[0].contains("S3 config"));
        assert!(!s3_only.iter().any(|l| l.contains("GCS config")));

        let gcs_only = config.multiline_display_for_backends(Some(&[IoBackendKind::Gcs]));
        assert_eq!(gcs_only.len(), 1);
        assert!(gcs_only[0].contains("GCS config"));
    }

    #[test]
    fn backend_kind_from_uris() {
        assert_eq!(
            IoBackendKind::from_uris(["s3://bucket/path", "s3://other/key"]),
            Some(vec![IoBackendKind::S3])
        );
        assert_eq!(
            IoBackendKind::from_uris(["gs://bucket/path", "https://example.com/f"]),
            Some(vec![IoBackendKind::Gcs, IoBackendKind::Http])
        );
        assert_eq!(IoBackendKind::from_uris(["local.parquet"]), None);
        assert_eq!(IoBackendKind::from_uris(["file:///tmp/x.parquet"]), None);
        // Unrecognized / alias schemes must not filter (show all non-defaults).
        assert_eq!(
            IoBackendKind::from_uris(["my-s3://bucket/path", "gs://other/path"]),
            None
        );
        assert_eq!(IoBackendKind::from_uris(["oss://bucket/path"]), None);
    }
}
