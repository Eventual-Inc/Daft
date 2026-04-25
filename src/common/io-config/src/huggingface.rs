use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
};

use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HuggingFaceConfig {
    pub token: Option<ObfuscatedString>,
    pub anonymous: bool,
    pub use_content_defined_chunking: Option<bool>,
    pub row_group_size: Option<usize>,
    pub target_filesize: usize,
    pub max_operations_per_commit: usize,
}

impl Default for HuggingFaceConfig {
    fn default() -> Self {
        Self {
            token: None,
            anonymous: false,
            use_content_defined_chunking: None,
            row_group_size: None,
            target_filesize: 512 * 1024 * 1024, // 512MB
            max_operations_per_commit: 100,
        }
    }
}

impl HuggingFaceConfig {
    pub fn to_opendal_config(
        &self,
        repo_type: &str,
        repo_id: &str,
        revision: Option<&str>,
    ) -> BTreeMap<String, String> {
        let mut config = BTreeMap::from([
            ("repo_type".to_string(), repo_type.to_string()),
            ("repo_id".to_string(), repo_id.to_string()),
        ]);

        if let Some(revision) = revision.filter(|revision| !revision.is_empty()) {
            config.insert("revision".to_string(), revision.to_string());
        }

        if !self.anonymous
            && let Some(token) = &self.token
        {
            config.insert("token".to_string(), token.as_string().clone());
        }

        config
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if let Some(token) = &self.token {
            res.push(format!("Token = {token}"));
        }
        res.push(format!("Anonymous = {}", self.anonymous));
        res
    }
}

impl Display for HuggingFaceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "HuggingFaceConfig\n{}",
            self.multiline_display().join("\n")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::HuggingFaceConfig;

    #[test]
    fn test_to_opendal_config_respects_anonymous_mode() {
        let config = HuggingFaceConfig {
            anonymous: true,
            ..Default::default()
        };

        let opendal = config.to_opendal_config("bucket", "user/bucket", None);

        assert_eq!(opendal.get("repo_type").map(String::as_str), Some("bucket"));
        assert_eq!(
            opendal.get("repo_id").map(String::as_str),
            Some("user/bucket")
        );
        assert!(!opendal.contains_key("token"));
    }
}
