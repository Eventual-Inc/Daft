use std::fmt::{Display, Formatter};

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
