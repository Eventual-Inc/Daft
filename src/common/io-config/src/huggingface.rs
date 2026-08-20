use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::ObfuscatedString;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HuggingFaceConfig {
    pub token: Option<ObfuscatedString>,
    pub anonymous: bool,
    /// When true, attempt to read Xet-backed files via the Xet protocol before
    /// falling back to the standard HTTP resolve path.
    pub use_xet: bool,
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
            use_xet: true,
            use_content_defined_chunking: None,
            row_group_size: None,
            target_filesize: 512 * 1024 * 1024, // 512MB
            max_operations_per_commit: 100,
        }
    }
}

impl HuggingFaceConfig {
    pub fn multiline_display(&self) -> Vec<String> {
        let defaults = Self::default();
        let mut res = vec![];
        if let Some(token) = &self.token {
            res.push(format!("Token = {token}"));
        }
        if self.anonymous != defaults.anonymous {
            res.push(format!("Anonymous = {}", self.anonymous));
        }
        if self.use_xet != defaults.use_xet {
            res.push(format!("Use Xet = {}", self.use_xet));
        }
        if self.use_content_defined_chunking != defaults.use_content_defined_chunking {
            res.push(format!(
                "Use content defined chunking = {:?}",
                self.use_content_defined_chunking
            ));
        }
        if self.row_group_size != defaults.row_group_size {
            res.push(format!("Row group size = {:?}", self.row_group_size));
        }
        if self.target_filesize != defaults.target_filesize {
            res.push(format!("Target filesize = {}", self.target_filesize));
        }
        if self.max_operations_per_commit != defaults.max_operations_per_commit {
            res.push(format!(
                "Max operations per commit = {}",
                self.max_operations_per_commit
            ));
        }
        res
    }
}

impl Display for HuggingFaceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let lines = self.multiline_display();
        if lines.is_empty() {
            write!(f, "HuggingFaceConfig {{}}")
        } else {
            write!(f, "HuggingFaceConfig\n{}", lines.join("\n"))
        }
    }
}
