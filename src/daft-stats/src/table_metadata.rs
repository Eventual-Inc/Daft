use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct TableMetadata {
    pub length: usize,
    /// Total uncompressed size in bytes, when known from file metadata (e.g. Parquet row group totals).
    #[serde(default)]
    pub size_bytes: Option<usize>,
}

impl TableMetadata {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Length = {}", self.length));
        res
    }
}
