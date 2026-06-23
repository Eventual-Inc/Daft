use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct TableMetadata {
    pub length: usize,
    /// Uncompressed size in bytes per top-level column, when known from file metadata
    /// (e.g. summed Parquet column-chunk totals across row groups). Keyed by column name
    /// so that size estimates can respect column-projection pushdown. `None` when the
    /// source does not expose per-column sizes.
    #[serde(default)]
    pub column_sizes: Option<BTreeMap<String, u64>>,
}

impl TableMetadata {
    #[must_use]
    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Length = {}", self.length));
        res
    }
}
