use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_dsl::ExprRef;

use crate::source_info::SourceInfo;

#[derive(Clone, Debug)]
pub struct Source {
    /// The schema of the output of this node (the source data schema).
    /// May be a subset of the source data schema; executors should push down this projection if possible.
    pub schema: SchemaRef,

    /// Information about the source data location.
    pub source_info: Arc<SourceInfo>,

    /// Optional filters to apply to the source data.
    pub filters: Vec<ExprRef>,
    /// Optional number of rows to read.
    pub limit: Option<usize>,
}

impl Source {
    pub(crate) fn new(schema: SchemaRef, source_info: Arc<SourceInfo>) -> Self {
        Self {
            schema,
            source_info,
            filters: vec![], // Will be populated by plan optimizer.
            limit: None,     // Will be populated by plan optimizer.
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];

        use SourceInfo::*;
        match &*self.source_info {
            FilesInfo(files_info) => {
                res.push(format!("Source: {:?}", files_info.file_format));
                for fp in files_info.filepaths.iter() {
                    res.push(format!("  {}", fp));
                }
                res.push(format!(
                    "  File schema: {}",
                    files_info.schema.short_string()
                ));
            }
        }
        res.push(format!("  Output schema: {}", self.schema.short_string()));
        if self.filters.is_empty() {
            res.push(format!("  Filters: {:?}", self.filters));
        }
        if let Some(limit) = self.limit {
            res.push(format!("  Limit: {}", limit));
        }
        res
    }
}
