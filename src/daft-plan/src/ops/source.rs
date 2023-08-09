use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_dsl::ExprRef;

use crate::{
    source_info::{ExternalInfo, SourceInfo},
    PartitionSpec,
};

#[derive(Clone, Debug)]
pub struct Source {
    /// The schema of the output of this node (the source data schema).
    /// May be a subset of the source data schema; executors should push down this projection if possible.
    pub schema: SchemaRef,

    /// Information about the source data location.
    pub source_info: Arc<SourceInfo>,

    pub partition_spec: Arc<PartitionSpec>,

    /// Optional filters to apply to the source data.
    pub filters: Vec<ExprRef>,
    /// Optional number of rows to read.
    pub limit: Option<usize>,
}

impl Source {
    pub(crate) fn new(
        schema: SchemaRef,
        source_info: Arc<SourceInfo>,
        partition_spec: Arc<PartitionSpec>,
    ) -> Self {
        Self {
            schema,
            source_info,
            partition_spec,
            filters: vec![], // Will be populated by plan optimizer.
            limit: None,     // Will be populated by plan optimizer.
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];

        match self.source_info.as_ref() {
            SourceInfo::ExternalInfo(ExternalInfo {
                schema,
                file_info,
                file_format_config,
            }) => {
                res.push(format!("Source: {:?}", file_format_config.var_name()));
                for fp in file_info.file_paths.iter() {
                    res.push(format!("  {}", fp));
                }
                res.push(format!("  File schema: {}", schema.short_string()));
                res.push(format!(
                    "  Format-specific config: {:?}",
                    file_format_config
                ));
            }
            #[cfg(feature = "python")]
            SourceInfo::InMemoryInfo(_) => {}
        }
        res.push(format!("  Output schema: {}", self.schema.short_string()));
        if !self.filters.is_empty() {
            res.push(format!("  Filters: {:?}", self.filters));
        }
        if let Some(limit) = self.limit {
            res.push(format!("  Limit: {}", limit));
        }
        res
    }
}
