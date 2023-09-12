use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_dsl::ExprRef;

use crate::{
    source_info::{ExternalInfo, SourceInfo},
    PartitionSpec,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Source {
    /// The schema of the output of this node (the source data schema).
    /// May be a subset of the source data schema; executors should push down this projection if possible.
    pub output_schema: SchemaRef,

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
        output_schema: SchemaRef,
        source_info: Arc<SourceInfo>,
        partition_spec: Arc<PartitionSpec>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            output_schema,
            source_info,
            partition_spec,
            limit,
            filters: vec![], // Will be populated by plan optimizer.
        }
    }

    pub fn with_limit(&self, limit: Option<usize>) -> Self {
        Self {
            output_schema: self.output_schema.clone(),
            source_info: self.source_info.clone(),
            partition_spec: self.partition_spec.clone(),
            filters: self.filters.clone(),
            limit,
        }
    }

    pub fn with_filters(&self, filters: Vec<ExprRef>) -> Self {
        Self {
            output_schema: self.output_schema.clone(),
            source_info: self.source_info.clone(),
            partition_spec: self.partition_spec.clone(),
            filters,
            limit: self.limit,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];

        match self.source_info.as_ref() {
            SourceInfo::ExternalInfo(ExternalInfo {
                source_schema,
                file_infos,
                file_format_config,
                storage_config,
            }) => {
                res.push(format!("Source: {}", file_format_config.var_name()));
                res.push(format!(
                    "File paths = [{}]",
                    file_infos.file_paths.join(", ")
                ));
                res.push(format!("File schema = {}", source_schema.short_string()));
                res.push(format!("Format-specific config = {:?}", file_format_config));
                res.push(format!("Storage config = {:?}", storage_config));
            }
            #[cfg(feature = "python")]
            SourceInfo::InMemoryInfo(_) => {}
        }
        res.push(format!(
            "Output schema = {}",
            self.output_schema.short_string()
        ));
        if !self.filters.is_empty() {
            res.push(format!(
                "Filters = {}",
                self.filters
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(limit) = self.limit {
            res.push(format!("Limit = {}", limit));
        }
        res
    }
}
