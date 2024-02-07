use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_scan::ScanExternalInfo;

use crate::source_info::{ExternalInfo, SourceInfo};

#[cfg(feature = "python")]
use crate::source_info::InMemoryInfo;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Source {
    /// The schema of the output of this node (the source data schema).
    /// May be a subset of the source data schema; executors should push down this projection if possible.
    pub output_schema: SchemaRef,

    /// Information about the source data location.
    pub source_info: Arc<SourceInfo>,
}

impl Source {
    pub(crate) fn new(output_schema: SchemaRef, source_info: Arc<SourceInfo>) -> Self {
        Self {
            output_schema,
            source_info,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];

        match self.source_info.as_ref() {
            SourceInfo::ExternalInfo(ExternalInfo::Legacy(legacy_external_info)) => {
                res.push(format!(
                    "Source: {}",
                    legacy_external_info.file_format_config.var_name()
                ));
                res.extend(legacy_external_info.multiline_display());
            }
            SourceInfo::ExternalInfo(ExternalInfo::Scan(ScanExternalInfo {
                source_schema,
                scan_op,
                partitioning_keys,
                pushdowns,
            })) => {
                use itertools::Itertools;
                res.push(format!(
                    "Source: {}",
                    scan_op.0.multiline_display().join(", ")
                ));

                res.push(format!("File schema = {}", source_schema.short_string()));
                res.push(format!(
                    "Partitioning keys = [{}]",
                    partitioning_keys.iter().map(|k| format!("{k}")).join(" ")
                ));
                res.extend(pushdowns.multiline_display());
            }
            #[cfg(feature = "python")]
            SourceInfo::InMemoryInfo(InMemoryInfo { num_partitions, .. }) => {
                res.push("Source:".to_string());
                res.push(format!("Number of partitions = {}", num_partitions));
            }
        }
        res.push(format!(
            "Output schema = {}",
            self.output_schema.short_string()
        ));
        res
    }
}
