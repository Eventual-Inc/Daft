use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_scan::PhysicalScanInfo;

use crate::source_info::SourceInfo;

use crate::source_info::InMemoryInfo;
use crate::source_info::PlaceHolderInfo;

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
            SourceInfo::Physical(PhysicalScanInfo {
                source_schema,
                scan_op,
                partitioning_keys,
                pushdowns,
            }) => {
                use itertools::Itertools;
                res.extend(scan_op.0.multiline_display());

                res.push(format!("File schema = {}", source_schema.short_string()));
                res.push(format!(
                    "Partitioning keys = [{}]",
                    partitioning_keys.iter().map(|k| format!("{k}")).join(" ")
                ));
                res.extend(pushdowns.multiline_display());
            }
            SourceInfo::InMemory(InMemoryInfo { num_partitions, .. }) => {
                res.push("Source:".to_string());
                res.push(format!("Number of partitions = {}", num_partitions));
            }
            SourceInfo::PlaceHolder(PlaceHolderInfo {
                source_id,
                clustering_spec,
                ..
            }) => {
                res.push("PlaceHolder:".to_string());
                res.push(format!("Source ID = {}", source_id));
                res.extend(clustering_spec.multiline_display());
            }
        }
        res.push(format!(
            "Output schema = {}",
            self.output_schema.short_string()
        ));
        res
    }
}
