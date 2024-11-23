use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_scan_info::{PhysicalScanInfo, ScanState};
use daft_schema::schema::SchemaRef;

use crate::{
    source_info::{InMemoryInfo, PlaceHolderInfo, SourceInfo},
    stats::{ApproxStats, PlanStats, StatsState},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Source {
    /// The schema of the output of this node (the source data schema).
    /// May be a subset of the source data schema; executors should push down this projection if possible.
    pub output_schema: SchemaRef,

    /// Information about the source data location.
    pub source_info: Arc<SourceInfo>,
    pub stats_state: StatsState,
}

impl Source {
    pub fn new(output_schema: SchemaRef, source_info: Arc<SourceInfo>) -> Self {
        Self {
            output_schema,
            source_info,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub(crate) fn build_materialized_scan_source(
        mut self,
        execution_config: Option<&DaftExecutionConfig>,
    ) -> Self {
        if let Some(scan_info) = Arc::get_mut(&mut self.source_info) {
            match scan_info {
                SourceInfo::Physical(physical_scan_info) => {
                    match &mut physical_scan_info.scan_state {
                        ScanState::Operator(scan_op) => {
                            let scan_tasks = scan_op
                                .0
                                .to_scan_tasks(
                                    physical_scan_info.pushdowns.clone(),
                                    execution_config,
                                )
                                .expect("Failed to get scan tasks from scan operator");
                            physical_scan_info.scan_state = ScanState::Tasks(scan_tasks);
                        }
                        ScanState::Tasks(_) => {
                            panic!("Physical scan nodes are being materialized more than once");
                        }
                    }
                }
                _ => panic!("Only unmaterialized physical scan nodes can be materialized"),
            }
        }
        self
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let approx_stats = match &*self.source_info {
            SourceInfo::InMemory(InMemoryInfo {
                size_bytes,
                num_rows,
                ..
            }) => ApproxStats {
                lower_bound_rows: *num_rows,
                upper_bound_rows: Some(*num_rows),
                lower_bound_bytes: *size_bytes,
                upper_bound_bytes: Some(*size_bytes),
            },
            SourceInfo::Physical(_) => {
                panic!("Scan nodes should be materialized before stats are materialized")
            }
            SourceInfo::PlaceHolder(_) => ApproxStats::empty(),
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats));
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];

        match self.source_info.as_ref() {
            SourceInfo::Physical(PhysicalScanInfo {
                source_schema,
                scan_state: scan_op,
                partitioning_keys,
                pushdowns,
            }) => {
                use itertools::Itertools;
                res.extend(scan_op.multiline_display());

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
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
