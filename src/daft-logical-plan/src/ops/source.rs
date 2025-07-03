use std::sync::Arc;

use common_error::DaftResult;
use common_scan_info::{PhysicalScanInfo, ScanState};
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::{
    source_info::{InMemoryInfo, PlaceHolderInfo, SourceInfo},
    stats::{ApproxStats, PlanStats, StatsState},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Source {
    pub plan_id: Option<usize>,
    pub node_id: Option<usize>,
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
            plan_id: None,
            node_id: None,
            output_schema,
            source_info,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub fn with_plan_id(mut self, plan_id: usize) -> Self {
        self.plan_id = Some(plan_id);
        self
    }

    pub fn with_node_id(mut self, node_id: usize) -> Self {
        self.node_id = Some(node_id);
        self
    }

    // Helper method that converts the ScanOperatorRef inside a Source node's PhysicalScanInfo into scan tasks.
    // Should only be called if a Source node's source info contains PhysicalScanInfo. The PhysicalScanInfo
    // should also hold a ScanState::Operator and not a ScanState::Tasks (which would indicate that we're
    // materializing this physical scan node multiple times).
    pub(crate) fn build_materialized_scan_source(mut self) -> DaftResult<Self> {
        let new_physical_scan_info = match Arc::unwrap_or_clone(self.source_info) {
            SourceInfo::Physical(mut physical_scan_info) => {
                let scan_tasks = match &physical_scan_info.scan_state {
                    ScanState::Operator(scan_op) => scan_op
                        .0
                        .to_scan_tasks(physical_scan_info.pushdowns.clone())?,
                    ScanState::Tasks(_) => {
                        panic!("Physical scan nodes are being materialized more than once");
                    }
                };
                physical_scan_info.scan_state = ScanState::Tasks(Arc::new(scan_tasks));
                physical_scan_info
            }
            _ => panic!("Only unmaterialized physical scan nodes can be materialized"),
        };
        self.source_info = Arc::new(SourceInfo::Physical(new_physical_scan_info));
        Ok(self)
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let approx_stats = match &*self.source_info {
            SourceInfo::InMemory(InMemoryInfo {
                size_bytes,
                num_rows,
                ..
            }) => ApproxStats {
                num_rows: *num_rows,
                size_bytes: *size_bytes,
                acc_selectivity: 1.0,
            },
            SourceInfo::Physical(physical_scan_info) => match &physical_scan_info.scan_state {
                ScanState::Operator(_) => {
                    panic!("Scan nodes should be materialized before stats are materialized")
                }
                ScanState::Tasks(scan_tasks) => {
                    let mut approx_stats = ApproxStats::empty();
                    for st in scan_tasks.iter() {
                        if let Some(num_rows) = st.num_rows() {
                            approx_stats.num_rows += num_rows;
                        } else if let Some(approx_num_rows) = st.approx_num_rows(None) {
                            approx_stats.num_rows += approx_num_rows as usize;
                        }
                        approx_stats.size_bytes +=
                            st.estimate_in_memory_size_bytes(None).unwrap_or(0);
                    }
                    approx_stats.acc_selectivity = physical_scan_info
                        .pushdowns
                        .estimated_selectivity(self.output_schema.as_ref());
                    approx_stats
                }
            },
            SourceInfo::PlaceHolder(_) => ApproxStats::empty(),
        };
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats).into());
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];

        match self.source_info.as_ref() {
            SourceInfo::Physical(PhysicalScanInfo {
                source_schema,
                scan_state,
                partitioning_keys,
                pushdowns,
            }) => {
                use itertools::Itertools;
                res.extend(scan_state.multiline_display());

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
                clustering_spec, ..
            }) => {
                res.push("PlaceHolder:".to_string());
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
