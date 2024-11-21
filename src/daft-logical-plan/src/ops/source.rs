use std::sync::Arc;

use common_scan_info::PhysicalScanInfo;
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

    pub(crate) fn materialize_stats(&self) -> Self {
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
            SourceInfo::Physical(PhysicalScanInfo {
                scan_op, pushdowns, ..
            }) => {
                // Python scans are potentially scans over generators. Materializing stats for
                // these would consume the generator, leading to empty generators after planning.
                if scan_op.0.is_python_scan() {
                    ApproxStats::empty()
                } else {
                    let scan_tasks = scan_op
                        .0
                        .to_scan_tasks(pushdowns.clone(), None)
                        .expect("Failed to get scan tasks from scan operator");
                    let mut approx_stats = ApproxStats::empty();
                    for st in scan_tasks {
                        approx_stats.lower_bound_rows += st.num_rows().unwrap_or(0);
                        let in_memory_size = st.estimate_in_memory_size_bytes(None);
                        approx_stats.lower_bound_bytes += in_memory_size.unwrap_or(0);
                        if let Some(st_ub) = st.upper_bound_rows() {
                            if let Some(ub) = approx_stats.upper_bound_rows {
                                approx_stats.upper_bound_rows = Some(ub + st_ub);
                            } else {
                                approx_stats.upper_bound_rows = st.upper_bound_rows();
                            }
                        }
                        if let Some(st_ub) = in_memory_size {
                            if let Some(ub) = approx_stats.upper_bound_bytes {
                                approx_stats.upper_bound_bytes = Some(ub + st_ub);
                            } else {
                                approx_stats.upper_bound_bytes = in_memory_size;
                            }
                        }
                    }
                    approx_stats
                }
            }
            SourceInfo::PlaceHolder(_) => ApproxStats::empty(),
        };
        Self {
            output_schema: self.output_schema.clone(),
            source_info: self.source_info.clone(),
            stats_state: StatsState::Materialized(PlanStats::new(approx_stats)),
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
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
