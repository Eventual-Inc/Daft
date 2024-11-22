use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use daft_schema::schema::SchemaRef;

use crate::stats::{ApproxStats, PlanStats, StatsState};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MaterializedScanSource {
    pub scan_tasks: Vec<ScanTaskLikeRef>,
    pub pushdowns: Pushdowns,
    pub schema: SchemaRef,
    pub stats_state: StatsState,
}

impl MaterializedScanSource {
    pub fn new(scan_tasks: Vec<ScanTaskLikeRef>, pushdowns: Pushdowns, schema: SchemaRef) -> Self {
        Self {
            scan_tasks,
            pushdowns,
            schema,
            stats_state: StatsState::NotMaterialized,
        }
    }

    pub(crate) fn with_materialized_stats(mut self) -> Self {
        let mut approx_stats = ApproxStats::empty();
        for st in &self.scan_tasks {
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
        self.stats_state = StatsState::Materialized(PlanStats::new(approx_stats));
        self
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!("Num Scan Tasks = [{}]", self.scan_tasks.len()));
        res.extend(self.pushdowns.multiline_display());
        res.push(format!("Output schema = {}", self.schema.short_string()));
        if let StatsState::Materialized(stats) = &self.stats_state {
            res.push(format!("Stats = {}", stats));
        }
        res
    }
}
