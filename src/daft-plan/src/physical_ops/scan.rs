use std::sync::Arc;

use daft_scan::ScanTask;
use itertools::Itertools;

use crate::PartitionSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TabularScan {
    pub scan_tasks: Vec<Arc<ScanTask>>,
    pub partition_spec: Arc<PartitionSpec>,
}

impl TabularScan {
    pub(crate) fn new(scan_tasks: Vec<Arc<ScanTask>>, partition_spec: Arc<PartitionSpec>) -> Self {
        Self {
            scan_tasks,
            partition_spec,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("TabularScan:".to_string());
        let num_scan_tasks = self.scan_tasks.len();
        let total_bytes: usize = self
            .scan_tasks
            .iter()
            .map(|st| st.size_bytes().unwrap_or(0))
            .sum();

        res.push(format!("Num Scan Tasks = {num_scan_tasks}",));
        res.push(format!("Estimated Scan Bytes = {total_bytes}",));

        res.push(format!(
            "Partition spec = {{ {} }}",
            self.partition_spec.multiline_display().join(", ")
        ));
        res
    }
}
