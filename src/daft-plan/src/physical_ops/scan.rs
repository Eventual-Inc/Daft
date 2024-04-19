use daft_scan::{file_format::FileFormatConfig, ScanTask};
use std::sync::Arc;

use crate::ClusteringSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TabularScan {
    pub scan_tasks: Vec<Arc<ScanTask>>,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl TabularScan {
    pub(crate) fn new(
        scan_tasks: Vec<Arc<ScanTask>>,
        clustering_spec: Arc<ClusteringSpec>,
    ) -> Self {
        Self {
            scan_tasks,
            clustering_spec,
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

        #[cfg(feature = "python")]
        if let FileFormatConfig::Database(config) = self.scan_tasks[0].file_format_config.as_ref() {
            if num_scan_tasks == 1 {
                res.push(format!("SQL Query = {}", config.sql));
            } else {
                res.push(format!("SQL Queries = [{},..]", config.sql));
            }
        }

        res.push(format!(
            "Clustering spec = {{ {} }}",
            self.clustering_spec.multiline_display().join(", ")
        ));
        res
    }
}
