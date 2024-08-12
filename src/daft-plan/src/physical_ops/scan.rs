use common_display::{tree::TreeDisplay, DisplayAs, DisplayLevel};
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
        self.description(DisplayLevel::Default)
            .lines()
            .map(|s| s.to_string())
            .collect()
    }
}

impl TreeDisplay for TabularScan {
    fn description(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        fn base_display(scan: &TabularScan) -> String {
            let num_scan_tasks = scan.scan_tasks.len();
            let total_bytes: usize = scan
                .scan_tasks
                .iter()
                .map(|st| st.size_bytes().unwrap_or(0))
                .sum();

            let clustering_spec = scan.clustering_spec.multiline_display().join(", ");
            let mut s = format!(
                "TabularScan:
Num Scan Tasks = {num_scan_tasks}
Estimated Scan Bytes = {total_bytes}
Clustering spec = {{ {clustering_spec} }}
"
            )
            .to_string();
            #[cfg(feature = "python")]
            if let FileFormatConfig::Database(config) =
                scan.scan_tasks[0].file_format_config.as_ref()
            {
                if num_scan_tasks == 1 {
                    writeln!(s, "SQL Query = {}", &config.sql).unwrap();
                } else {
                    writeln!(s, "SQL Queries = [{},..]", &config.sql).unwrap();
                }
            }
            s
        }
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default => {
                let mut s = base_display(self);
                let tasks = self.scan_tasks.iter();

                writeln!(s, "Scan Tasks: [").unwrap();
                for (i, st) in tasks.enumerate() {
                    if i < 3 || i >= self.scan_tasks.len() - 3 {
                        writeln!(s, "{}", st.as_ref().display_as(DisplayLevel::Default)).unwrap();
                    } else if i == 3 {
                        writeln!(s, "...").unwrap();
                    }
                }
                writeln!(s, "]").unwrap();

                s
            }
            DisplayLevel::Verbose => {
                let mut s = base_display(self);
                writeln!(s, "Scan Tasks: [").unwrap();

                for st in &self.scan_tasks {
                    writeln!(s, "{}", st.as_ref().display_as(DisplayLevel::Verbose)).unwrap();
                }
                s
            }
        }
    }

    fn get_name(&self) -> String {
        "TabularScan".to_string()
    }

    fn get_children(&self) -> Vec<Arc<dyn TreeDisplay>> {
        vec![]
    }
}
