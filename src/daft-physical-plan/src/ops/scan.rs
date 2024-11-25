use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayAs, DisplayLevel};
use common_file_formats::FileFormatConfig;
use common_scan_info::ScanTaskLikeRef;
use daft_logical_plan::partitioning::ClusteringSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabularScan {
    pub scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl TabularScan {
    pub(crate) fn new(
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
        clustering_spec: Arc<ClusteringSpec>,
    ) -> Self {
        Self {
            scan_tasks,
            clustering_spec,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        self.display_as(DisplayLevel::Default)
            .lines()
            .map(|s| s.to_string())
            .collect()
    }
}

impl TreeDisplay for TabularScan {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        fn base_display(scan: &TabularScan) -> String {
            let num_scan_tasks = scan.scan_tasks.len();
            let total_bytes: usize = scan
                .scan_tasks
                .iter()
                .map(|st| st.size_bytes_on_disk().unwrap_or(0))
                .sum();

            let clustering_spec = scan.clustering_spec.multiline_display().join(", ");
            #[allow(unused_mut)]
            let mut s = format!(
                "TabularScan:
Num Scan Tasks = {num_scan_tasks}
Estimated Scan Bytes = {total_bytes}
Clustering spec = {{ {clustering_spec} }}
"
            );
            #[cfg(feature = "python")]
            if let FileFormatConfig::Database(config) =
                scan.scan_tasks[0].file_format_config().as_ref()
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
                // We're only going to display the pushdowns and schema for the first scan task.
                let pushdown = self.scan_tasks[0].pushdowns();
                if !pushdown.is_empty() {
                    s.push_str(&pushdown.display_as(DisplayLevel::Compact));
                    s.push('\n');
                }

                let schema = self.scan_tasks[0].schema();
                writeln!(
                    s,
                    "Schema: {{{}}}",
                    schema.display_as(DisplayLevel::Compact)
                )
                .unwrap();

                let tasks = self.scan_tasks.iter();

                writeln!(s, "Scan Tasks: [").unwrap();
                for (i, st) in tasks.enumerate() {
                    if i < 3 || i >= self.scan_tasks.len() - 3 {
                        writeln!(s, "{}", st.as_ref().display_as(DisplayLevel::Compact)).unwrap();
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

                for st in self.scan_tasks.iter() {
                    writeln!(s, "{}", st.as_ref().display_as(DisplayLevel::Verbose)).unwrap();
                }
                s
            }
        }
    }

    fn get_name(&self) -> String {
        "TabularScan".to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}
