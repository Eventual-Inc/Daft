use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use daft_logical_plan::ClusteringSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreviousStageScan {
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl PreviousStageScan {
    pub(crate) fn new(clustering_spec: Arc<ClusteringSpec>) -> Self {
        Self { clustering_spec }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![
            "PreviousStageScan".to_string(),
            self.clustering_spec.multiline_display().join(", "),
        ]
    }

    pub fn clustering_spec(&self) -> &Arc<ClusteringSpec> {
        &self.clustering_spec
    }
}
impl TreeDisplay for PreviousStageScan {
    fn display_as(&self, level: DisplayLevel) -> String {
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default => {
                format!(
                    "PreviousStageScan:
Clustering spec = {{ {} }}",
                    self.clustering_spec.multiline_display().join(", ")
                )
            }
            DisplayLevel::Verbose => todo!(),
        }
    }

    fn get_name(&self) -> String {
        "PreviousStageScan".to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}
