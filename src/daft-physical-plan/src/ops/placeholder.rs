use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use daft_logical_plan::ClusteringSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaceholderScan {
    pub source_id: usize,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl PlaceholderScan {
    pub(crate) fn new(source_id: usize, clustering_spec: Arc<ClusteringSpec>) -> Self {
        Self {
            source_id,
            clustering_spec,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("PlaceholderScan:".to_string());
        res.push(format!("Source ID = {}", self.source_id));
        res.push(self.clustering_spec.multiline_display().join(", "));
        res
    }

    pub fn clustering_spec(&self) -> &Arc<ClusteringSpec> {
        &self.clustering_spec
    }
}
impl TreeDisplay for PlaceholderScan {
    fn display_as(&self, level: DisplayLevel) -> String {
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default => {
                format!(
                    "PlaceholderScan:
Source ID = {}
Clustering spec = {{ {} }}",
                    self.source_id,
                    self.clustering_spec.multiline_display().join(", ")
                )
            }
            DisplayLevel::Verbose => todo!(),
        }
    }

    fn get_name(&self) -> String {
        format!("PlaceholderScan: Source ID = {}", self.source_id)
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}
