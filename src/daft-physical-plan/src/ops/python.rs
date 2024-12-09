use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use daft_logical_plan::{source_info::PythonInfo, ClusteringSpec};
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonScan {
    pub schema: SchemaRef,
    pub in_memory_info: PythonInfo,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl PythonScan {
    pub(crate) fn new(
        schema: SchemaRef,
        in_memory_info: PythonInfo,
        clustering_spec: Arc<ClusteringSpec>,
    ) -> Self {
        Self {
            schema,
            in_memory_info,
            clustering_spec,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("PythonScan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!("Size bytes = {}", self.in_memory_info.size_bytes,));
        res.push(format!(
            "Clustering spec = {{ {} }}",
            self.clustering_spec.multiline_display().join(", ")
        ));
        res
    }
}
impl TreeDisplay for PythonScan {
    fn display_as(&self, level: DisplayLevel) -> String {
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default => {
                format!(
                    "PythonScan:
Schema = {},
Size bytes = {},
Clustering spec = {{ {} }}",
                    self.schema.short_string(),
                    self.in_memory_info.size_bytes,
                    self.clustering_spec.multiline_display().join(", ")
                )
            }
            DisplayLevel::Verbose => todo!(),
        }
    }

    fn get_name(&self) -> String {
        "InMemoryScan".to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}
