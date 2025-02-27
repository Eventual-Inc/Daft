use std::sync::Arc;

use common_display::{tree::TreeDisplay, DisplayLevel};
use daft_logical_plan::{source_info::InMemoryInfo, ClusteringSpec};
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InMemoryScan {
    pub schema: SchemaRef,
    pub in_memory_info: InMemoryInfo,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl InMemoryScan {
    pub(crate) fn new(
        schema: SchemaRef,
        in_memory_info: InMemoryInfo,
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
        res.push("InMemoryScan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!("Size bytes = {}", self.in_memory_info.size_bytes,));
        res.push(format!(
            "Clustering spec = {{ {} }}",
            self.clustering_spec.multiline_display().join(", ")
        ));
        if let Some(source_stage_id) = self.in_memory_info.source_stage_id {
            res.push(format!("Source Stage ID = {}", source_stage_id));
        }
        res
    }
}
impl TreeDisplay for InMemoryScan {
    fn display_as(&self, level: DisplayLevel) -> String {
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default => {
                format!(
                    "InMemoryScan:
Schema = {},
Size bytes = {},
Clustering spec = {{ {} }}
Source Stage ID = {}
",
                    self.schema.short_string(),
                    self.in_memory_info.size_bytes,
                    self.clustering_spec.multiline_display().join(", "),
                    match self.in_memory_info.source_stage_id {
                        Some(source_stage_id) => source_stage_id.to_string(),
                        None => "None".to_string(),
                    }
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
