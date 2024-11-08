use std::sync::Arc;

use common_display::tree::TreeDisplay;
use daft_logical_plan::partitioning::ClusteringSpec;
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmptyScan {
    pub schema: SchemaRef,
    pub clustering_spec: Arc<ClusteringSpec>,
}

impl EmptyScan {
    pub(crate) fn new(schema: SchemaRef, clustering_spec: Arc<ClusteringSpec>) -> Self {
        Self {
            schema,
            clustering_spec,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("EmptyScan:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.push(format!(
            "Clustering spec = {{ {} }}",
            self.clustering_spec.multiline_display().join(", ")
        ));
        res
    }
}

impl TreeDisplay for EmptyScan {
    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        match level {
            common_display::DisplayLevel::Compact => self.get_name(),
            _ => self.multiline_display().join("\n"),
        }
    }

    fn get_name(&self) -> String {
        "EmptyScan".to_string()
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}
