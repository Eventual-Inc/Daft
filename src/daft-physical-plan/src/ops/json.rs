use daft_logical_plan::sink_info::OutputFileInfo;
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TabularWriteJson {
    pub schema: SchemaRef,
    pub file_info: OutputFileInfo,
    // Upstream node.
    pub input: PhysicalPlanRef,
}

impl TabularWriteJson {
    pub(crate) fn new(
        schema: SchemaRef,
        file_info: OutputFileInfo,
        input: PhysicalPlanRef,
    ) -> Self {
        Self {
            schema,
            file_info,
            input,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("TabularWriteJson:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.extend(self.file_info.multiline_display());
        res
    }
}

crate::impl_default_tree_display!(TabularWriteJson);
