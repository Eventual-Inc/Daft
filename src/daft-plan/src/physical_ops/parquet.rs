use daft_core::schema::SchemaRef;

use crate::{physical_plan::PhysicalPlanRef, sink_info::OutputFileInfo};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TabularWriteParquet {
    pub schema: SchemaRef,
    pub file_info: OutputFileInfo,
    // Upstream node.
    pub input: PhysicalPlanRef,
}

impl TabularWriteParquet {
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
        res.push("TabularWriteParquet:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.extend(self.file_info.multiline_display());
        res
    }
}
