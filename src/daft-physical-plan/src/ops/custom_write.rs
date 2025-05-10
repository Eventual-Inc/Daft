use daft_logical_plan::sink_info::CustomInfo;
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CustomWrite {
    pub schema: SchemaRef,
    pub custom_info: CustomInfo,
    // Upstream node.
    pub input: PhysicalPlanRef,
}

impl CustomWrite {
    pub(crate) fn new(schema: SchemaRef, custom_info: CustomInfo, input: PhysicalPlanRef) -> Self {
        Self {
            schema,
            custom_info,
            input,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("CustomWrite:".to_string());
        res.extend(self.custom_info.multiline_display());
        res
    }
}

crate::impl_default_tree_display!(CustomWrite);
