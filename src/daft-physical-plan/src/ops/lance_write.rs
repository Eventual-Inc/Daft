use daft_logical_plan::sink_info::LanceCatalogInfo;
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LanceWrite {
    pub schema: SchemaRef,
    pub lance_info: LanceCatalogInfo,
    // Upstream node.
    pub input: PhysicalPlanRef,
}

impl LanceWrite {
    pub(crate) fn new(
        schema: SchemaRef,
        lance_info: LanceCatalogInfo,
        input: PhysicalPlanRef,
    ) -> Self {
        Self {
            schema,
            lance_info,
            input,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("LanceWrite:".to_string());
        res.extend(self.lance_info.multiline_display());
        res
    }
}

crate::impl_default_tree_display!(LanceWrite);
