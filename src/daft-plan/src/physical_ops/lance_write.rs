use daft_core::schema::SchemaRef;

use crate::{physical_plan::PhysicalPlanRef, sink_info::LanceCatalogInfo};
use serde::{Deserialize, Serialize};

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
