use daft_logical_plan::sink_info::DeltaLakeCatalogInfo;
use daft_schema::schema::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::PhysicalPlanRef;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeltaLakeWrite {
    pub schema: SchemaRef,
    pub delta_lake_info: DeltaLakeCatalogInfo,
    // Upstream node.
    pub input: PhysicalPlanRef,
}

impl DeltaLakeWrite {
    pub(crate) fn new(
        schema: SchemaRef,
        delta_lake_info: DeltaLakeCatalogInfo,
        input: PhysicalPlanRef,
    ) -> Self {
        Self {
            schema,
            delta_lake_info,
            input,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("DeltaLakeWrite:".to_string());
        res.extend(self.delta_lake_info.multiline_display());
        res
    }
}

crate::impl_default_tree_display!(DeltaLakeWrite);
