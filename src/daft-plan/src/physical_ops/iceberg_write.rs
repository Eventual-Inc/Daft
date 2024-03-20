use daft_core::schema::SchemaRef;

use crate::{physical_plan::PhysicalPlanRef, sink_info::IcebergCatalogInfo};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]

pub struct IcebergWrite {
    pub schema: SchemaRef,
    pub iceberg_info: IcebergCatalogInfo,
    // Upstream node.
    pub input: PhysicalPlanRef,
}

impl IcebergWrite {
    pub(crate) fn new(
        schema: SchemaRef,
        iceberg_info: IcebergCatalogInfo,
        input: PhysicalPlanRef,
    ) -> Self {
        Self {
            schema,
            iceberg_info,
            input,
        }
    }

    pub fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("IcebergWrite:".to_string());
        res.push(format!("Schema = {}", self.schema.short_string()));
        res.extend(self.iceberg_info.multiline_display());
        res
    }
}
