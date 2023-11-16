use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_scan::Pushdowns;

use crate::{
    physical_plan::PhysicalPlan, sink_info::OutputFileInfo, source_info::LegacyExternalInfo,
    PartitionSpec,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TabularScanCsv {
    pub projection_schema: SchemaRef,
    pub external_info: LegacyExternalInfo,
    pub partition_spec: Arc<PartitionSpec>,
    pub pushdowns: Pushdowns,
}

impl TabularScanCsv {
    pub(crate) fn new(
        projection_schema: SchemaRef,
        external_info: LegacyExternalInfo,
        partition_spec: Arc<PartitionSpec>,
        pushdowns: Pushdowns,
    ) -> Self {
        Self {
            projection_schema,
            external_info,
            partition_spec,
            pushdowns,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TabularWriteCsv {
    pub schema: SchemaRef,
    pub file_info: OutputFileInfo,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl TabularWriteCsv {
    pub(crate) fn new(
        schema: SchemaRef,
        file_info: OutputFileInfo,
        input: Arc<PhysicalPlan>,
    ) -> Self {
        Self {
            schema,
            file_info,
            input,
        }
    }
}
