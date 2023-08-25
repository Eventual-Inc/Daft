use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_dsl::ExprRef;

use crate::{
    physical_plan::PhysicalPlan, sink_info::OutputFileInfo,
    source_info::ExternalInfo as ExternalSourceInfo, PartitionSpec,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TabularScanParquet {
    pub projection_schema: SchemaRef,
    pub external_info: ExternalSourceInfo,
    pub partition_spec: Arc<PartitionSpec>,
    pub limit: Option<usize>,
    pub filters: Vec<ExprRef>,
}

impl TabularScanParquet {
    pub(crate) fn new(
        projection_schema: SchemaRef,
        external_info: ExternalSourceInfo,
        partition_spec: Arc<PartitionSpec>,
        limit: Option<usize>,
        filters: Vec<ExprRef>,
    ) -> Self {
        Self {
            projection_schema,
            external_info,
            partition_spec,
            limit,
            filters,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TabularWriteParquet {
    pub schema: SchemaRef,
    pub file_info: OutputFileInfo,
    // Upstream node.
    pub input: Arc<PhysicalPlan>,
}

impl TabularWriteParquet {
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
