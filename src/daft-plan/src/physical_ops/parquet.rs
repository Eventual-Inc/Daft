use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_dsl::ExprRef;

use crate::{source_info::SourceInfo, PartitionSpec};

#[derive(Debug)]
pub struct TabularScanParquet {
    pub schema: SchemaRef,
    pub source_info: Arc<SourceInfo>,
    pub partition_spec: Arc<PartitionSpec>,
    pub limit: Option<usize>,
    pub filters: Vec<ExprRef>,
}

impl TabularScanParquet {
    pub(crate) fn new(
        schema: SchemaRef,
        source_info: Arc<SourceInfo>,
        partition_spec: Arc<PartitionSpec>,
        limit: Option<usize>,
        filters: Vec<ExprRef>,
    ) -> Self {
        Self {
            schema,
            source_info,
            partition_spec,
            limit,
            filters,
        }
    }
}
