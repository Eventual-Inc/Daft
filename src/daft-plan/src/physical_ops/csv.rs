use std::sync::Arc;

use daft_core::schema::SchemaRef;
use daft_dsl::ExprRef;

use crate::{source_info::ExternalInfo, PartitionSpec};

#[derive(Debug)]
pub struct TabularScanCsv {
    pub schema: SchemaRef,
    pub external_info: ExternalInfo,
    pub partition_spec: Arc<PartitionSpec>,
    pub limit: Option<usize>,
    pub filters: Vec<ExprRef>,
}

impl TabularScanCsv {
    pub(crate) fn new(
        schema: SchemaRef,
        external_info: ExternalInfo,
        partition_spec: Arc<PartitionSpec>,
        limit: Option<usize>,
        filters: Vec<ExprRef>,
    ) -> Self {
        Self {
            schema,
            external_info,
            partition_spec,
            limit,
            filters,
        }
    }
}
