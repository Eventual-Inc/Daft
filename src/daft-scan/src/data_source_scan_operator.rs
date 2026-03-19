//! Thin [`ScanOperator`] carrier for an [`Arc<dyn DataSource>`].
//!
//! Exists solely so a `DataSource` can flow through the logical plan
//! (which speaks `ScanOperatorRef`). The execution engine calls
//! [`as_data_source`](ScanOperator::as_data_source) to extract the inner
//! `DataSource` and never calls `to_scan_tasks`.

use std::sync::Arc;

use common_error::DaftResult;
use daft_schema::schema::SchemaRef;

use crate::{
    DataSource, PartitionField, Pushdowns, ScanOperator, ScanTaskRef, SupportsPushdownFilters,
};

#[derive(Debug)]
pub struct DataSourceScanOperator {
    data_source: Arc<dyn DataSource>,
    name: String,
    schema: SchemaRef,
    partitioning_keys: Vec<PartitionField>,
}

impl DataSourceScanOperator {
    pub fn new(data_source: Arc<dyn DataSource>) -> Self {
        let name = data_source.name();
        let schema = data_source.schema();
        let partitioning_keys = data_source.partition_fields();
        Self {
            data_source,
            name,
            schema,
            partitioning_keys,
        }
    }
}

impl ScanOperator for DataSourceScanOperator {
    fn name(&self) -> &str {
        &self.name
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn partitioning_keys(&self) -> &[PartitionField] {
        &self.partitioning_keys
    }
    fn file_path_column(&self) -> Option<&str> {
        None
    }
    fn generated_fields(&self) -> Option<SchemaRef> {
        None
    }
    fn can_absorb_filter(&self) -> bool {
        false
    }
    fn can_absorb_select(&self) -> bool {
        false
    }
    fn can_absorb_limit(&self) -> bool {
        false
    }
    fn can_absorb_shard(&self) -> bool {
        false
    }
    fn multiline_display(&self) -> Vec<String> {
        vec![format!("DataSource: {}", self.name)]
    }
    fn as_pushdown_filter(&self) -> Option<&dyn SupportsPushdownFilters> {
        None
    }
    fn as_data_source(&self) -> Option<Arc<dyn DataSource>> {
        Some(self.data_source.clone())
    }
    fn to_scan_tasks(&self, _pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskRef>> {
        Err(common_error::DaftError::InternalError(
            "DataSourceScanOperator does not produce ScanTasks; \
             use as_data_source() instead"
                .into(),
        ))
    }
}
