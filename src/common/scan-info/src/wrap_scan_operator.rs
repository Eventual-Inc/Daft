use std::{fmt::Debug, sync::Arc};

use common_error::DaftResult;
use daft_schema::schema::SchemaRef;

use crate::{PartitionField, Pushdowns, ScanOperator, ScanTaskLikeRef};

#[derive(Debug)]
pub struct PushdownEnabledScanOperator {
    delegate: Arc<dyn ScanOperator>,
}

impl PushdownEnabledScanOperator {
    pub fn new(delegate: Arc<dyn ScanOperator>) -> Self {
        Self { delegate }
    }
}

impl ScanOperator for PushdownEnabledScanOperator {
    fn name(&self) -> &str {
        self.delegate.name()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        self.delegate.partitioning_keys()
    }

    fn file_path_column(&self) -> Option<&str> {
        self.delegate.file_path_column()
    }

    fn generated_fields(&self) -> Option<SchemaRef> {
        self.delegate.generated_fields()
    }

    fn can_absorb_filter(&self) -> bool {
        self.delegate.can_absorb_filter()
    }

    fn can_absorb_select(&self) -> bool {
        self.delegate.can_absorb_select()
    }

    fn can_absorb_limit(&self) -> bool {
        self.delegate.can_absorb_limit()
    }

    fn can_absorb_shard(&self) -> bool {
        self.delegate.can_absorb_shard()
    }

    fn multiline_display(&self) -> Vec<String> {
        self.delegate.multiline_display()
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
        self.delegate.to_scan_tasks(pushdowns)
    }

    fn schema(&self) -> SchemaRef {
        self.delegate.schema()
    }

    fn is_python_scanner(&self) -> bool {
        false
    }
}
