//! ScanOperator implementation for native extension sources.

use std::sync::Arc;

use common_error::DaftResult;
use common_file_formats::FileFormatConfig;
use common_scan_info::{PartitionField, Pushdowns, ScanTaskLikeRef};
use daft_schema::schema::SchemaRef;

use crate::{DataSource, ScanTask, storage_config::StorageConfig};

/// A `ScanOperator` backed by a native extension source.
///
/// Created when the user calls `read_source()`. The operator caches the
/// schema and task count (obtained from the extension at planning time)
/// and produces one `ScanTask` per partition with `DataSource::Extension`
/// and `FileFormatConfig::Extension`.
#[derive(Debug)]
pub struct ExtensionScanOperator {
    source_name: String,
    options: String,
    schema: SchemaRef,
    num_tasks: u32,
}

impl ExtensionScanOperator {
    pub fn new(
        source_name: impl Into<String>,
        options: impl Into<String>,
        schema: SchemaRef,
        num_tasks: u32,
    ) -> Self {
        Self {
            source_name: source_name.into(),
            options: options.into(),
            schema,
            num_tasks,
        }
    }
}

impl common_scan_info::ScanOperator for ExtensionScanOperator {
    fn name(&self) -> &str {
        "ExtensionScanOperator"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &[]
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
        true
    }

    fn can_absorb_limit(&self) -> bool {
        true
    }

    fn can_absorb_shard(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("ExtensionScanOperator({})", self.source_name),
            format!("Tasks = {}", self.num_tasks),
            format!("Schema = {}", self.schema.short_string()),
        ]
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
        let file_format_config = Arc::new(FileFormatConfig::Extension {
            source_name: self.source_name.clone(),
        });
        let storage_config = Arc::new(StorageConfig::default());

        let tasks: Vec<ScanTaskLikeRef> = (0..self.num_tasks)
            .map(|task_index| {
                let source = DataSource::Extension {
                    source_name: self.source_name.clone(),
                    options: self.options.clone(),
                    task_index,
                    size_bytes: None,
                    metadata: None,
                    statistics: None,
                };
                let scan_task = ScanTask::new(
                    vec![source],
                    file_format_config.clone(),
                    self.schema.clone(),
                    storage_config.clone(),
                    pushdowns.clone(),
                    None,
                );
                Arc::new(scan_task) as ScanTaskLikeRef
            })
            .collect();

        Ok(tasks)
    }
}
