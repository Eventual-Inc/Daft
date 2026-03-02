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
    #[allow(clippy::unnecessary_literal_bound)]
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_scan_info::{Pushdowns, ScanOperator};
    use daft_schema::{dtype::DataType, field::Field, schema::Schema};

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("name", DataType::Utf8),
        ]))
    }

    #[test]
    fn operator_name() {
        let op = ExtensionScanOperator::new("test_src", "{}", test_schema(), 4);
        assert_eq!(op.name(), "ExtensionScanOperator");
    }

    #[test]
    fn operator_schema() {
        let schema = test_schema();
        let op = ExtensionScanOperator::new("test_src", "{}", schema.clone(), 4);
        assert_eq!(op.schema(), schema);
    }

    #[test]
    fn operator_capabilities() {
        let op = ExtensionScanOperator::new("test_src", "{}", test_schema(), 1);
        assert!(op.can_absorb_select());
        assert!(op.can_absorb_limit());
        assert!(!op.can_absorb_filter());
        assert!(!op.can_absorb_shard());
        assert!(op.partitioning_keys().is_empty());
        assert!(op.file_path_column().is_none());
        assert!(op.generated_fields().is_none());
    }

    #[test]
    fn operator_multiline_display() {
        let op = ExtensionScanOperator::new("range", "{}", test_schema(), 3);
        let lines = op.multiline_display();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "ExtensionScanOperator(range)");
        assert_eq!(lines[1], "Tasks = 3");
        assert!(lines[2].starts_with("Schema = "));
    }

    #[test]
    fn to_scan_tasks_produces_correct_count() {
        let op = ExtensionScanOperator::new("range", r#"{"n":100}"#, test_schema(), 4);
        let tasks = op.to_scan_tasks(Pushdowns::default()).unwrap();
        assert_eq!(tasks.len(), 4);
    }

    #[test]
    fn to_scan_tasks_zero_partitions() {
        let op = ExtensionScanOperator::new("empty", "{}", test_schema(), 0);
        let tasks = op.to_scan_tasks(Pushdowns::default()).unwrap();
        assert!(tasks.is_empty());
    }

    #[test]
    fn to_scan_tasks_data_source_fields() {
        let schema = test_schema();
        let op = ExtensionScanOperator::new("my_source", r#"{"k":"v"}"#, schema.clone(), 2);
        let tasks = op.to_scan_tasks(Pushdowns::default()).unwrap();

        let task = tasks[0]
            .as_any()
            .downcast_ref::<ScanTask>()
            .expect("should be a ScanTask");
        assert_eq!(task.sources.len(), 1);

        match &task.sources[0] {
            DataSource::Extension {
                source_name,
                options,
                task_index,
                size_bytes,
                metadata,
                statistics,
            } => {
                assert_eq!(source_name, "my_source");
                assert_eq!(options, r#"{"k":"v"}"#);
                assert_eq!(*task_index, 0);
                assert!(size_bytes.is_none());
                assert!(metadata.is_none());
                assert!(statistics.is_none());
            }
            other => panic!("expected DataSource::Extension, got {other:?}"),
        }

        // Second task has task_index=1.
        let task1 = tasks[1].as_any().downcast_ref::<ScanTask>().unwrap();
        match &task1.sources[0] {
            DataSource::Extension { task_index, .. } => assert_eq!(*task_index, 1),
            other => panic!("expected DataSource::Extension, got {other:?}"),
        }
    }

    #[test]
    fn to_scan_tasks_file_format_config() {
        let op = ExtensionScanOperator::new("my_source", "{}", test_schema(), 1);
        let tasks = op.to_scan_tasks(Pushdowns::default()).unwrap();
        let task = tasks[0].as_any().downcast_ref::<ScanTask>().unwrap();
        match task.file_format_config.as_ref() {
            FileFormatConfig::Extension { source_name } => {
                assert_eq!(source_name, "my_source");
            }
            other => panic!("expected FileFormatConfig::Extension, got {other:?}"),
        }
    }
}
