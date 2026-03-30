use std::sync::Arc;

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_schema::schema::SchemaRef;
use daft_stats::PartitionSpec;
use futures::StreamExt;

use crate::{
    DataSourceRef, PartitionField, Pushdowns, ScanOperator, ScanTask, ScanTaskRef,
    source::{DataSourceTask, DataSourceTaskStatistics, Precision, ReadOptions, RecordBatchStream},
};

/// A [`ScanOperator`] shims a [`DataSource`] as a `ScanOperator`.
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ShimSource {
    source: DataSourceRef,
    // Cached from DataSource methods because ScanOperator returns references.
    name: String,
    schema: SchemaRef,
    partition_fields: Vec<PartitionField>,
}

impl ShimSource {
    pub fn new(source: DataSourceRef) -> Self {
        let name = source.name();
        let schema = source.schema();
        let partition_fields = source.partition_fields();
        Self {
            source,
            name,
            schema,
            partition_fields,
        }
    }
}

impl ShimSource {
    /// Wrap a pure-Python [`DataSourceTask`] in a `python_factory_func_scan_task`.
    ///
    /// The resulting [`ScanTask`] calls back into `daft.io.__shim._get_record_batches`
    /// at execution time, which drains the task's async `read()` generator.
    #[cfg(feature = "python")]
    fn make_python_factory_scan_task(
        &self,
        task: &dyn DataSourceTask,
        pushdowns: &Pushdowns,
    ) -> DaftResult<ScanTaskRef> {
        use crate::{
            ScanSourceKind, SourceConfig, python::PythonTablesFactoryArgs,
            storage_config::StorageConfig,
        };

        let py_obj = pyo3::Python::attach(|py| {
            task.to_py(py).ok_or_else(|| {
                DaftError::InternalError(
                    "Non-native DataSourceTask must be backed by a Python object".to_string(),
                )
            })
        })?;

        let source = crate::ScanSource {
            size_bytes: None,
            metadata: None,
            statistics: None,
            partition_spec: None,
            kind: ScanSourceKind::PythonFactoryFunction {
                module: "daft.io.__shim".to_string(),
                func_name: "_get_record_batches".to_string(),
                func_args: PythonTablesFactoryArgs::new(vec![Arc::new(py_obj)]),
            },
        };

        let source_config = Arc::new(SourceConfig::PythonFunction {
            source_name: Some(self.name.clone()),
            module_name: Some("daft.io.__shim".to_string()),
            function_name: Some("_get_record_batches".to_string()),
        });

        Ok(Arc::new(ScanTask::new(
            vec![source],
            source_config,
            task.schema(),
            Arc::new(StorageConfig::default()),
            pushdowns.clone(),
            None,
        )))
    }

    #[cfg(not(feature = "python"))]
    fn make_python_factory_scan_task(
        &self,
        _task: &dyn DataSourceTask,
        _pushdowns: &Pushdowns,
    ) -> DaftResult<ScanTaskRef> {
        Err(DaftError::InternalError(
            "Pure-Python DataSourceTasks require the python feature".to_string(),
        ))
    }
}

impl ScanOperator for ShimSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        &self.partition_fields
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
        vec![
            format!("DataSource: {}", self.name),
            format!("Schema: {}", self.schema.short_string()),
        ]
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskRef>> {
        let io_runtime = get_io_runtime(true);
        let task_stream = io_runtime.block_on_current_thread(self.source.get_tasks(&pushdowns))?;
        let tasks: Vec<_> = io_runtime.block_on_current_thread(task_stream.collect::<Vec<_>>());

        let mut scan_tasks = Vec::with_capacity(tasks.len());
        for result in tasks {
            let task = result?;
            if let Some(st) = task.as_scan_task() {
                scan_tasks.push(st.clone());
            } else {
                scan_tasks.push(self.make_python_factory_scan_task(task.as_ref(), &pushdowns)?);
            }
        }
        Ok(scan_tasks)
    }
}

/// A [`DataSourceTask`] backed by a native [`ScanTask`].
///
/// Created by [`DataSourceTask::parquet()`] (and future factory methods).
/// The [`ScanSource`] bridge extracts the inner [`ScanTask`] via
/// [`as_scan_task()`](DataSourceTask::as_scan_task) so it flows through
/// the existing execution path without calling [`read()`](DataSourceTask::read).
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct ShimSourceTask(ScanTaskRef);

impl ShimSourceTask {
    pub fn new(scan_task: ScanTaskRef) -> Self {
        Self(scan_task)
    }
}

#[async_trait]
impl DataSourceTask for ShimSourceTask {
    fn schema(&self) -> SchemaRef {
        self.0.schema.clone()
    }

    fn statistics(&self) -> Option<DataSourceTaskStatistics> {
        Some(DataSourceTaskStatistics {
            size_bytes: match self.0.size_bytes_on_disk {
                Some(n) => Precision::Exact(n),
                None => Precision::Absent,
            },
            column_stats: self.0.statistics.clone(),
        })
    }

    fn partition_values(&self) -> Option<&PartitionSpec> {
        self.0
            .sources
            .first()
            .and_then(|s| s.partition_spec.as_ref())
    }

    /// TEMPORARY DURING MIGRATION
    /// Used by the [`ScanOperator`] bridge to extract native scan tasks
    fn as_scan_task(&self) -> Option<&ScanTaskRef> {
        Some(&self.0)
    }

    /// TEMPORARY DURING MIGRATION
    /// We unwrap the ScanTask in the bridge, so we don't need to implement this.
    async fn read(&self, _options: ReadOptions) -> DaftResult<RecordBatchStream> {
        unreachable!("ScanSourceTask is executed via the native ScanTask path, not read()")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use common_error::DaftResult;
    use daft_core::{datatypes::Int64Array, series::IntoSeries};
    use daft_recordbatch::RecordBatch;
    use daft_schema::{
        dtype::DataType,
        field::Field,
        schema::{Schema, SchemaRef},
        time_unit::TimeUnit,
    };
    use daft_stats::{PartitionSpec, TableStatistics};
    use futures::stream;

    use super::*;
    use crate::{
        DataSource, DataSourceTaskStream, FileFormatConfig, ParquetSourceConfig, Pushdowns,
        ScanOperator, ScanSource, ScanSourceKind, ScanTask, SourceConfig,
        source::{DataSourceTaskRef, ReadOptions, RecordBatchStream},
        storage_config::StorageConfig,
    };

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Build a simple schema with the given field names (all Int64).
    fn make_schema(fields: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            fields.iter().map(|name| Field::new(*name, DataType::Int64)),
        ))
    }

    /// Build a `ScanTask` with configurable size_bytes, statistics, and partition spec.
    fn make_scan_task(
        schema: SchemaRef,
        size_bytes: Option<u64>,
        statistics: Option<TableStatistics>,
        partition_spec: Option<PartitionSpec>,
    ) -> Arc<ScanTask> {
        let source = ScanSource {
            size_bytes,
            metadata: None,
            statistics,
            partition_spec,
            kind: ScanSourceKind::File {
                path: "test_file.parquet".to_string(),
                chunk_spec: None,
                iceberg_delete_files: None,
                parquet_metadata: None,
            },
        };

        let source_config = SourceConfig::File(FileFormatConfig::Parquet(ParquetSourceConfig {
            coerce_int96_timestamp_unit: TimeUnit::Seconds,
            field_id_mapping: None,
            row_groups: None,
            chunk_size: None,
        }));

        Arc::new(ScanTask::new(
            vec![source],
            Arc::new(source_config),
            schema,
            Arc::new(StorageConfig::default()),
            Pushdowns::default(),
            None,
        ))
    }

    /// Build a minimal `PartitionSpec` with a single Int64 column.
    fn make_partition_spec(col_name: &str, value: i64) -> PartitionSpec {
        let schema = make_schema(&[col_name]);
        let series = Int64Array::from_slice(col_name, &[value]).into_series();
        let batch = RecordBatch::new_with_size(schema, vec![series], 1).unwrap();
        PartitionSpec { keys: batch }
    }

    // -----------------------------------------------------------------------
    // Mock DataSource that yields ScanSourceTasks
    // -----------------------------------------------------------------------

    #[derive(Debug)]
    struct MockDataSource {
        name: String,
        schema: SchemaRef,
        partition_fields: Vec<PartitionField>,
        tasks: Vec<DataSourceTaskRef>,
    }

    #[async_trait]
    impl DataSource for MockDataSource {
        fn name(&self) -> String {
            self.name.clone()
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn partition_fields(&self) -> Vec<PartitionField> {
            self.partition_fields.clone()
        }

        async fn get_tasks(&self, _pushdowns: &Pushdowns) -> DaftResult<DataSourceTaskStream> {
            let tasks: Vec<DaftResult<DataSourceTaskRef>> =
                self.tasks.iter().cloned().map(Ok).collect();
            Ok(Box::pin(stream::iter(tasks)))
        }
    }

    // -----------------------------------------------------------------------
    // Mock DataSourceTask that does NOT wrap a ScanTask (simulates pure-Python)
    // -----------------------------------------------------------------------

    #[derive(Debug)]
    struct NonNativeTask {
        schema: SchemaRef,
    }

    #[async_trait]
    impl DataSourceTask for NonNativeTask {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        async fn read(&self, _options: ReadOptions) -> DaftResult<RecordBatchStream> {
            unimplemented!("not used in test")
        }
    }

    #[test]
    fn test_scan_source_task_schema() {
        let schema = make_schema(&["a", "b", "c"]);
        let scan_task = make_scan_task(schema.clone(), None, None, None);
        let task = ShimSourceTask::new(scan_task);

        assert_eq!(task.schema(), schema);
    }

    #[test]
    fn test_scan_source_task_statistics_with_size_bytes() {
        let schema = make_schema(&["x"]);
        let scan_task = make_scan_task(schema, Some(42_000), None, None);
        let task = ShimSourceTask::new(scan_task);

        let stats = task.statistics().expect("statistics should be Some");
        match stats.size_bytes {
            Precision::Exact(n) => assert_eq!(n, 42_000),
            other => panic!("expected Precision::Exact(42000), got {:?}", other),
        }
        assert!(stats.column_stats.is_none());
    }

    #[test]
    fn test_scan_source_task_statistics_without_size_bytes() {
        let schema = make_schema(&["x"]);
        let scan_task = make_scan_task(schema, None, None, None);
        let task = ShimSourceTask::new(scan_task);

        let stats = task.statistics().expect("statistics should be Some");
        assert!(
            matches!(stats.size_bytes, Precision::Absent),
            "expected Precision::Absent, got {:?}",
            stats.size_bytes
        );
        assert!(stats.column_stats.is_none());
    }

    #[test]
    fn test_scan_source_task_statistics_with_column_stats() {
        let schema = make_schema(&["val"]);
        // Create a minimal TableStatistics (empty columns, matching schema).
        let table_stats = TableStatistics::new(vec![], schema.clone());
        let scan_task = make_scan_task(schema, Some(100), Some(table_stats), None);
        let task = ShimSourceTask::new(scan_task);

        let stats = task.statistics().expect("statistics should be Some");
        match stats.size_bytes {
            Precision::Exact(n) => assert_eq!(n, 100),
            other => panic!("expected Precision::Exact(100), got {:?}", other),
        }
        assert!(stats.column_stats.is_some());
    }

    #[test]
    fn test_scan_source_task_partition_values_present() {
        let schema = make_schema(&["data", "part_col"]);
        let partition_spec = make_partition_spec("part_col", 7);
        let scan_task = make_scan_task(schema, None, None, Some(partition_spec.clone()));
        let task = ShimSourceTask::new(scan_task);

        let pv = task
            .partition_values()
            .expect("partition_values should be Some");
        assert_eq!(*pv, partition_spec);
    }

    #[test]
    fn test_scan_source_task_partition_values_absent() {
        let schema = make_schema(&["data"]);
        let scan_task = make_scan_task(schema, None, None, None);
        let task = ShimSourceTask::new(scan_task);

        assert!(task.partition_values().is_none());
    }

    #[test]
    fn test_scan_source_task_as_scan_task_returns_original() {
        let schema = make_schema(&["a"]);
        let scan_task = make_scan_task(schema, None, None, None);
        let task = ShimSourceTask::new(scan_task.clone());

        let unwrapped = task.as_scan_task().expect("as_scan_task should be Some");
        assert!(Arc::ptr_eq(unwrapped, &scan_task));
    }

    #[test]
    fn test_scan_source_caches_name() {
        let schema = make_schema(&["col1"]);
        let ds: DataSourceRef = Arc::new(MockDataSource {
            name: "my_source".to_string(),
            schema,
            partition_fields: vec![],
            tasks: vec![],
        });
        let bridge = ShimSource::new(ds);

        assert_eq!(bridge.name(), "my_source");
    }

    #[test]
    fn test_scan_source_caches_schema() {
        let schema = make_schema(&["alpha", "beta"]);
        let ds: DataSourceRef = Arc::new(MockDataSource {
            name: "test".to_string(),
            schema: schema.clone(),
            partition_fields: vec![],
            tasks: vec![],
        });
        let bridge = ShimSource::new(ds);

        assert_eq!(bridge.schema(), schema);
    }

    #[test]
    fn test_scan_source_caches_partition_fields() {
        let schema = make_schema(&["id", "date"]);
        let pf = PartitionField::new(Field::new("date", DataType::Int64), None, None).unwrap();

        let ds: DataSourceRef = Arc::new(MockDataSource {
            name: "partitioned".to_string(),
            schema,
            partition_fields: vec![pf.clone()],
            tasks: vec![],
        });
        let bridge = ShimSource::new(ds);

        let keys = bridge.partitioning_keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], pf);
    }

    #[test]
    fn test_scan_source_file_path_column_is_none() {
        let schema = make_schema(&["a"]);
        let ds: DataSourceRef = Arc::new(MockDataSource {
            name: "t".to_string(),
            schema,
            partition_fields: vec![],
            tasks: vec![],
        });
        let bridge = ShimSource::new(ds);

        assert!(bridge.file_path_column().is_none());
    }

    #[test]
    fn test_scan_source_generated_fields_is_none() {
        let schema = make_schema(&["a"]);
        let ds: DataSourceRef = Arc::new(MockDataSource {
            name: "t".to_string(),
            schema,
            partition_fields: vec![],
            tasks: vec![],
        });
        let bridge = ShimSource::new(ds);

        assert!(bridge.generated_fields().is_none());
    }

    #[test]
    fn test_scan_source_cannot_absorb_anything() {
        let schema = make_schema(&["a"]);
        let ds: DataSourceRef = Arc::new(MockDataSource {
            name: "t".to_string(),
            schema,
            partition_fields: vec![],
            tasks: vec![],
        });
        let bridge = ShimSource::new(ds);

        assert!(!bridge.can_absorb_filter());
        assert!(!bridge.can_absorb_select());
        assert!(!bridge.can_absorb_limit());
        assert!(!bridge.can_absorb_shard());
    }

    #[test]
    fn test_scan_source_multiline_display() {
        let schema = make_schema(&["x", "y"]);
        let ds: DataSourceRef = Arc::new(MockDataSource {
            name: "display_test".to_string(),
            schema,
            partition_fields: vec![],
            tasks: vec![],
        });
        let bridge = ShimSource::new(ds);

        let lines = bridge.multiline_display();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("display_test"));
        assert!(lines[1].starts_with("Schema:"));
    }

    #[test]
    fn test_scan_source_to_scan_tasks_extracts_native_tasks() {
        let schema = make_schema(&["col"]);
        let st1 = make_scan_task(schema.clone(), Some(100), None, None);
        let st2 = make_scan_task(schema.clone(), Some(200), None, None);

        let task1: DataSourceTaskRef = Arc::new(ShimSourceTask::new(st1.clone()));
        let task2: DataSourceTaskRef = Arc::new(ShimSourceTask::new(st2.clone()));

        let ds: DataSourceRef = Arc::new(MockDataSource {
            name: "native".to_string(),
            schema,
            partition_fields: vec![],
            tasks: vec![task1, task2],
        });
        let bridge = ShimSource::new(ds);

        let scan_tasks = bridge.to_scan_tasks(Pushdowns::default()).unwrap();
        assert_eq!(scan_tasks.len(), 2);
        assert!(Arc::ptr_eq(&scan_tasks[0], &st1));
        assert!(Arc::ptr_eq(&scan_tasks[1], &st2));
    }

    #[test]
    fn test_scan_source_to_scan_tasks_empty_stream() {
        let schema = make_schema(&["col"]);
        let ds: DataSourceRef = Arc::new(MockDataSource {
            name: "empty".to_string(),
            schema,
            partition_fields: vec![],
            tasks: vec![],
        });
        let bridge = ShimSource::new(ds);

        let scan_tasks = bridge.to_scan_tasks(Pushdowns::default()).unwrap();
        assert!(scan_tasks.is_empty());
    }

    #[test]
    fn test_scan_source_to_scan_tasks_errors_on_non_native_task() {
        let schema = make_schema(&["col"]);
        let non_native: DataSourceTaskRef = Arc::new(NonNativeTask {
            schema: schema.clone(),
        });

        let ds: DataSourceRef = Arc::new(MockDataSource {
            name: "non_native".to_string(),
            schema,
            partition_fields: vec![],
            tasks: vec![non_native],
        });
        let bridge = ShimSource::new(ds);

        let result = bridge.to_scan_tasks(Pushdowns::default());
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Pure-Python DataSourceTasks"),
            "unexpected error message: {}",
            err_msg
        );
    }

    #[test]
    fn test_scan_source_to_scan_tasks_errors_on_mixed_tasks() {
        // First task is native, second is non-native. Should error on the second.
        let schema = make_schema(&["col"]);
        let st = make_scan_task(schema.clone(), None, None, None);
        let native: DataSourceTaskRef = Arc::new(ShimSourceTask::new(st));
        let non_native: DataSourceTaskRef = Arc::new(NonNativeTask {
            schema: schema.clone(),
        });

        let ds: DataSourceRef = Arc::new(MockDataSource {
            name: "mixed".to_string(),
            schema,
            partition_fields: vec![],
            tasks: vec![native, non_native],
        });
        let bridge = ShimSource::new(ds);

        let result = bridge.to_scan_tasks(Pushdowns::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_non_native_task_as_scan_task_returns_none() {
        // Verify the default trait implementation returns None.
        let schema = make_schema(&["col"]);
        let task = NonNativeTask { schema };
        assert!(task.as_scan_task().is_none());
    }
}
