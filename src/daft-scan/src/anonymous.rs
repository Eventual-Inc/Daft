use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_scan_info::{PartitionField, Pushdowns, ScanOperator, ScanTaskLike, ScanTaskLikeRef};
use daft_schema::schema::SchemaRef;

use crate::{
    scan_task_iters::{merge_by_sizes, split_by_row_groups, BoxScanTaskIter},
    storage_config::StorageConfig,
    ChunkSpec, DataSource, ScanTask,
};
#[derive(Debug)]
pub struct AnonymousScanOperator {
    files: Vec<String>,
    schema: SchemaRef,
    file_format_config: Arc<FileFormatConfig>,
    storage_config: Arc<StorageConfig>,
}

impl AnonymousScanOperator {
    #[must_use]
    pub fn new(
        files: Vec<String>,
        schema: SchemaRef,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
    ) -> Self {
        Self {
            files,
            schema,
            file_format_config,
            storage_config,
        }
    }
}

impl ScanOperator for AnonymousScanOperator {
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
        false
    }
    fn can_absorb_limit(&self) -> bool {
        false
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![
            "AnonymousScanOperator".to_string(),
            format!("File paths = [{}]", self.files.join(", ")),
        ];
        lines.extend(self.file_format_config.multiline_display());
        lines.extend(self.storage_config.multiline_display());

        lines
    }

    fn to_scan_tasks(
        &self,
        pushdowns: Pushdowns,
        cfg: Option<&DaftExecutionConfig>,
    ) -> DaftResult<Vec<ScanTaskLikeRef>> {
        let files = self.files.clone();
        let file_format_config = self.file_format_config.clone();
        let schema = self.schema.clone();
        let storage_config = self.storage_config.clone();

        let row_groups = if let FileFormatConfig::Parquet(ParquetSourceConfig {
            row_groups: Some(row_groups),
            ..
        }) = self.file_format_config.as_ref()
        {
            row_groups.clone()
        } else {
            std::iter::repeat(None).take(files.len()).collect()
        };

        // Create one ScanTask per file.
        let mut scan_tasks: BoxScanTaskIter =
            Box::new(files.into_iter().zip(row_groups).map(|(f, rg)| {
                let chunk_spec = rg.map(ChunkSpec::Parquet);
                Ok(ScanTask::new(
                    vec![DataSource::File {
                        path: f,
                        chunk_spec,
                        size_bytes: None,
                        iceberg_delete_files: None,
                        metadata: None,
                        partition_spec: None,
                        statistics: None,
                        parquet_metadata: None,
                    }],
                    file_format_config.clone(),
                    schema.clone(),
                    storage_config.clone(),
                    pushdowns.clone(),
                    None,
                )
                .into())
            }));

        if let Some(cfg) = cfg {
            scan_tasks = split_by_row_groups(
                scan_tasks,
                cfg.parquet_split_row_groups_max_files,
                cfg.scan_tasks_min_size_bytes,
                cfg.scan_tasks_max_size_bytes,
            );

            scan_tasks = merge_by_sizes(scan_tasks, &pushdowns, cfg);
        }

        scan_tasks
            .map(|st| st.map(|task| task as Arc<dyn ScanTaskLike>))
            .collect()
    }
}
