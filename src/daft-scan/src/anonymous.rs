use std::sync::Arc;

use common_error::DaftResult;
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_scan_info::{PartitionField, Pushdowns, ScanOperator, ScanTaskLike, ScanTaskLikeRef};
use daft_schema::schema::SchemaRef;

use crate::{ChunkSpec, DataSource, ScanTask, storage_config::StorageConfig};
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
    fn name(&self) -> &'static str {
        "AnonymousScanOperator"
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
        false
    }
    fn can_absorb_limit(&self) -> bool {
        false
    }

    fn can_absorb_shard(&self) -> bool {
        false
    }

    fn supports_count_pushdown(&self) -> bool {
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

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
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
            std::iter::repeat_n(None, files.len()).collect()
        };

        // Create one ScanTask per file.
        Ok(files
            .into_iter()
            .zip(row_groups)
            .map(|(f, rg)| {
                let chunk_spec = rg.map(ChunkSpec::Parquet);
                Arc::new(ScanTask::new(
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
                ))
            })
            .map(|st| st as Arc<dyn ScanTaskLike>)
            .collect())
    }
}
