use std::sync::Arc;

use common_error::DaftResult;
use daft_core::schema::SchemaRef;

use crate::{
    file_format::{FileFormatConfig, ParquetSourceConfig},
    storage_config::StorageConfig,
    ChunkSpec, DataSource, PartitionField, Pushdowns, ScanOperator, ScanTask, ScanTaskRef,
};
#[derive(Debug)]
pub struct AnonymousScanOperator {
    files: Vec<String>,
    schema: SchemaRef,
    file_format_config: Arc<FileFormatConfig>,
    storage_config: Arc<StorageConfig>,
}

impl AnonymousScanOperator {
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
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<ScanTaskRef>>>> {
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
        Ok(Box::new(files.into_iter().zip(row_groups).map(
            move |(f, rg)| {
                let chunk_spec = rg.map(ChunkSpec::Parquet);
                Ok(ScanTask::new(
                    vec![DataSource::File {
                        path: f.to_string(),
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
                )
                .into())
            },
        )))
    }
}
