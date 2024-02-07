use std::{fmt::Display, sync::Arc};

use common_error::DaftResult;
use daft_core::schema::SchemaRef;
use itertools::Itertools;

use crate::{
    file_format::FileFormatConfig, storage_config::StorageConfig, DataFileSource, PartitionField,
    Pushdowns, ScanOperator, ScanTask, ScanTaskRef,
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

impl Display for AnonymousScanOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AnonymousScanOperator: File paths=[{}], Format-specific config = {:?}, Storage config = {:?}", self.files.join(", "), self.file_format_config, self.storage_config)
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
        let mut res = vec![];
        res.push("AnonymousScanOperator".to_string());
        res.push(format!("File paths = [{}]", self.files.iter().join(", ")));
        let file_format = self.file_format_config.multiline_display();
        if !file_format.is_empty() {
            res.push(format!(
                "{} config= {}",
                self.file_format_config.var_name(),
                file_format.join(", ")
            ));
        }
        let storage_config = self.storage_config.multiline_display();
        if !storage_config.is_empty() {
            res.push(format!(
                "{} storage config = {{ {} }}",
                self.storage_config.var_name(),
                storage_config.join(", ")
            ));
        }
        res
    }

    fn to_scan_tasks(
        &self,
        pushdowns: Pushdowns,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<ScanTaskRef>>>> {
        let files = self.files.clone();
        let file_format_config = self.file_format_config.clone();
        let schema = self.schema.clone();
        let storage_config = self.storage_config.clone();

        // Create one ScanTask per file.
        Ok(Box::new(files.into_iter().map(move |f| {
            Ok(ScanTask::new(
                vec![DataFileSource::AnonymousDataFile {
                    path: f.to_string(),
                    chunk_spec: None,
                    size_bytes: None,
                    metadata: None,
                    partition_spec: None,
                    statistics: None,
                }],
                file_format_config.clone(),
                schema.clone(),
                storage_config.clone(),
                pushdowns.clone(),
            )
            .into())
        })))
    }
}
