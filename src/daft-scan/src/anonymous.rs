use std::{fmt::Display, sync::Arc};

use common_error::DaftResult;
use daft_core::schema::SchemaRef;

use crate::{
    file_format::FileFormatConfig, storage_config::StorageConfig, DataFileSource, PartitionField,
    Pushdowns, ScanOperator, ScanTask,
};
#[derive(Debug, PartialEq, Hash)]
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
        write!(f, "{:#?}", self)
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

    fn to_scan_tasks(
        &self,
        pushdowns: Pushdowns,
    ) -> DaftResult<Box<dyn Iterator<Item = DaftResult<crate::ScanTask>>>> {
        let columns = pushdowns.columns;
        let file_format_config = self.file_format_config.clone();
        let storage_config = self.storage_config.clone();
        let limit = pushdowns.limit;
        let schema = self.schema.clone();
        let iter = self.files.clone().into_iter().map(move |f| {
            let source = DataFileSource::AnonymousDataFile {
                path: f,
                metadata: None,
                partition_spec: None,
                statistics: None,
            };
            Ok(ScanTask {
                source,
                file_format_config: file_format_config.clone(),
                schema: schema.clone(),
                storage_config: storage_config.clone(),
                columns: columns.clone(),
                limit,
            })
        });
        Ok(Box::new(iter))
    }
}
