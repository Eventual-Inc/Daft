use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_file_formats::FileFormatConfig;
use common_scan_info::{
    PartitionField, Pushdowns, ScanOperator, ScanTaskLike, ScanTaskLikeRef, StorageConfig,
    SupportsPushdownFilters,
};
use daft_core::{count_mode::CountMode, prelude::SchemaRef};
use daft_lance::read::{LanceScanOperator, LanceScanTask};

use crate::ScanTask;

pub struct NativeScanOperator {
    scan_operator: Arc<dyn ScanOperator>,
}

impl NativeScanOperator {
    pub fn try_new(
        uri: String,
        file_format_config: Arc<FileFormatConfig>,
        storage_config: Arc<StorageConfig>,
        storage_options: Arc<HashMap<String, String>>,
    ) -> DaftResult<Self> {
        let scan_operator = if let FileFormatConfig::Lance(_) = file_format_config.as_ref() {
            LanceScanOperator::try_new(uri, file_format_config, storage_config, storage_options)
        } else {
            return Err(DaftError::not_implemented(format!(
                "Native scan doesn't support {} file format",
                file_format_config.file_format()
            )));
        }?;

        Ok(Self {
            scan_operator: Arc::new(scan_operator),
        })
    }
}

impl Debug for NativeScanOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.scan_operator.fmt(f)
    }
}

impl ScanOperator for NativeScanOperator {
    fn name(&self) -> &str {
        self.scan_operator.name()
    }

    fn schema(&self) -> SchemaRef {
        self.scan_operator.schema()
    }

    fn partitioning_keys(&self) -> &[PartitionField] {
        self.scan_operator.partitioning_keys()
    }

    fn file_path_column(&self) -> Option<&str> {
        self.scan_operator.file_path_column()
    }

    fn generated_fields(&self) -> Option<SchemaRef> {
        self.scan_operator.generated_fields()
    }

    fn can_absorb_filter(&self) -> bool {
        self.scan_operator.can_absorb_filter()
    }

    fn can_absorb_select(&self) -> bool {
        self.scan_operator.can_absorb_select()
    }

    fn can_absorb_limit(&self) -> bool {
        self.scan_operator.can_absorb_limit()
    }

    fn can_absorb_shard(&self) -> bool {
        self.scan_operator.can_absorb_shard()
    }

    fn multiline_display(&self) -> Vec<String> {
        self.scan_operator.multiline_display()
    }

    fn supports_count_pushdown(&self) -> bool {
        self.scan_operator.supports_count_pushdown()
    }

    fn supported_count_modes(&self) -> Vec<CountMode> {
        self.scan_operator.supported_count_modes()
    }

    fn to_scan_tasks(&self, pushdowns: Pushdowns) -> DaftResult<Vec<ScanTaskLikeRef>> {
        let scan_tasks = self.scan_operator.to_scan_tasks(pushdowns);
        if let Ok(scan_tasks) = scan_tasks {
            scan_tasks
                .into_iter()
                .map(|task| {
                    if let Some(lance_task) = task.as_any().downcast_ref::<LanceScanTask>() {
                        let scan_task: ScanTask = lance_task.try_into()?;
                        Ok(Arc::new(scan_task) as Arc<dyn ScanTaskLike>)
                    } else {
                        Ok(task)
                    }
                })
                .collect()
        } else {
            scan_tasks
        }
    }

    fn as_pushdown_filter(&self) -> Option<&dyn SupportsPushdownFilters> {
        self.scan_operator.as_pushdown_filter()
    }
}
