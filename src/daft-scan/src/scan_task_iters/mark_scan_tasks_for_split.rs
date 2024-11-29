use std::{ops::Deref, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};

use super::{split_parquet_files_by_rowgroup::SplittableScanTaskRef, BoxScanTaskIter};
use crate::{storage_config::StorageConfig, DataSource, ScanTaskRef};

/// Decides whether or not ScanTasks need to be split
///
/// Returns an iterator of [`ScanTaskSplitDecision`], which implement [`NeedsParquetMetadata`]. These can then be fed into
/// [`super::retrieve_parquet_metadata::batched_parquet_metadata_retrieval`] which will transform the [`ScanTaskSplitDecision`]
/// into [`ScanTaskSplitByRowgroup`] by fetching the Parquet metadatas.
pub(crate) fn decide_scantask_split_by_rowgroups<'cfg>(
    scan_tasks: BoxScanTaskIter<'cfg>,
    config: &'cfg DaftExecutionConfig,
) -> impl Iterator<Item = DaftResult<ScanTaskSplitDecision<'cfg>>> {
    scan_tasks.map(|st| Ok(ScanTaskSplitDecision::from_scan_task(st?, config)))
}

/// Wraps a ScanTaskRef, indicating whether or not it should be split by Parquet Rowgroups
pub(crate) struct ScanTaskSplitDecision<'cfg> {
    decision: Decision,
    config: &'cfg DaftExecutionConfig,
}

enum Decision {
    NoSplit(ScanTaskRef),
    Split(ScanTaskRef),
}

impl<'cfg> ScanTaskSplitDecision<'cfg> {
    /// Optionally creates [`SplitScanTaskInfo`] depending on whether or not a given ScanTask can be split
    pub fn from_scan_task(scan_task: ScanTaskRef, config: &'cfg DaftExecutionConfig) -> Self {
        /* Only split parquet tasks if they:
            - have one source
            - have no specified chunk spec or number of rows
            - have size past split threshold
            - no iceberg delete files
        */
        if let (
            FileFormatConfig::Parquet(ParquetSourceConfig { .. }),
            [source],
            Some(None),
            None,
            est_materialized_size,
        ) = (
            scan_task.file_format_config.as_ref(),
            &scan_task.sources[..],
            scan_task.sources.first().map(DataSource::get_chunk_spec),
            scan_task.pushdowns.limit,
            scan_task.estimate_in_memory_size_bytes(Some(config)),
        ) && est_materialized_size.map_or(true, |est| est > config.scan_tasks_max_size_bytes)
            && source
                .get_iceberg_delete_files()
                .map_or(true, std::vec::Vec::is_empty)
        {
            Self {
                decision: Decision::Split(scan_task),
                config,
            }
        } else {
            Self {
                decision: Decision::NoSplit(scan_task),
                config,
            }
        }
    }

    pub(crate) fn should_fetch_parquet_metadata(&self) -> bool {
        matches!(&self.decision, Decision::Split(..))
    }

    pub(crate) fn get_parquet_path(&self) -> &str {
        if let [source] = &self.sources[..] {
            source.get_path()
        } else {
            panic!("Calling get_parquet_path on a malformed ScanTaskSplitDecision");
        }
    }

    pub(crate) fn get_io_config(&self) -> Arc<daft_io::IOConfig> {
        Arc::new(self.storage_config.io_config.clone().unwrap_or_default())
    }

    pub(crate) fn get_parquet_source_config(&self) -> &ParquetSourceConfig {
        if let FileFormatConfig::Parquet(cfg) = self.file_format_config.as_ref() {
            cfg
        } else {
            panic!("Calling get_parquet_source_config on a malformed ScanTaskSplitDecision");
        }
    }

    pub(crate) fn apply_metadata(
        &self,
        metadata: Option<parquet2::metadata::FileMetaData>,
    ) -> SplittableScanTaskRef<'cfg> {
        match metadata {
            Some(metadata) => SplittableScanTaskRef::Split((*self).clone(), metadata, self.config),
            None => SplittableScanTaskRef::NoSplit((*self).clone()),
        }
    }
}

impl<'cfg> Deref for ScanTaskSplitDecision<'cfg> {
    type Target = ScanTaskRef;

    fn deref(&self) -> &Self::Target {
        match &self.decision {
            Decision::NoSplit(st) | Decision::Split(st) => st,
        }
    }
}
