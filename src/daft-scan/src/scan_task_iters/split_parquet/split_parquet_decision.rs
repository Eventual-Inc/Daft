use std::{ops::Deref, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};

use super::{
    fetch_parquet_metadata::ParquetSplitScanTaskGenerator, split_parquet_file::ParquetFileSplitter,
};
use crate::{scan_task_iters::BoxScanTaskIter, DataSource, ScanTaskRef};

/// An iterator that determines whether incoming ScanTasks should be split by Parquet rowgroups.
///
/// # Returns
///
/// Returns an iterator of [`Decision`] objects indicating whether and how to split each task.
pub(super) struct DecideSplitIterator<'cfg> {
    inputs: BoxScanTaskIter<'cfg>,
    cfg: &'cfg DaftExecutionConfig,
}

impl<'cfg> DecideSplitIterator<'cfg> {
    pub fn new(inputs: BoxScanTaskIter<'cfg>, cfg: &'cfg DaftExecutionConfig) -> Self {
        Self { inputs, cfg }
    }
}

pub(super) enum Decision {
    Split(ScanTaskRef),
    NoSplit(ScanTaskRef),
}

impl<'cfg> Iterator for DecideSplitIterator<'cfg> {
    type Item = DaftResult<Decision>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(scan_task) = self.inputs.next() {
            let scan_task = match scan_task {
                Ok(st) => st,
                Err(e) => {
                    return Some(Err(e));
                }
            };
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
                scan_task.estimate_in_memory_size_bytes(Some(self.cfg)),
            ) && est_materialized_size
                .map_or(true, |est| est > self.cfg.scan_tasks_max_size_bytes)
                && source
                    .get_iceberg_delete_files()
                    .map_or(true, std::vec::Vec::is_empty)
            {
                return Some(Ok(Decision::Split(scan_task)));
            } else {
                return Some(Ok(Decision::NoSplit(scan_task)));
            }
        }
        None
    }
}

impl Deref for Decision {
    type Target = ScanTaskRef;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Split(st) | Self::NoSplit(st) => st,
        }
    }
}

impl Decision {
    pub(super) fn get_parquet_path(&self) -> &str {
        if let [source] = &self.sources[..] {
            source.get_path()
        } else {
            panic!("Calling get_parquet_path on a malformed ScanTaskSplitDecision");
        }
    }

    pub(super) fn get_io_config(&self) -> Arc<daft_io::IOConfig> {
        Arc::new(self.storage_config.io_config.clone().unwrap_or_default())
    }

    pub(super) fn get_parquet_source_config(&self) -> &ParquetSourceConfig {
        if let FileFormatConfig::Parquet(cfg) = self.file_format_config.as_ref() {
            cfg
        } else {
            panic!("Calling get_parquet_source_config on a malformed ScanTaskSplitDecision");
        }
    }

    pub(super) fn apply_metadata(
        self,
        metadata: Option<parquet2::metadata::FileMetaData>,
    ) -> ParquetSplitScanTaskGenerator {
        match metadata {
            Some(_metadata) => {
                ParquetSplitScanTaskGenerator::Split(ParquetFileSplitter::new(self.unwrap()))
            }
            None => ParquetSplitScanTaskGenerator::NoSplit(std::iter::once(Ok(self.unwrap()))),
        }
    }

    fn unwrap(self) -> ScanTaskRef {
        match self {
            Self::Split(st) | Self::NoSplit(st) => st,
        }
    }
}
