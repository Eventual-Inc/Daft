use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};

use crate::{scan_task_iters::BoxScanTaskIter, DataSource};

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
    Split(()),
    NoSplit(()),
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
                return Some(Ok(Decision::Split(())));
            } else {
                return Some(Ok(Decision::NoSplit(())));
            }
        }
        None
    }
}
