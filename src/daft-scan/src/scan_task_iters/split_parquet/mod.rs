use std::iter::Flatten;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;

use super::BoxScanTaskIter;
use crate::ScanTaskRef;

mod fetch_parquet_metadata;
mod split_parquet_decision;
mod split_parquet_file;

/// Splits input scan tasks into smaller scan tasks based on Parquet file metadata.
///
/// This struct provides functionality to split scan tasks by:
/// 1. Deciding whether or not to split each input ScanTask
/// 2. Fetching the Parquet metadata of the ScanTask (if deemed necessary to split)
/// 3. Performing the splitting of the ScanTask into smaller ScanTasks
///
/// Note that this may be expensive if the incoming stream has many large ScanTasks, incurring a
/// higher cost at planning-time.
///
/// # Examples
///
/// ```
/// # use daft_scan::scan_task_iters::BoxScanTaskIter;
/// # use common_daft_config::DaftExecutionConfig;
/// # let input_tasks: BoxScanTaskIter = unimplemented!();
/// # let config = DaftExecutionConfig::default();
/// let splitter = SplitParquetScanTasks::new(input_tasks, &config);
/// let split_tasks = splitter.into_iter();
/// ```
pub struct SplitParquetScanTasks<'cfg> {
    retriever: fetch_parquet_metadata::RetrieveParquetMetadataIterator<'cfg>,
}

impl<'cfg> SplitParquetScanTasks<'cfg> {
    pub fn new(inputs: BoxScanTaskIter<'cfg>, cfg: &'cfg DaftExecutionConfig) -> Self {
        let decider = split_parquet_decision::DecideSplitIterator::new(inputs, cfg);
        let retriever = fetch_parquet_metadata::RetrieveParquetMetadataIterator::new(decider, cfg);
        SplitParquetScanTasks { retriever }
    }
}

pub struct SplitParquetScanTasksIterator<'cfg>(
    Flatten<fetch_parquet_metadata::RetrieveParquetMetadataIterator<'cfg>>,
);

impl<'cfg> IntoIterator for SplitParquetScanTasks<'cfg> {
    type IntoIter = SplitParquetScanTasksIterator<'cfg>;
    type Item = DaftResult<ScanTaskRef>;

    fn into_iter(self) -> Self::IntoIter {
        SplitParquetScanTasksIterator(self.retriever.flatten())
    }
}

impl<'cfg> Iterator for SplitParquetScanTasksIterator<'cfg> {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}
