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
pub struct SplitParquetScanTasksIterator<'cfg> {
    split_result_iter: Flatten<fetch_parquet_metadata::RetrieveParquetMetadataIterator<'cfg>>,
}

impl<'cfg> SplitParquetScanTasksIterator<'cfg> {
    pub fn new(inputs: BoxScanTaskIter<'cfg>, cfg: &'cfg DaftExecutionConfig) -> Self {
        let decider = split_parquet_decision::DecideSplitIterator::new(inputs, cfg);
        let retriever = fetch_parquet_metadata::RetrieveParquetMetadataIterator::new(decider, cfg);
        SplitParquetScanTasksIterator {
            split_result_iter: retriever.flatten(),
        }
    }
}

impl Iterator for SplitParquetScanTasksIterator<'_> {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        self.split_result_iter.next()
    }
}
