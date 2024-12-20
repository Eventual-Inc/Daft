use std::iter::Flatten;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;

use super::BoxScanTaskIter;
use crate::ScanTaskRef;

mod fetch_parquet_metadata;
mod split_parquet_decision;
mod split_parquet_file;

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
