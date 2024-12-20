use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;

use super::{split_parquet_decision, split_parquet_file};
use crate::ScanTaskRef;

pub(super) struct RetrieveParquetMetadataIterator<'cfg> {
    decider: split_parquet_decision::DecideSplitIterator<'cfg>,
    _cfg: &'cfg DaftExecutionConfig,
}

impl<'cfg> RetrieveParquetMetadataIterator<'cfg> {
    pub(super) fn new(
        decider: split_parquet_decision::DecideSplitIterator<'cfg>,
        cfg: &'cfg DaftExecutionConfig,
    ) -> Self {
        Self { decider, _cfg: cfg }
    }
}

pub(super) enum ParquetSplitScanTaskGenerator {
    _NoSplit(std::iter::Once<DaftResult<ScanTaskRef>>),
    _Split(split_parquet_file::ParquetFileSplitter),
}

impl<'cfg> Iterator for RetrieveParquetMetadataIterator<'cfg> {
    type Item = ParquetSplitScanTaskGenerator;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(_decision) = self.decider.next() {
            todo!("Implement windowed metadata fetching and yielding of ParquetSplitScanTaskGenerator");
        }
        None
    }
}

impl Iterator for ParquetSplitScanTaskGenerator {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::_NoSplit(iter) => iter.next(),
            Self::_Split(iter) => iter.next(),
        }
    }
}
