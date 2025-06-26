use common_daft_config::DaftExecutionConfig;

use crate::scan_task_iters::BoxScanTaskIter;

/// An iterator that determines whether incoming ScanTasks should be split by Parquet rowgroups.
///
/// # Returns
///
/// Returns an iterator of [`Decision`] objects indicating whether and how to split each task.
pub(super) struct DecideSplitIterator<'cfg> {
    inputs: BoxScanTaskIter<'cfg>,
    _cfg: &'cfg DaftExecutionConfig,
}

impl<'cfg> DecideSplitIterator<'cfg> {
    pub fn new(inputs: BoxScanTaskIter<'cfg>, cfg: &'cfg DaftExecutionConfig) -> Self {
        Self { inputs, _cfg: cfg }
    }
}

pub(super) struct Decision {}

impl Iterator for DecideSplitIterator<'_> {
    type Item = Decision;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(_scan_task) = self.inputs.next() {
            return Some(Decision {});
        }
        None
    }
}
