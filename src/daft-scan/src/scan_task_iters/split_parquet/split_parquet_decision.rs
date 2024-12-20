use common_daft_config::DaftExecutionConfig;

use crate::scan_task_iters::BoxScanTaskIter;

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

impl<'cfg> Iterator for DecideSplitIterator<'cfg> {
    type Item = Decision;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(_scan_task) = self.inputs.next() {
            return Some(Decision {});
        }
        None
    }
}
