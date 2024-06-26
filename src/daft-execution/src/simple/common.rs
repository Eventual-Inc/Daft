use std::sync::Arc;

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use std::any::Any;

use common_error::DaftResult;

pub enum SourceResultType {
    HasMoreData(Arc<MicroPartition>),
    Done,
}

pub enum SinkResultType {
    NeedMoreInput,
    Finished,
}

#[derive(Clone)]
pub enum SourceType {
    ScanTask(Arc<ScanTask>),
    InMemory(Arc<MicroPartition>),
}

pub trait Sink: Send {
    // make sink streaming
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType>;
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>>;
}
