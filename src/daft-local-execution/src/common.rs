use std::sync::Arc;

use daft_micropartition::MicroPartition;
use daft_scan::ScanTask;

use common_error::DaftResult;

pub enum SinkResultType {
    NeedMoreInput,
    Finished,
}

#[derive(Clone)]
pub enum SourceType {
    ScanTask(Arc<ScanTask>),
    InMemory(Arc<MicroPartition>),
}

pub trait Sink: Send + dyn_clone::DynClone {
    // make sink streaming
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType>;
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>>;
}

dyn_clone::clone_trait_object!(Sink);
