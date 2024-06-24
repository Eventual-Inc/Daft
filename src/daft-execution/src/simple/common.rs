use std::sync::Arc;

use daft_micropartition::MicroPartition;

use std::any::Any;

use common_error::DaftResult;

pub enum SourceResultType {
    HasMoreData(Arc<MicroPartition>),
    Done,
}

pub trait Source {
    fn get_data(&mut self) -> DaftResult<SourceResultType>;
}

pub enum SinkResultType {
    NeedMoreInput,
    Finished,
}

pub trait Sink {
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType>;
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>>;
}

pub trait IntermediateOperator {
    fn execute(&mut self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>>;
}
