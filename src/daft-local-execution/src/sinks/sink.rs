use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

pub enum SinkResultType {
    NeedMoreInput,
    Finished,
}

pub trait Sink: Send + Sync {
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType>;
    fn in_order(&self) -> bool;
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>>;
}
