use std::sync::Arc;

use daft_micropartition::MicroPartition;

pub enum SinkResultType {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    Finished(Option<Arc<MicroPartition>>),
}

pub trait Sink: Send {
    fn sink(&mut self, input: &[Arc<MicroPartition>], id: usize) -> SinkResultType;
    fn queue_size(&self) -> usize;
    fn in_order(&self) -> bool;
    fn finalize(&mut self) -> Option<Vec<Arc<MicroPartition>>>;
}
