pub mod collect;
pub mod limit;

use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;

use crate::{
    executor::Executor,
    partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
};

pub trait SinkSpec<T: PartitionRef, E: Executor<T> + 'static> {
    fn to_runnable_sink(self: Box<Self>, executor: Arc<E>) -> Box<dyn Sink<T>>;

    fn buffer_size(&self) -> usize;
}

#[async_trait(?Send)]
pub trait Sink<T: PartitionRef> {
    async fn run(
        self: Box<Self>,
        inputs: Vec<VirtualPartitionSet<T>>,
        output_channel: tokio::sync::mpsc::Sender<DaftResult<Vec<T>>>,
    );
}
