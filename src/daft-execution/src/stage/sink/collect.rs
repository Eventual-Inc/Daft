use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;

use crate::{
    executor::Executor,
    partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
    scheduler::streaming::{SenderWrapper, StreamingPartitionTaskScheduler},
    tree::OpNode,
};

use super::{Sink, SinkSpec};

#[derive(Debug)]
pub struct CollectSinkSpec<T: PartitionRef> {
    task_graph: OpNode,
    _marker: PhantomData<T>,
}

impl<T: PartitionRef> CollectSinkSpec<T> {
    pub fn new(task_graph: OpNode) -> Self {
        Self {
            task_graph,
            _marker: PhantomData,
        }
    }
}
impl<T: PartitionRef, E: Executor<T> + 'static> SinkSpec<T, E> for CollectSinkSpec<T> {
    fn to_runnable_sink(self: Box<Self>, executor: Arc<E>) -> Box<dyn Sink<T>> {
        Box::new(CollectSink {
            spec: self,
            executor,
        })
    }

    fn buffer_size(&self) -> usize {
        10000
    }
}

pub struct CollectSink<T: PartitionRef, E: Executor<T>> {
    spec: Box<CollectSinkSpec<T>>,
    executor: Arc<E>,
}

#[async_trait(?Send)]
impl<T: PartitionRef, E: Executor<T> + 'static> Sink<T> for CollectSink<T, E> {
    async fn run(
        self: Box<Self>,
        inputs: Vec<VirtualPartitionSet<T>>,
        output_channel: tokio::sync::mpsc::Sender<DaftResult<Vec<T>>>,
    ) {
        let wrapper = SenderWrapper(output_channel);
        let task_scheduler = StreamingPartitionTaskScheduler::new(
            self.spec.task_graph,
            inputs,
            wrapper,
            None,
            self.executor.clone(),
        );
        task_scheduler.execute().await;
    }
}
