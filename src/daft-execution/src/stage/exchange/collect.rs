use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;

use crate::{
    executor::Executor,
    partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
    scheduler::bulk::BulkPartitionTaskScheduler,
    tree::OpNode,
};

use super::Exchange;

#[derive(Debug)]
pub struct CollectExchange<T: PartitionRef, E: Executor<T>> {
    task_graph: OpNode,
    executor: Arc<E>,
    _marker: PhantomData<T>,
}

impl<T: PartitionRef, E: Executor<T>> CollectExchange<T, E> {
    pub fn new(task_graph: OpNode, executor: Arc<E>) -> Self {
        Self {
            task_graph,
            executor,
            _marker: PhantomData,
        }
    }
}

#[async_trait(?Send)]
impl<T: PartitionRef, E: Executor<T>> Exchange<T> for CollectExchange<T, E> {
    async fn run(self: Box<Self>, inputs: Vec<VirtualPartitionSet<T>>) -> DaftResult<Vec<Vec<T>>> {
        let task_scheduler =
            BulkPartitionTaskScheduler::new(self.task_graph, inputs, None, self.executor.clone());
        task_scheduler.execute().await
    }
}
