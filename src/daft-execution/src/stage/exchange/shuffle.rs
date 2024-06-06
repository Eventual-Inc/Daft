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
pub struct ShuffleExchange<T: PartitionRef, E: Executor<T>> {
    map_task_graph: OpNode,
    reduce_task_graph: OpNode,
    executor: Arc<E>,
    _marker: PhantomData<T>,
}

impl<T: PartitionRef, E: Executor<T>> ShuffleExchange<T, E> {
    pub fn new(map_task_graph: OpNode, reduce_task_graph: OpNode, executor: Arc<E>) -> Self {
        Self {
            map_task_graph,
            reduce_task_graph,
            executor,
            _marker: PhantomData,
        }
    }
}

#[async_trait(?Send)]
impl<T: PartitionRef, E: Executor<T>> Exchange<T> for ShuffleExchange<T, E> {
    async fn run(self: Box<Self>, inputs: Vec<VirtualPartitionSet<T>>) -> DaftResult<Vec<Vec<T>>> {
        let map_task_scheduler = BulkPartitionTaskScheduler::new(
            self.map_task_graph,
            inputs,
            None,
            self.executor.clone(),
        );
        let map_outs = map_task_scheduler.execute().await?;
        let reduce_ins = map_outs
            .into_iter()
            .map(|parts| VirtualPartitionSet::PartitionRef(parts))
            .collect::<Vec<_>>();
        let reduce_task_scheduler = BulkPartitionTaskScheduler::new(
            self.reduce_task_graph,
            reduce_ins,
            None,
            self.executor.clone(),
        );
        reduce_task_scheduler.execute().await
    }
}
