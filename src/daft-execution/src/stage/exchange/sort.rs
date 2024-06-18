use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;

use crate::{
    executor::Executor,
    partition::{virtual_partition::VirtualPartitionSet, PartitionRef},
    scheduler::bulk::BulkPartitionTaskScheduler,
    tree::OpNode,
};

use super::{Exchange, ShuffleExchange};

#[derive(Debug)]
pub struct SortExchange<T: PartitionRef, E: Executor<T>> {
    upstream_task_graph: OpNode,
    sampling_task_graph: OpNode,
    reduce_to_quantiles_task_graph: OpNode,
    shuffle_exchange: Box<ShuffleExchange<T, E>>,
    executor: Arc<E>,
    _marker: PhantomData<T>,
}

impl<T: PartitionRef, E: Executor<T>> SortExchange<T, E> {
    pub fn new(
        upstream_task_graph: OpNode,
        sampling_task_graph: OpNode,
        reduce_to_quantiles_task_graph: OpNode,
        map_task_graph: OpNode,
        reduce_task_graph: OpNode,
        executor: Arc<E>,
    ) -> Self {
        let shuffle_exchange = Box::new(ShuffleExchange::new(
            map_task_graph,
            reduce_task_graph,
            executor.clone(),
        ));
        Self {
            upstream_task_graph,
            sampling_task_graph,
            reduce_to_quantiles_task_graph,
            shuffle_exchange,
            executor,
            _marker: PhantomData,
        }
    }
}

#[async_trait(?Send)]
impl<T: PartitionRef, E: Executor<T>> Exchange<T> for SortExchange<T, E> {
    async fn run(self: Box<Self>, inputs: Vec<VirtualPartitionSet<T>>) -> DaftResult<Vec<Vec<T>>> {
        let upstream_task_scheduler = BulkPartitionTaskScheduler::new(
            self.upstream_task_graph,
            inputs,
            None,
            self.executor.clone(),
        );
        let upstream_outs = upstream_task_scheduler.execute().await?;
        assert!(upstream_outs.len() == 1);
        let upstream_outs = upstream_outs
            .into_iter()
            .map(|out| VirtualPartitionSet::PartitionRef(out))
            .collect::<Vec<_>>();
        let sample_task_scheduler = BulkPartitionTaskScheduler::new(
            self.sampling_task_graph,
            upstream_outs.clone(),
            None,
            self.executor.clone(),
        );
        let sample_outs = sample_task_scheduler.execute().await?;
        let sample_outs = sample_outs
            .into_iter()
            .map(|out| VirtualPartitionSet::PartitionRef(out))
            .collect::<Vec<_>>();

        let quantiles_task_scheduler = BulkPartitionTaskScheduler::new(
            self.reduce_to_quantiles_task_graph,
            sample_outs,
            None,
            self.executor.clone(),
        );
        let boundaries = quantiles_task_scheduler.execute().await?;
        assert!(boundaries.len() == 1);
        let boundaries = boundaries.into_iter().next().unwrap();
        let inputs = vec![
            VirtualPartitionSet::PartitionRef(boundaries),
            upstream_outs.into_iter().next().unwrap(),
        ];
        self.shuffle_exchange.run(inputs).await
    }
}
