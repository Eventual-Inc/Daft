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
pub struct LimitSinkSpec<T: PartitionRef> {
    task_graph: OpNode,
    limit: usize,
    _marker: PhantomData<T>,
}

impl<T: PartitionRef> LimitSinkSpec<T> {
    pub fn new(task_graph: OpNode, limit: usize) -> Self {
        Self {
            task_graph,
            limit,
            _marker: PhantomData,
        }
    }
}
impl<T: PartitionRef, E: Executor<T> + 'static> SinkSpec<T, E> for LimitSinkSpec<T> {
    fn to_runnable_sink(self: Box<Self>, executor: Arc<E>) -> Box<dyn Sink<T>> {
        Box::new(LimitSink {
            spec: self,
            executor,
        })
    }

    fn buffer_size(&self) -> usize {
        1
    }
}

pub struct LimitSink<T: PartitionRef, E: Executor<T>> {
    spec: Box<LimitSinkSpec<T>>,
    executor: Arc<E>,
}

#[async_trait(?Send)]
impl<T: PartitionRef, E: Executor<T> + 'static> Sink<T> for LimitSink<T, E> {
    async fn run(
        self: Box<Self>,
        inputs: Vec<VirtualPartitionSet<T>>,
        output_channel: tokio::sync::mpsc::Sender<DaftResult<Vec<T>>>,
    ) {
        let limit = self.spec.limit;
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let wrapper = SenderWrapper(tx);
        let task_scheduler = StreamingPartitionTaskScheduler::new(
            self.spec.task_graph,
            inputs,
            wrapper,
            Some(1),
            self.executor.clone(),
        );
        let exec_fut = task_scheduler.execute();
        tokio::pin!(exec_fut);
        let mut running_num_rows = 0;
        // TODO(Clark): Send remaining rows limit to each new partition task.
        loop {
            tokio::select! {
                _ = &mut exec_fut => break,
                Some(part) = rx.recv() => {
                    if let Ok(ref p) = part {
                        assert!(p.len() == 1);
                        running_num_rows += p[0].metadata().num_rows.unwrap_or(0);
                    }
                    output_channel.send(part).await.unwrap();
                    if running_num_rows >= limit {
                        break;
                    }
                },
            };
        }
    }
}
