use std::sync::Arc;

use common_error::DaftResult;

use crate::{
    executor::Executor,
    partition::PartitionRef,
    stage::{ExchangeStage, SinkStage},
};

/// A runner for exchange stages.
pub struct ExchangeStageRunner<T: PartitionRef> {
    stage: ExchangeStage<T>,
}

impl<T: PartitionRef> ExchangeStageRunner<T> {
    pub fn new(stage: ExchangeStage<T>) -> Self {
        Self { stage }
    }
}

impl<T: PartitionRef> ExchangeStageRunner<T> {
    pub fn run(self) -> DaftResult<Vec<Vec<T>>> {
        log::info!("Running exchange stage: {}", self.stage.stage_id);
        let local = tokio::task::LocalSet::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        local.block_on(&runtime, async move {
            let stage = self.stage.op;
            let inputs = self.stage.inputs;
            // TODO(Clark): Select over the stage running future and a SIGINT future.
            tokio::task::spawn_local(async move { stage.run(inputs).await })
                .await
                .unwrap()
        })
    }
}

/// A runner for sink stages.
pub struct SinkStageRunner<T: PartitionRef, E: Executor<T> + 'static> {
    stage: SinkStage<T, E>,
}

impl<T: PartitionRef, E: Executor<T> + 'static> SinkStageRunner<T, E> {
    pub fn new(stage: SinkStage<T, E>) -> Self {
        Self { stage }
    }
}

impl<T: PartitionRef, E: Executor<T> + 'static> SinkStageRunner<T, E> {
    pub fn run(
        self,
        output_channel: tokio::sync::mpsc::Sender<DaftResult<Vec<T>>>,
        executor: Arc<E>,
    ) {
        log::info!("Running sink stage: {}", self.stage.stage_id);
        let local = tokio::task::LocalSet::new();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        local.block_on(&runtime, async move {
            let sink = self.stage.op.to_runnable_sink(executor);
            let inputs = self.stage.inputs;
            // TODO(Clark): Select over the stage running future and a SIGINT future.
            tokio::task::spawn_local(async move { sink.run(inputs, output_channel).await })
                .await
                .unwrap()
        })
    }
}
