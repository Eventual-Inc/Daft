use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::{info_span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkState, BlockingSinkStatus,
};
use crate::{state_bridge::BroadcastStateBridgeRef, ExecutionTaskSpawner};

struct CrossJoinCollectState(Option<Vec<RecordBatch>>);

impl BlockingSinkState for CrossJoinCollectState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct CrossJoinCollectSink {
    state_bridge: BroadcastStateBridgeRef<Vec<RecordBatch>>,
}

impl CrossJoinCollectSink {
    pub(crate) fn new(state_bridge: BroadcastStateBridgeRef<Vec<RecordBatch>>) -> Self {
        Self { state_bridge }
    }
}

impl BlockingSink for CrossJoinCollectSink {
    fn name(&self) -> &'static str {
        "CrossJoinCollect"
    }

    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        if input.is_empty() {
            return Ok(BlockingSinkStatus::NeedMoreInput(state)).into();
        }

        spawner
            .spawn(
                async move {
                    let cross_join_collect_state = state
                        .as_any_mut()
                        .downcast_mut::<CrossJoinCollectState>()
                        .expect("CrossJoinCollectSink should have CrossJoinCollectState");

                    cross_join_collect_state
                        .0
                        .as_mut()
                        .expect("Collected tables should not be consumed before sink stage is done")
                        .extend(input.get_tables()?.iter().cloned());

                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                info_span!("CrossJoinCollectSink::sink"),
            )
            .into()
    }

    #[instrument(skip_all, name = "CrossJoinCollectSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let mut state = states.into_iter().next().unwrap();
        let cross_join_collect_state = state
            .as_any_mut()
            .downcast_mut::<CrossJoinCollectState>()
            .expect("CrossJoinCollectSink should have CrossJoinCollectState");

        let tables = cross_join_collect_state
            .0
            .take()
            .expect("Cross join collect state should have tables before finalize is called");

        self.state_bridge.set_state(Arc::new(tables));
        Ok(BlockingSinkFinalizeOutput::Finished(vec![])).into()
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(CrossJoinCollectState(Some(Vec::new()))))
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["CrossJoinCollect".to_string()]
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}
