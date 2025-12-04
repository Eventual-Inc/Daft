use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::{info_span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName, state_bridge::BroadcastStateBridgeRef};

pub(crate) struct JoinCollectState(Option<Vec<RecordBatch>>);

pub struct JoinCollectSink {
    state_bridge: BroadcastStateBridgeRef<Vec<RecordBatch>>,
}

impl JoinCollectSink {
    pub(crate) fn new(state_bridge: BroadcastStateBridgeRef<Vec<RecordBatch>>) -> Self {
        Self { state_bridge }
    }
}

impl BlockingSink for JoinCollectSink {
    type State = JoinCollectState;

    fn name(&self) -> NodeName {
        "JoinCollect".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::JoinCollect
    }

    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        if input.is_empty() {
            return Ok(BlockingSinkStatus::NeedMoreInput(state)).into();
        }

        spawner
            .spawn(
                async move {
                    state
                        .0
                        .as_mut()
                        .expect("Collected tables should not be consumed before sink stage is done")
                        .extend(input.record_batches().iter().cloned());

                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                info_span!("JoinCollectSink::sink"),
            )
            .into()
    }

    #[instrument(skip_all, name = "JoinCollectSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        assert_eq!(states.len(), 1);
        let mut state = states.into_iter().next().unwrap();
        let tables = state
            .0
            .take()
            .expect("Cross join collect state should have tables before finalize is called");

        self.state_bridge.set_state(Arc::new(tables));
        Ok(BlockingSinkFinalizeOutput::Finished(vec![])).into()
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(JoinCollectState(Some(Vec::new())))
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["JoinCollect".to_string()]
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}
