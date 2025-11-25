use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinType, prelude::SchemaRef};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::{Span, instrument};

use crate::{
    ExecutionTaskSpawner,
    pipeline::NodeName,
    state_bridge::BroadcastStateBridgeRef,
    streaming_sink::base::{
        StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
        StreamingSinkFinalizeResult, StreamingSinkOutput,
    },
};

pub(crate) enum SortMergeJoinProbeState {
    Building(BroadcastStateBridgeRef<Vec<RecordBatch>>),
    Probing(Arc<Vec<RecordBatch>>, Vec<Arc<MicroPartition>>),
}

impl SortMergeJoinProbeState {
    async fn get_or_await_probe_state(
        &mut self,
    ) -> (&mut Arc<Vec<RecordBatch>>, &mut Vec<Arc<MicroPartition>>) {
        if let Self::Building(bridge) = self {
            let build_state = bridge.get_state().await;
            *self = Self::Probing(build_state, vec![]);
        }
        match self {
            Self::Probing(bridge, probe_state) => (bridge, probe_state),
            _ => unreachable!(),
        }
    }
}

struct SortMergeJoinParams {
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    left_schema: SchemaRef,
    join_type: JoinType,
}

pub struct SortMergeJoinProbe {
    params: Arc<SortMergeJoinParams>,
    state_bridge: BroadcastStateBridgeRef<Vec<RecordBatch>>,
}

impl SortMergeJoinProbe {
    pub fn new(
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        left_schema: SchemaRef,
        join_type: JoinType,
        state_bridge: BroadcastStateBridgeRef<Vec<RecordBatch>>,
    ) -> Self {
        Self {
            params: Arc::new(SortMergeJoinParams {
                left_on,
                right_on,
                left_schema,
                join_type,
            }),
            state_bridge,
        }
    }
}

impl StreamingSink for SortMergeJoinProbe {
    type State = SortMergeJoinProbeState;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    #[instrument(skip_all, name = "SortMergeJoinProbe::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        // Just collect all inputs for now
        spawner
            .spawn(
                async move {
                    let (_, probe_contents) = state.get_or_await_probe_state().await;
                    probe_contents.push(input);
                    Ok((state, StreamingSinkOutput::NeedMoreInput(None)))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "SortMergeJoinProbe::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        debug_assert_eq!(states.len(), 1);
        let mut state = states.into_iter().next().expect("Expect exactly one state");
        let params = self.params.clone();

        spawner
            .spawn(
                async move {
                    let (build_contents, probe_contents) = state.get_or_await_probe_state().await;

                    let left_mp = MicroPartition::new_loaded(
                        params.left_schema.clone(),
                        build_contents.clone(),
                        None,
                    );
                    let right_mp = MicroPartition::concat(probe_contents.iter())?;

                    // TODO: Handle pre-sorted?
                    let joined = left_mp.sort_merge_join(
                        &right_mp,
                        &params.left_on,
                        &params.right_on,
                        params.join_type,
                        false,
                    )?;
                    Ok(StreamingSinkFinalizeOutput::Finished(Some(Arc::new(
                        joined,
                    ))))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "SortMergeJoinProbe".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::SortMergeJoinProbe
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["SortMergeJoinProbe".to_string()]
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(SortMergeJoinProbeState::Building(self.state_bridge.clone()))
    }
    fn batching_strategy(&self) -> Self::BatchingStrategy {
        crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        )
    }
}
