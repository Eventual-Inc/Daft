use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinSide, prelude::SchemaRef};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::{Span, instrument};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName, state_bridge::BroadcastStateBridgeRef};

pub(crate) struct CrossJoinState {
    bridge: BroadcastStateBridgeRef<Vec<RecordBatch>>,
    stream_idx: usize,
    collect_idx: usize,
}

impl CrossJoinState {
    fn new(bridge: BroadcastStateBridgeRef<Vec<RecordBatch>>) -> Self {
        Self {
            bridge,
            stream_idx: 0,
            collect_idx: 0,
        }
    }
}

pub struct CrossJoinOperator {
    output_schema: SchemaRef,
    stream_side: JoinSide,
    state_bridge: BroadcastStateBridgeRef<Vec<RecordBatch>>,
}

impl CrossJoinOperator {
    pub(crate) fn new(
        output_schema: SchemaRef,
        stream_side: JoinSide,
        state_bridge: BroadcastStateBridgeRef<Vec<RecordBatch>>,
    ) -> Self {
        Self {
            output_schema,
            stream_side,
            state_bridge,
        }
    }
}

fn empty_result(
    state: CrossJoinState,
    output_schema: SchemaRef,
) -> DaftResult<(CrossJoinState, IntermediateOperatorResult)> {
    let empty = Arc::new(MicroPartition::empty(Some(output_schema)));

    Ok((
        state,
        IntermediateOperatorResult::NeedMoreInput(Some(empty)),
    ))
}

impl IntermediateOperator for CrossJoinOperator {
    type State = CrossJoinState;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    #[instrument(skip_all, name = "CrossJoinOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        let output_schema = self.output_schema.clone();

        if input.is_empty() {
            return empty_result(state, output_schema).into();
        }

        let stream_side = self.stream_side;

        task_spawner
            .spawn(
                async move {
                    let collect_tables = state.bridge.get_state().await;
                    if collect_tables.is_empty() {
                        return empty_result(state, output_schema);
                    }

                    let stream_tables = input.record_batches();

                    let stream_tbl = &stream_tables[state.stream_idx];
                    let collect_tbl = &collect_tables[state.collect_idx];

                    let (left_tbl, right_tbl) = match stream_side {
                        JoinSide::Left => (stream_tbl, collect_tbl),
                        JoinSide::Right => (collect_tbl, stream_tbl),
                    };

                    let output_tbl = left_tbl.cross_join(right_tbl, stream_side)?;

                    let output_morsel = Arc::new(MicroPartition::new_loaded(
                        output_schema,
                        Arc::new(vec![output_tbl]),
                        None,
                    ));

                    // increment inner loop index
                    state.collect_idx = (state.collect_idx + 1) % collect_tables.len();

                    if state.collect_idx == 0 {
                        // finished the inner loop, increment outer loop index
                        state.stream_idx = (state.stream_idx + 1) % stream_tables.len();
                    }

                    let result = if state.stream_idx == 0 && state.collect_idx == 0 {
                        // finished the outer loop, move onto next input
                        IntermediateOperatorResult::NeedMoreInput(Some(output_morsel))
                    } else {
                        // still looping through tables
                        IntermediateOperatorResult::HasMoreOutput(output_morsel)
                    };
                    Ok((state, result))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "CrossJoin".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::CrossJoin
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            "CrossJoin:".to_string(),
            format!("Stream Side = {:?}", self.stream_side),
        ]
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(CrossJoinState::new(self.state_bridge.clone()))
    }
    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }
}
