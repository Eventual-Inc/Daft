use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_core::{join::JoinSide, prelude::SchemaRef};
use daft_micropartition::MicroPartition;
use daft_table::Table;
use tracing::{info_span, instrument, Instrument};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::state_bridge::BroadcastStateBridgeRef;

struct CrossJoinState {
    bridge: BroadcastStateBridgeRef<Vec<Table>>,
    stream_idx: usize,
    collect_idx: usize,
}

impl CrossJoinState {
    fn new(bridge: BroadcastStateBridgeRef<Vec<Table>>) -> Self {
        Self {
            bridge,
            stream_idx: 0,
            collect_idx: 0,
        }
    }
}

impl IntermediateOpState for CrossJoinState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct CrossJoinOperator {
    output_schema: SchemaRef,
    stream_side: JoinSide,
    state_bridge: BroadcastStateBridgeRef<Vec<Table>>,
}

impl CrossJoinOperator {
    pub(crate) fn new(
        output_schema: SchemaRef,
        stream_side: JoinSide,
        state_bridge: BroadcastStateBridgeRef<Vec<Table>>,
    ) -> Self {
        Self {
            output_schema,
            stream_side,
            state_bridge,
        }
    }
}

fn empty_result(
    state: Box<dyn IntermediateOpState>,
    output_schema: SchemaRef,
) -> DaftResult<(Box<dyn IntermediateOpState>, IntermediateOperatorResult)> {
    let empty = Arc::new(MicroPartition::empty(Some(output_schema)));

    Ok((
        state,
        IntermediateOperatorResult::NeedMoreInput(Some(empty)),
    ))
}

impl IntermediateOperator for CrossJoinOperator {
    #[instrument(skip_all, name = "CrossJoinOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn IntermediateOpState>,
        runtime: &RuntimeRef,
    ) -> IntermediateOpExecuteResult {
        let output_schema = self.output_schema.clone();

        if input.is_empty() {
            return empty_result(state, output_schema).into();
        }

        let stream_side = self.stream_side;

        runtime
            .spawn(
                async move {
                    let cross_join_state = state
                        .as_any_mut()
                        .downcast_mut::<CrossJoinState>()
                        .expect("CrossJoinState should be used with CrossJoinOperator");

                    let collect_tables = cross_join_state.bridge.get_state().await;
                    if collect_tables.is_empty() {
                        return empty_result(state, output_schema);
                    }

                    let stream_tables = input.get_tables()?;

                    let stream_tbl = &stream_tables[cross_join_state.stream_idx];
                    let collect_tbl = &collect_tables[cross_join_state.collect_idx];

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
                    cross_join_state.collect_idx =
                        (cross_join_state.collect_idx + 1) % collect_tables.len();

                    if cross_join_state.collect_idx == 0 {
                        // finished the inner loop, increment outer loop index
                        cross_join_state.stream_idx =
                            (cross_join_state.stream_idx + 1) % stream_tables.len();
                    }

                    let result =
                        if cross_join_state.stream_idx == 0 && cross_join_state.collect_idx == 0 {
                            // finished the outer loop, move onto next input
                            IntermediateOperatorResult::NeedMoreInput(Some(output_morsel))
                        } else {
                            // still looping through tables
                            IntermediateOperatorResult::HasMoreOutput(output_morsel)
                        };
                    Ok((state, result))
                }
                .instrument(info_span!("CrossJoinOperator::execute")),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "CrossJoinOperator"
    }

    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(CrossJoinState::new(self.state_bridge.clone())))
    }
}
