use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_core::{join::JoinSide, prelude::SchemaRef};
use daft_micropartition::MicroPartition;

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::sinks::cross_join_collect::CrossJoinStateBridgeRef;

struct CrossJoinState(CrossJoinStateBridgeRef);

impl IntermediateOpState for CrossJoinState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct CrossJoinOperator {
    output_schema: SchemaRef,
    stream_side: JoinSide,
    state_bridge: CrossJoinStateBridgeRef,
}

impl CrossJoinOperator {
    pub(crate) fn new(
        output_schema: SchemaRef,
        stream_side: JoinSide,
        state_bridge: CrossJoinStateBridgeRef,
    ) -> Self {
        Self {
            output_schema,
            stream_side,
            state_bridge,
        }
    }
}

impl IntermediateOperator for CrossJoinOperator {
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn IntermediateOpState>,
        runtime: &RuntimeRef,
    ) -> IntermediateOpExecuteResult {
        let output_schema = self.output_schema.clone();

        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(output_schema)));
            return Ok((
                state,
                IntermediateOperatorResult::NeedMoreInput(Some(empty)),
            ))
            .into();
        }

        let stream_side = self.stream_side;

        runtime
            .spawn(async move {
                let cross_join_state = state
                    .as_any_mut()
                    .downcast_mut::<CrossJoinState>()
                    .expect("CrossJoinState should be used with CrossJoinOperator");

                let stream_tables = input.get_tables()?;
                let collect_tables = cross_join_state.0.get_state().await;

                let output_tables = Arc::new(
                    stream_tables
                        .iter()
                        .flat_map(|stream_tbl| {
                            collect_tables.iter().map(move |collect_tbl| {
                                let (left, right) = match stream_side {
                                    JoinSide::Left => (stream_tbl, collect_tbl),
                                    JoinSide::Right => (collect_tbl, stream_tbl),
                                };

                                left.cross_join(right, stream_side)
                            })
                        })
                        .collect::<DaftResult<Vec<_>>>()?,
                );

                let result = Arc::new(MicroPartition::new_loaded(
                    output_schema,
                    output_tables,
                    None,
                ));

                Ok((
                    state,
                    IntermediateOperatorResult::NeedMoreInput(Some(result)),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "CrossJoinOperator"
    }

    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(CrossJoinState(self.state_bridge.clone())))
    }
}
