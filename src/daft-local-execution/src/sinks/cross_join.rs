use std::sync::Arc;

use common_runtime::RuntimeRef;
use daft_core::{join::JoinSide, prelude::SchemaRef};
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
    StreamingSinkState,
};
use crate::{
    dispatcher::{DispatchSpawner, UnorderedDispatcher},
    ExecutionRuntimeContext,
};

#[derive(Default)]
struct CrossJoinSinkState {
    right_side_buffer: Vec<Arc<MicroPartition>>,
    received_left_morsels: bool,
    loop_index: usize,
}

impl StreamingSinkState for CrossJoinSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct CrossJoinSink {
    right_schema: SchemaRef,
}

impl CrossJoinSink {
    pub fn new(right_schema: SchemaRef) -> Self {
        Self { right_schema }
    }
}

impl StreamingSink for CrossJoinSink {
    /// Cross join execute expects all right side morsels to be passed in first and then all left side morsels
    #[instrument(skip_all, name = "CrossJoinSink::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        runtime_ref: &RuntimeRef,
    ) -> StreamingSinkExecuteResult {
        if input.schema() == self.right_schema {
            // collect right side morsels

            let join_state = state
                .as_any_mut()
                .downcast_mut::<CrossJoinSinkState>()
                .expect("Cross join sink should have CrossJoinSinkState");

            assert!(!join_state.received_left_morsels, "cross join morsels received out of order, expected all right side and then all left side");

            join_state.right_side_buffer.push(input);
            Ok((state, StreamingSinkOutput::NeedMoreInput(None))).into()
        } else {
            runtime_ref
                .spawn(async move {
                    // cross join left side morsel with each right side morsel, emitting the join between one pair of morsels at a time.

                    let join_state = state
                        .as_any_mut()
                        .downcast_mut::<CrossJoinSinkState>()
                        .expect("Cross join sink should have CrossJoinSinkState");

                    join_state.received_left_morsels = true;

                    let right = &join_state.right_side_buffer[join_state.loop_index];
                    let output = Arc::new(input.cross_join(right, JoinSide::Left)?);

                    join_state.loop_index =
                        (join_state.loop_index + 1) % join_state.right_side_buffer.len();

                    if join_state.loop_index == 0 {
                        Ok((state, StreamingSinkOutput::NeedMoreInput(Some(output))))
                    } else {
                        Ok((state, StreamingSinkOutput::HasMoreOutput(output)))
                    }
                })
                .into()
        }
    }

    fn name(&self) -> &'static str {
        "CrossJoin"
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn StreamingSinkState>>,
        _runtime_ref: &RuntimeRef,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::<CrossJoinSinkState>::default()
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn dispatch_spawner(
        &self,
        runtime_handle: &ExecutionRuntimeContext,
        _maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        // Dispatcher doesn't matter since there will only be one op running
        Arc::new(UnorderedDispatcher::new(Some(
            runtime_handle.default_morsel_size(),
        )))
    }
}
