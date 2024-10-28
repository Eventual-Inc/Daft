use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{
    DynStreamingSinkState, StreamingSink, StreamingSinkOutput, StreamingSinkState,
};
use crate::pipeline::PipelineResultType;

struct ConcatSinkState {
    // The index of the last morsel of data that was received, which should be strictly non-decreasing.
    pub curr_idx: usize,
}
impl DynStreamingSinkState for ConcatSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct ConcatSink {}

impl StreamingSink for ConcatSink {
    /// Execute for the ConcatSink operator does not do any computation and simply returns the input data.
    /// It only expects that the indices of the input data are strictly non-decreasing.
    /// TODO(Colin): If maintain_order is false, technically we could accept any index. Make this optimization later.
    #[instrument(skip_all, name = "ConcatSink::sink")]
    fn execute(
        &self,
        index: usize,
        input: &PipelineResultType,
        state_handle: &StreamingSinkState,
    ) -> DaftResult<StreamingSinkOutput> {
        state_handle.with_state_mut::<ConcatSinkState, _, _>(|state| {
            // If the index is the same as the current index or one more than the current index, then we can accept the morsel.
            if state.curr_idx == index || state.curr_idx + 1 == index {
                state.curr_idx = index;
                Ok(StreamingSinkOutput::NeedMoreInput(Some(
                    input.as_data().clone(),
                )))
            } else {
                Err(DaftError::ComputeError(format!("Concat sink received out-of-order data. Expected index to be {} or {}, but got {}.", state.curr_idx, state.curr_idx + 1, index)))
            }
        })
    }

    fn name(&self) -> &'static str {
        "Concat"
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn DynStreamingSinkState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        Ok(None)
    }

    fn make_state(&self) -> Box<dyn DynStreamingSinkState> {
        Box::new(ConcatSinkState { curr_idx: 0 })
    }

    /// Since the ConcatSink does not do any computation, it does not need to spawn multiple workers.
    fn max_concurrency(&self) -> usize {
        1
    }
}
