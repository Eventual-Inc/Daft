use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{
    DynStreamingSinkState, StreamingSink, StreamingSinkOutput, StreamingSinkState,
};
use crate::pipeline::PipelineResultType;

struct LimitSinkState {
    remaining: usize,
}

impl LimitSinkState {
    fn new(remaining: usize) -> Self {
        Self { remaining }
    }

    fn get_remaining_mut(&mut self) -> &mut usize {
        &mut self.remaining
    }
}

impl DynStreamingSinkState for LimitSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct LimitSink {
    limit: usize,
}

impl LimitSink {
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }
}

impl StreamingSink for LimitSink {
    #[instrument(skip_all, name = "LimitSink::sink")]
    fn execute(
        &self,
        index: usize,
        input: &PipelineResultType,
        state_handle: &StreamingSinkState,
    ) -> DaftResult<StreamingSinkOutput> {
        assert_eq!(index, 0);
        let input = input.as_data();
        let input_num_rows = input.len();

        state_handle.with_state_mut::<LimitSinkState, _, _>(|state| {
            let remaining = state.get_remaining_mut();
            use std::cmp::Ordering::{Equal, Greater, Less};
            match input_num_rows.cmp(remaining) {
                Less => {
                    *remaining -= input_num_rows;
                    Ok(StreamingSinkOutput::NeedMoreInput(Some(input.clone())))
                }
                Equal => {
                    *remaining = 0;
                    Ok(StreamingSinkOutput::Finished(Some(input.clone())))
                }
                Greater => {
                    let taken = input.head(*remaining)?;
                    *remaining = 0;
                    Ok(StreamingSinkOutput::Finished(Some(Arc::new(taken))))
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "Limit"
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn DynStreamingSinkState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        Ok(None)
    }

    fn make_state(&self) -> Box<dyn DynStreamingSinkState> {
        Box::new(LimitSinkState::new(self.limit))
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}
