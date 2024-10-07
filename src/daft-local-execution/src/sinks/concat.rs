use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{StreamingSink, StreamingSinkOutput, StreamingSinkState};
use crate::pipeline::PipelineResultType;

struct ConcatSinkState {}
impl StreamingSinkState for ConcatSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct ConcatSink {}

impl StreamingSink for ConcatSink {
    #[instrument(skip_all, name = "ConcatSink::sink")]
    fn execute(
        &self,
        _index: usize,
        input: &PipelineResultType,
        _state: &mut dyn StreamingSinkState,
    ) -> DaftResult<StreamingSinkOutput> {
        let input = input.as_data();
        Ok(StreamingSinkOutput::NeedMoreInput(Some(input.clone())))
    }

    fn name(&self) -> &'static str {
        "Concat"
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn StreamingSinkState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        Ok(None)
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(ConcatSinkState {})
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}
