use std::sync::Arc;

use common_runtime::get_compute_pool_num_threads;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
    StreamingSinkState,
};
use crate::ExecutionTaskSpawner;

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
        input: Arc<MicroPartition>,
        state: Box<dyn StreamingSinkState>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult {
        Ok((state, StreamingSinkOutput::NeedMoreInput(Some(input)))).into()
    }

    fn name(&self) -> &'static str {
        "Concat"
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["Concat".to_string()]
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn StreamingSinkState>>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(ConcatSinkState {})
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}
