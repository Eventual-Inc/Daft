use std::sync::Arc;

use common_runtime::get_compute_pool_num_threads;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
    StreamingSinkState,
};
use crate::{
    dispatcher::{DispatchSpawner, RoundRobinDispatcher, UnorderedDispatcher},
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

struct ConcatSinkState {}
impl StreamingSinkState for ConcatSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct ConcatSink {}

impl StreamingSink for ConcatSink {
    /// By default, if the streaming_sink is called with maintain_order = true, input is distributed round-robin to the workers,
    /// and the output is received in the same order. Therefore, the 'execute' method does not need to do anything.
    /// If maintain_order = false, the input is distributed randomly to the workers, and the output is received in random order.
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
        get_compute_pool_num_threads()
    }

    fn dispatch_spawner(
        &self,
        _runtime_handle: &ExecutionRuntimeContext,
        maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        if maintain_order {
            Arc::new(RoundRobinDispatcher::unbounded())
        } else {
            Arc::new(UnorderedDispatcher::unbounded())
        }
    }
}
