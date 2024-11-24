use std::sync::Arc;

use common_runtime::RuntimeRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
    StreamingSinkState,
};
use crate::{
    dispatcher::{DispatchSpawner, RoundRobinDispatcher, UnorderedDispatcher},
    ExecutionRuntimeContext, NUM_CPUS,
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
        _runtime_ref: &RuntimeRef,
    ) -> StreamingSinkExecuteResult {
        Ok((state, StreamingSinkOutput::NeedMoreInput(Some(input)))).into()
    }

    fn name(&self) -> &'static str {
        "Concat"
    }

    fn finalize(
        &self,
        _states: Vec<Box<dyn StreamingSinkState>>,
        _runtime_ref: &RuntimeRef,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(ConcatSinkState {})
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    fn dispatch_spawner(
        &self,
        runtime_handle: &ExecutionRuntimeContext,
        maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        if maintain_order {
            Arc::new(RoundRobinDispatcher::new(Some(
                runtime_handle.default_morsel_size(),
            )))
        } else {
            Arc::new(UnorderedDispatcher::new(Some(
                runtime_handle.default_morsel_size(),
            )))
        }
    }
}
