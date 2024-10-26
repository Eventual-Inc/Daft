use std::sync::Arc;

use async_trait::async_trait;
use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{
    DynStreamingSinkState, StreamingSink, StreamingSinkOutput, StreamingSinkState,
};
use crate::NUM_CPUS;

struct ConcatSinkState {}
impl DynStreamingSinkState for ConcatSinkState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct ConcatSink {}

#[async_trait]
impl StreamingSink for ConcatSink {
    /// By default, if the streaming_sink is called with maintain_order = true, input is distributed round-robin to the workers,
    /// and the output is received in the same order. Therefore, the 'execute' method does not need to do anything.
    /// If maintain_order = false, the input is distributed randomly to the workers, and the output is received in random order.
    #[instrument(skip_all, name = "ConcatSink::sink")]
    fn execute(
        &self,
        _index: usize,
        input: &Arc<MicroPartition>,
        _state_handle: &StreamingSinkState,
    ) -> DaftResult<StreamingSinkOutput> {
        Ok(StreamingSinkOutput::NeedMoreInput(Some(input.clone())))
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

    async fn make_state(&self) -> Box<dyn DynStreamingSinkState> {
        Box::new(ConcatSinkState {})
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    /// The ConcatSink does not do any computation in the sink method, so no need to buffer.
    fn morsel_size(&self) -> Option<usize> {
        None
    }
}
