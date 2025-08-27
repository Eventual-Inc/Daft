use std::sync::Arc;

use common_runtime::get_compute_pool_num_threads;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{ops::NodeType, pipeline::NodeName, ExecutionTaskSpawner};

pub struct ConcatSink {}

impl StreamingSink for ConcatSink {
    type State = ();
    /// By default, if the streaming_sink is called with maintain_order = true, input is distributed round-robin to the workers,
    /// and the output is received in the same order. Therefore, the 'execute' method does not need to do anything.
    /// If maintain_order = false, the input is distributed randomly to the workers, and the output is received in random order.
    #[instrument(skip_all, name = "ConcatSink::sink")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Self::State,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        Ok((state, StreamingSinkOutput::NeedMoreInput(Some(input)))).into()
    }

    fn name(&self) -> NodeName {
        "Concat".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Concat
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["Concat".to_string()]
    }

    fn finalize(
        &self,
        _states: Vec<Self::State>,
        _spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        Ok(None).into()
    }

    fn make_state(&self) -> Self::State {}

    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
}
