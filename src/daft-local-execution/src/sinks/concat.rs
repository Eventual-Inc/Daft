use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::{
    sink::{DoubleInputSink, SinkResultType},
    state::SinkTaskState,
};

#[derive(Clone)]
pub struct ConcatSink {}

impl ConcatSink {
    pub fn new() -> Self {
        Self {}
    }
}

impl DoubleInputSink for ConcatSink {
    #[instrument(skip_all, name = "ConcatSink::sink")]
    fn sink_left(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut SinkTaskState,
    ) -> DaftResult<SinkResultType> {
        state.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    #[instrument(skip_all, name = "ConcatSink::sink")]
    fn sink_right(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut SinkTaskState,
    ) -> DaftResult<SinkResultType> {
        state.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        true
    }

    #[instrument(skip_all, name = "ConcatSink::finalize")]
    fn finalize(
        &self,
        input_left: &Arc<MicroPartition>,
        input_right: &Arc<MicroPartition>,
    ) -> DaftResult<Vec<Arc<MicroPartition>>> {
        Ok(vec![input_left.clone(), input_right.clone()])
    }

    fn name(&self) -> &'static str {
        "Concat"
    }
}
