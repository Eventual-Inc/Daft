use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::streaming_sink::{StreamSinkOutput, StreamingSink};

pub struct ConcatSink {}

impl ConcatSink {
    pub fn new() -> Self {
        Self {}
    }
    pub fn boxed(self) -> Box<dyn StreamingSink> {
        Box::new(self)
    }
}

impl StreamingSink for ConcatSink {
    #[instrument(skip_all, name = "ConcatSink::sink")]
    fn execute(
        &mut self,
        _index: usize,
        input: &Arc<MicroPartition>,
    ) -> DaftResult<StreamSinkOutput> {
        Ok(StreamSinkOutput::NeedMoreInput(Some(input.clone())))
    }

    fn name(&self) -> &'static str {
        "Limit"
    }
}
