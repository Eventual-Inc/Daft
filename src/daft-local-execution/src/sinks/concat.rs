use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::sink::{DoubleInputSink, SinkResultType};

#[derive(Clone)]
pub struct ConcatSink {
    result_left: Vec<Arc<MicroPartition>>,
    result_right: Vec<Arc<MicroPartition>>,
}

impl ConcatSink {
    pub fn new() -> Self {
        Self {
            result_left: Vec::new(),
            result_right: Vec::new(),
        }
    }
}

impl DoubleInputSink for ConcatSink {
    #[instrument(skip_all, name = "ConcatSink::sink")]
    fn sink_left(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        self.result_left.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    #[instrument(skip_all, name = "ConcatSink::sink")]
    fn sink_right(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        self.result_right.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        true
    }

    #[instrument(skip_all, name = "ConcatSink::finalize")]
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        Ok(self
            .result_left
            .clone()
            .into_iter()
            .chain(self.result_right.clone())
            .collect())
    }

    fn name(&self) -> &'static str {
        "Concat"
    }
}
