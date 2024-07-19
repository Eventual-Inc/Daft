use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

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
    fn sink_left(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        log::debug!("Concat::sink_left");

        self.result_left.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn sink_right(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        log::debug!("Concat::sink_right");

        self.result_right.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        true
    }

    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        log::debug!("Concat::finalize");
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
