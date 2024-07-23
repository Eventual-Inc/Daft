use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::sink::{SingleInputSink, SinkResultType};

#[derive(Clone)]
pub struct SortSink {
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
    parts: Vec<Arc<MicroPartition>>,
}

impl SortSink {
    pub fn new(sort_by: Vec<ExprRef>, descending: Vec<bool>) -> Self {
        Self {
            sort_by,
            descending,
            parts: Vec::new(),
        }
    }
}

impl SingleInputSink for SortSink {
    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        self.parts.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        false
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let concated =
            MicroPartition::concat(&self.parts.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
        let sorted = concated.sort(&self.sort_by, &self.descending)?;
        Ok(vec![Arc::new(sorted)])
    }
}
