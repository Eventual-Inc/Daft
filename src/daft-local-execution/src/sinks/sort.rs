use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::sink::{Sink, SinkResultType};

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

impl Sink for SortSink {
    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(&mut self, index: usize, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        assert_eq!(index, 0);
        self.parts.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        false
    }

    fn num_inputs(&self) -> usize {
        1
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(self: Box<Self>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let concated =
            MicroPartition::concat(&self.parts.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
        let sorted = concated.sort(&self.sort_by, &self.descending)?;
        Ok(vec![Arc::new(sorted)])
    }
}
