use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkStatus};

pub struct SortSink {
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
    state: Vec<Arc<MicroPartition>>,
}

impl SortSink {
    pub fn new(sort_by: Vec<ExprRef>, descending: Vec<bool>) -> Self {
        Self {
            sort_by,
            descending,
            state: vec![],
        }
    }
    pub fn boxed(self) -> Box<dyn BlockingSink> {
        Box::new(self)
    }
}

impl BlockingSink for SortSink {
    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus> {
        self.state.push(input.clone());
        Ok(BlockingSinkStatus::NeedMoreInput)
    }
    fn name(&self) -> &'static str {
        "Sort"
    }
    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        assert!(
            !self.state.is_empty(),
            "We can not finalize SortSink with no data"
        );
        let concated =
            MicroPartition::concat(&self.state.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
        let sorted = concated.sort(&self.sort_by, &self.descending)?;
        Ok(Some(Arc::new(sorted)))
    }
}
