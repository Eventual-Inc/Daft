use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::{
    sink::{SingleInputSink, SinkResultType},
    state::SinkTaskState,
};

#[derive(Clone)]
pub struct SortSink {
    sort_by: Vec<ExprRef>,
    descending: Vec<bool>,
}

impl SortSink {
    pub fn new(sort_by: Vec<ExprRef>, descending: Vec<bool>) -> Self {
        Self {
            sort_by,
            descending,
        }
    }
}

impl SingleInputSink for SortSink {
    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut SinkTaskState,
    ) -> DaftResult<SinkResultType> {
        state.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        false
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn can_parallelize(&self) -> bool {
        true
    }

    fn finalize(&self, input: &Arc<MicroPartition>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let sorted = input.sort(&self.sort_by, &self.descending)?;
        Ok(vec![Arc::new(sorted)])
    }
}
