use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::{
    sink::{SingleInputSink, SinkResultType},
    state::SinkTaskState,
};

#[derive(Clone)]
pub struct LimitSink {
    limit: usize,
}

impl LimitSink {
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }
}

impl SingleInputSink for LimitSink {
    #[instrument(skip_all, name = "LimitSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut SinkTaskState,
    ) -> DaftResult<SinkResultType> {

        if state.curr_len >= self.limit {
            Ok(SinkResultType::Finished)
        } else {
            let num_rows_to_take = self.limit - state.curr_len;
            if input.len() <= num_rows_to_take {
                state.push(input.clone());
                if state.curr_len == self.limit {
                    Ok(SinkResultType::Finished)
                } else {
                    Ok(SinkResultType::NeedMoreInput)
                }
            } else {
                let taken = input.head(num_rows_to_take)?;
                state.push(Arc::new(taken));
                Ok(SinkResultType::Finished)
            }
        }
    }

    fn in_order(&self) -> bool {
        true
    }

    #[instrument(skip_all, name = "LimitSink::finalize")]
    fn finalize(&self, input: &Arc<MicroPartition>) -> DaftResult<Vec<Arc<MicroPartition>>> {
        Ok(vec![input.clone()])
    }
}
