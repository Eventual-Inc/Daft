use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use super::sink::{SingleInputSink, SinkResultType};

#[derive(Clone)]
pub struct LimitSink {
    limit: usize,
    num_rows_taken: usize,
    result: Vec<Arc<MicroPartition>>,
}

impl LimitSink {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            num_rows_taken: 0,
            result: Vec::new(),
        }
    }
}

impl SingleInputSink for LimitSink {
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        log::debug!("LimitSink::sink");
        let input_num_rows = input.len();

        if self.num_rows_taken == self.limit {
            return Ok(SinkResultType::Finished);
        }

        if self.num_rows_taken + input_num_rows <= self.limit {
            self.num_rows_taken += input_num_rows;
            self.result.push(input.clone());
            Ok(SinkResultType::NeedMoreInput)
        } else {
            let num_rows_to_take = self.limit - self.num_rows_taken;
            let taken = input.head(num_rows_to_take)?;
            self.num_rows_taken = self.limit;
            self.result.push(Arc::new(taken));
            Ok(SinkResultType::Finished)
        }
    }

    fn in_order(&self) -> bool {
        false
    }

    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        log::debug!("LimitSink::finalize");
        Ok(self.result.clone())
    }
}
