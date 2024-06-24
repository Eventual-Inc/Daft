use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::simple::common::{Sink, SinkResultType};

pub struct LimitSink {
    limit: usize,
    partitions: Vec<Arc<MicroPartition>>,
    num_rows_taken: usize,
}

impl LimitSink {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            partitions: vec![],
            num_rows_taken: 0,
        }
    }
}

impl Sink for LimitSink {
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        println!("LimitSink::sink");
        let input_num_rows = input.len();

        if self.num_rows_taken + input_num_rows <= self.limit {
            self.num_rows_taken += input_num_rows;
            self.partitions.push(input.clone());
            Ok(SinkResultType::NeedMoreInput)
        } else {
            let num_rows_to_take = self.limit - self.num_rows_taken;
            let taken = input.head(num_rows_to_take)?;
            self.partitions.push(Arc::new(taken));
            Ok(SinkResultType::Finished)
        }
    }
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        println!("LimitSink::finalize");
        Ok(self.partitions.clone())
    }
}
