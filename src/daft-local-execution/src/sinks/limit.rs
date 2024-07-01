use std::sync::Arc;

use daft_micropartition::MicroPartition;

use super::sink::{Sink, SinkResultType};

pub struct LimitSink {
    limit: usize,
    num_rows_taken: usize,
}

impl LimitSink {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            num_rows_taken: 0,
        }
    }
}

impl Sink for LimitSink {
    fn sink(&mut self, input: &[Arc<MicroPartition>], _id: usize) -> SinkResultType {
        assert_eq!(input.len(), 1);
        let input = input.first().unwrap();
        let input_num_rows = input.len();

        if self.num_rows_taken == self.limit {
            return SinkResultType::Finished(None);
        }

        if self.num_rows_taken + input_num_rows <= self.limit {
            self.num_rows_taken += input_num_rows;
            SinkResultType::NeedMoreInput(Some(input.clone()))
        } else {
            let num_rows_to_take = self.limit - self.num_rows_taken;
            let taken = input.head(num_rows_to_take).unwrap();
            self.num_rows_taken = self.limit;
            SinkResultType::Finished(Some(Arc::new(taken)))
        }
    }

    fn queue_size(&self) -> usize {
        1
    }

    fn in_order(&self) -> bool {
        true
    }

    fn finalize(&mut self) -> Option<Vec<Arc<MicroPartition>>> {
        None
    }
}
