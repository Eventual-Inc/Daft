use std::sync::Arc;

use crate::common::{Sink, SinkResultType};
use common_error::DaftResult;
use daft_micropartition::MicroPartition;

#[derive(Clone)]
pub struct CoalesceSink {
    partitions: Vec<Arc<MicroPartition>>,
    holding_area: Vec<Arc<MicroPartition>>,
    chunk_sizes: Vec<usize>,
    current_chunk: usize,
}

impl CoalesceSink {
    pub fn new(num_from: usize, num_to: usize) -> Self {
        let q = num_from / num_to;
        let r = num_from % num_to;

        let mut distribution = vec![q; num_to];
        for bucket in distribution.iter_mut().take(r) {
            *bucket += 1;
        }

        Self {
            partitions: vec![],
            holding_area: vec![],
            chunk_sizes: distribution,
            current_chunk: 0,
        }
    }
}

impl Sink for CoalesceSink {
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        self.holding_area.push(input.clone());

        if self.holding_area.len() == self.chunk_sizes[self.current_chunk] {
            let concated_partition = MicroPartition::concat(
                &self
                    .holding_area
                    .iter()
                    .map(|x| x.as_ref())
                    .collect::<Vec<_>>(),
            )?;
            self.partitions.push(Arc::new(concated_partition));
            self.holding_area.clear();
        }

        Ok(SinkResultType::NeedMoreInput)
    }
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        Ok(self.partitions.clone())
    }
}
