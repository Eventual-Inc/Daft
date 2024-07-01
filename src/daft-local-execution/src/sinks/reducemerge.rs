use std::sync::Arc;

use daft_micropartition::MicroPartition;

use super::sink::{Sink, SinkResultType};

pub struct ReduceMergeSink {
    partitions_to_reduce: Vec<Vec<Arc<MicroPartition>>>,
}

impl ReduceMergeSink {
    pub fn new() -> Self {
        Self {
            partitions_to_reduce: Vec::new(),
        }
    }
}

impl Sink for ReduceMergeSink {
    fn sink(&mut self, input: &[Arc<MicroPartition>], _id: usize) -> SinkResultType {
        self.partitions_to_reduce.push(input.to_vec());
        SinkResultType::NeedMoreInput(None)
    }

    fn queue_size(&self) -> usize {
        32
    }

    fn in_order(&self) -> bool {
        false
    }

    fn finalize(&mut self) -> Option<Vec<Arc<MicroPartition>>> {
        let mut results = vec![];
        for i in 0..self.partitions_to_reduce[0].len() {
            let mut parts = vec![];
            for j in 0..self.partitions_to_reduce.len() {
                parts.push(self.partitions_to_reduce[j][i].as_ref());
            }
            let reduced = MicroPartition::concat(parts.as_slice()).unwrap();
            results.push(Arc::new(reduced));
        }
        Some(results)
    }
}
