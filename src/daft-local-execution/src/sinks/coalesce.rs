use std::{collections::VecDeque, sync::Arc};

use daft_micropartition::MicroPartition;

use super::sink::{Sink, SinkResultType};

pub struct CoalesceSink {
    partitions: VecDeque<Arc<MicroPartition>>,
    chunk_sizes: Vec<usize>,
    current_chunk: usize,
    holding_area: Vec<Arc<MicroPartition>>,
}

impl CoalesceSink {
    pub fn new(chunk_sizes: Vec<usize>) -> Self {
        Self {
            partitions: VecDeque::new(),
            chunk_sizes,
            current_chunk: 0,
            holding_area: Vec::new(),
        }
    }
}

impl Sink for CoalesceSink {
    fn sink(&mut self, input: &[Arc<MicroPartition>], _id: usize) -> SinkResultType {
        assert_eq!(input.len(), 1);
        let input = input.first().unwrap();
        self.holding_area.push(input.clone());

        if self.holding_area.len() == self.chunk_sizes[self.current_chunk] {
            let concated_partition = MicroPartition::concat(
                &self
                    .holding_area
                    .iter()
                    .map(|x| x.as_ref())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            self.partitions.push_back(Arc::new(concated_partition));
            self.holding_area.clear();
            self.current_chunk += 1;
        }

        let part = self.partitions.pop_front();
        if self.current_chunk == self.chunk_sizes.len() {
            SinkResultType::Finished(part)
        } else {
            SinkResultType::NeedMoreInput(part)
        }
    }

    fn queue_size(&self) -> usize {
        32
    }

    fn in_order(&self) -> bool {
        false
    }

    fn finalize(&mut self) -> Option<Vec<Arc<MicroPartition>>> {
        if self.partitions.is_empty() {
            return None;
        }
        return Some(self.partitions.drain(..).collect());
    }
}
