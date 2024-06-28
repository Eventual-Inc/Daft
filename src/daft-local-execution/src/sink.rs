use std::{collections::VecDeque, sync::Arc};

use daft_core::JoinType;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

pub enum SinkResultType {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    Finished(Option<Arc<MicroPartition>>),
}

pub trait Sink: Send {
    fn sink(&mut self, input: &Arc<MicroPartition>, id: usize) -> SinkResultType;
    fn queue_size(&self) -> usize;
    fn in_order(&self) -> bool;
    fn finalize(&mut self) -> Option<Vec<Arc<MicroPartition>>>;
}

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
    fn sink(&mut self, input: &Arc<MicroPartition>, _id: usize) -> SinkResultType {
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
    fn sink(&mut self, input: &Arc<MicroPartition>, _id: usize) -> SinkResultType {
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
        true
    }

    fn finalize(&mut self) -> Option<Vec<Arc<MicroPartition>>> {
        if self.partitions.is_empty() {
            return None;
        }
        return Some(self.partitions.drain(..).collect());
    }
}

pub struct HashJoinSink {
    partitions: VecDeque<Arc<MicroPartition>>,
    left_parts: Vec<Arc<MicroPartition>>,
    right_parts: Vec<Arc<MicroPartition>>,
    left_on: Vec<ExprRef>,
    right_on: Vec<ExprRef>,
    join_type: JoinType,
    left_id: usize,
    right_id: usize,
}

impl HashJoinSink {
    pub fn new(
        left_on: Vec<ExprRef>,
        right_on: Vec<ExprRef>,
        join_type: JoinType,
        left_id: usize,
        right_id: usize,
    ) -> Self {
        Self {
            partitions: VecDeque::new(),
            left_parts: Vec::new(),
            right_parts: Vec::new(),
            left_on,
            right_on,
            join_type,
            left_id,
            right_id,
        }
    }
}

impl Sink for HashJoinSink {
    fn sink(&mut self, input: &Arc<MicroPartition>, id: usize) -> SinkResultType {
        if id == self.left_id {
            self.left_parts.push(input.clone());
        } else if id == self.right_id {
            self.right_parts.push(input.clone());
        } else {
            panic!("Invalid id");
        }

        if self.left_parts.len() == 1 && self.right_parts.len() == 1 {
            let left = self.left_parts.pop().unwrap();
            let right = self.right_parts.pop().unwrap();
            let joined = left.hash_join(
                right.as_ref(),
                &self.left_on,
                &self.right_on,
                self.join_type,
            );
            self.partitions.push_back(Arc::new(joined.unwrap()));
        }

        let part = self.partitions.pop_front();
        SinkResultType::NeedMoreInput(part)
    }

    fn queue_size(&self) -> usize {
        32
    }

    fn in_order(&self) -> bool {
        true
    }

    fn finalize(&mut self) -> Option<Vec<Arc<MicroPartition>>> {
        if self.partitions.is_empty() {
            return None;
        }
        return Some(self.partitions.drain(..).collect());
    }
}
