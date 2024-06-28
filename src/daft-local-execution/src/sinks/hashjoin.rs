use std::{collections::VecDeque, sync::Arc};

use daft_core::JoinType;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

use super::sink::{Sink, SinkResultType};

pub struct HashJoinSink {
    partitions: VecDeque<Arc<MicroPartition>>,
    left_parts: VecDeque<Arc<MicroPartition>>,
    right_parts: VecDeque<Arc<MicroPartition>>,
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
            left_parts: VecDeque::new(),
            right_parts: VecDeque::new(),
            left_on,
            right_on,
            join_type,
            left_id,
            right_id,
        }
    }
}

impl Sink for HashJoinSink {
    fn sink(&mut self, input: &[Arc<MicroPartition>], id: usize) -> SinkResultType {
        assert_eq!(input.len(), 1);
        let input = input.first().unwrap();
        if id == self.left_id {
            self.left_parts.push_back(input.clone());
        } else if id == self.right_id {
            self.right_parts.push_back(input.clone());
        } else {
            panic!("Invalid id");
        }

        while !self.left_parts.is_empty() && !self.right_parts.is_empty() {
            let left = self.left_parts.pop_front().unwrap();
            let right = self.right_parts.pop_front().unwrap();
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
        let returning = self.partitions.drain(..).collect::<Vec<_>>();
        Some(returning)
    }
}
