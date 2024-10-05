use std::{cmp::Ordering::*, collections::VecDeque};

use common_error::DaftResult;
use daft_table::Table;

pub struct OperatorBuffer {
    pub buffer: VecDeque<Table>,
    pub curr_len: usize,
    pub threshold: usize,
}

impl OperatorBuffer {
    pub fn new(threshold: usize) -> Self {
        assert!(threshold > 0);
        Self {
            buffer: VecDeque::new(),
            curr_len: 0,
            threshold,
        }
    }

    pub fn push(&mut self, parts: &[Table]) {
        for part in parts {
            self.buffer.push_back(part.clone());
            self.curr_len += part.len();
        }
    }

    pub fn try_clear(&mut self) -> Option<DaftResult<Vec<Table>>> {
        match self.curr_len.cmp(&self.threshold) {
            Less => None,
            Equal => self.clear_all(),
            Greater => Some(self.clear_enough()),
        }
    }

    fn clear_enough(&mut self) -> DaftResult<Vec<Table>> {
        assert!(self.curr_len > self.threshold);
        let mut to_concat = Vec::with_capacity(self.buffer.len());
        let mut remaining = self.threshold;

        while remaining > 0 {
            let part = self.buffer.pop_front().expect("Buffer should not be empty");
            let part_len = part.len();
            if part_len <= remaining {
                remaining -= part_len;
                to_concat.push(part);
            } else {
                let (head, tail) = part.split_at(remaining)?;
                remaining = 0;
                to_concat.push(head);
                self.buffer.push_front(tail);
                break;
            }
        }
        assert_eq!(remaining, 0);

        self.curr_len -= self.threshold;
        Ok(to_concat)
    }

    pub fn clear_all(&mut self) -> Option<DaftResult<Vec<Table>>> {
        if self.buffer.is_empty() {
            return None;
        }

        self.curr_len = 0;
        Some(
            std::mem::take(&mut self.buffer)
                .into_iter()
                .map(Ok)
                .collect(),
        )
    }
}
