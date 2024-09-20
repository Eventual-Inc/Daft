use std::{cmp::Ordering::*, collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

pub struct Buffer {
    pub buffer: VecDeque<Arc<MicroPartition>>,
    pub curr_size: usize,
    pub threshold: usize,
}

impl Buffer {
    pub fn new(threshold: usize) -> Self {
        assert!(threshold > 0);
        Self {
            buffer: VecDeque::new(),
            curr_size: 0,
            threshold,
        }
    }

    pub fn push(&mut self, part: Arc<MicroPartition>) {
        self.curr_size += part.size_bytes().unwrap().unwrap();
        self.buffer.push_back(part);
    }

    pub fn try_clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        match self.curr_size.cmp(&self.threshold) {
            Less => None,
            Equal => self.clear_all(),
            Greater => Some(self.clear_enough()),
        }
    }

    fn clear_enough(&mut self) -> DaftResult<Arc<MicroPartition>> {
        assert!(self.curr_size > self.threshold);

        let mut to_concat = Vec::with_capacity(self.buffer.len());
        let mut remaining = self.threshold;

        while remaining > 0 {
            let part = self.buffer.pop_front().expect("Buffer should not be empty");
            let part_size = part.size_bytes().unwrap().unwrap();
            if part_size <= remaining {
                remaining -= part_size;
                to_concat.push(part);
            } else {
                let num_rows_to_take =
                    (remaining as f64 / part_size as f64 * part.len() as f64).ceil() as usize;
                let (head, tail) = part.split_at(num_rows_to_take)?;
                remaining = 0;
                to_concat.push(Arc::new(head));
                self.buffer.push_front(Arc::new(tail));
                break;
            }
        }
        assert_eq!(remaining, 0);

        self.curr_size -= self.threshold;
        match to_concat.len() {
            1 => Ok(to_concat.pop().unwrap()),
            _ => MicroPartition::concat(&to_concat.iter().map(|x| x.as_ref()).collect::<Vec<_>>())
                .map(Arc::new),
        }
    }

    pub fn clear_all(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        if self.buffer.is_empty() {
            return None;
        }
        let concated =
            MicroPartition::concat(&self.buffer.iter().map(|x| x.as_ref()).collect::<Vec<_>>())
                .map(Arc::new);
        self.buffer.clear();
        self.curr_size = 0;
        Some(concated)
    }
}
