use std::{cmp::Ordering::*, collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

pub struct RowBasedBuffer {
    pub buffer: VecDeque<Arc<MicroPartition>>,
    pub curr_len: usize,
    pub threshold: usize,
}

impl RowBasedBuffer {
    pub fn new(threshold: usize) -> Self {
        assert!(threshold > 0);
        Self {
            buffer: VecDeque::new(),
            curr_len: 0,
            threshold,
        }
    }

    pub fn push(&mut self, part: Arc<MicroPartition>) {
        self.curr_len += part.len();
        self.buffer.push_back(part);
    }

    pub fn pop_enough(&mut self) -> DaftResult<Option<Vec<Arc<MicroPartition>>>> {
        match self.curr_len.cmp(&self.threshold) {
            Less => Ok(None),
            Equal => {
                if self.buffer.len() == 1 {
                    let part = self.buffer.pop_front().unwrap();
                    self.curr_len = 0;
                    Ok(Some(vec![part]))
                } else {
                    let chunk = MicroPartition::concat(
                        &std::mem::take(&mut self.buffer)
                            .iter()
                            .map(|x| x.as_ref())
                            .collect::<Vec<_>>(),
                    )?;
                    self.curr_len = 0;
                    Ok(Some(vec![Arc::new(chunk)]))
                }
            }
            Greater => {
                let num_ready_chunks = self.curr_len / self.threshold;
                let concated = MicroPartition::concat(
                    &std::mem::take(&mut self.buffer)
                        .iter()
                        .map(|x| x.as_ref())
                        .collect::<Vec<_>>(),
                )?;
                let mut start = 0;
                let mut parts_to_return = Vec::with_capacity(num_ready_chunks);
                for _ in 0..num_ready_chunks {
                    let end = start + self.threshold;
                    let part = Arc::new(concated.slice(start, end)?);
                    parts_to_return.push(part);
                    start = end;
                }
                if start < concated.len() {
                    let part = Arc::new(concated.slice(start, concated.len())?);
                    self.curr_len = part.len();
                    self.buffer.push_back(part);
                } else {
                    self.curr_len = 0;
                }
                Ok(Some(parts_to_return))
            }
        }
    }

    pub fn pop_all(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        assert!(self.curr_len < self.threshold);
        if self.buffer.is_empty() {
            Ok(None)
        } else {
            let concated = MicroPartition::concat(
                &std::mem::take(&mut self.buffer)
                    .iter()
                    .map(|x| x.as_ref())
                    .collect::<Vec<_>>(),
            )?;
            self.curr_len = 0;
            Ok(Some(Arc::new(concated)))
        }
    }
}
