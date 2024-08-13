use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::DEFAULT_MORSEL_SIZE;

/// State of an operator task, used to buffer data and output it when a threshold is reached.
pub struct OperatorTaskState {
    pub buffer: Vec<Arc<MicroPartition>>,
    pub curr_len: usize,
    pub threshold: usize,
}

impl OperatorTaskState {
    pub fn new() -> Self {
        Self {
            buffer: vec![],
            curr_len: 0,
            threshold: DEFAULT_MORSEL_SIZE,
        }
    }

    // Add a micro partition to the buffer.
    pub fn add(&mut self, part: Arc<MicroPartition>) {
        self.curr_len += part.len();
        self.buffer.push(part);
    }

    // Try to clear the buffer if the threshold is reached.
    pub fn try_clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        if self.curr_len >= self.threshold {
            self.clear()
        } else {
            None
        }
    }

    // Clear the buffer and return the concatenated MicroPartition.
    pub fn clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        if self.buffer.is_empty() {
            return None;
        }

        let concated =
            MicroPartition::concat(&self.buffer.iter().map(|x| x.as_ref()).collect::<Vec<_>>())
                .map(Arc::new);
        self.buffer.clear();
        self.curr_len = 0;
        Some(concated)
    }
}
