use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

/// State of an sink task, used to store intermediate results.
pub struct SinkTaskState {
    pub buffer: Vec<Arc<MicroPartition>>,
    pub curr_len: usize,
}

impl SinkTaskState {
    pub fn new() -> Self {
        Self {
            buffer: vec![],
            curr_len: 0,
        }
    }

    // Push a micro partition to the buffer.
    pub fn push(&mut self, part: Arc<MicroPartition>) {
        self.curr_len += part.len();
        self.buffer.push(part);
    }

    // Clear the buffer and return the concatenated MicroPartition.
    pub fn clear(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let concated =
            MicroPartition::concat(&self.buffer.iter().map(|p| p.as_ref()).collect::<Vec<_>>())?;
        self.buffer.clear();
        self.curr_len = 0;
        Ok(Some(Arc::new(concated)))
    }
}
