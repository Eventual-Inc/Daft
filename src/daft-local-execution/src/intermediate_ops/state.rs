use std::sync::Arc;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::DEFAULT_MORSEL_SIZE;

pub trait OperatorState: Send + Sync {
    fn clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>>;
    fn add(&mut self, input: Arc<MicroPartition>);
    fn try_clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>>;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    fn as_any(&self) -> &dyn std::any::Any;
}

/// State of an operator task, used to buffer data and output it when a threshold is reached.
pub struct GenericOperatorState {
    pub buffer: Vec<Arc<MicroPartition>>,
    pub curr_len: usize,
    pub threshold: usize,
}

impl Default for GenericOperatorState {
    fn default() -> Self {
        Self::new(DEFAULT_MORSEL_SIZE)
    }
}

impl GenericOperatorState {
    pub fn new(threshold: usize) -> Self {
        Self {
            buffer: vec![],
            curr_len: 0,
            threshold,
        }
    }
}

impl OperatorState for GenericOperatorState {
    // Add a micro partition to the buffer.
    fn add(&mut self, part: Arc<MicroPartition>) {
        self.curr_len += part.len();
        self.buffer.push(part);
    }

    // Try to clear the buffer if the threshold is reached.
    fn try_clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
        if self.curr_len >= self.threshold {
            self.clear()
        } else {
            None
        }
    }

    // Clear the buffer and return the concatenated MicroPartition.
    fn clear(&mut self) -> Option<DaftResult<Arc<MicroPartition>>> {
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

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
