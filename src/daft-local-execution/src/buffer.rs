use std::{collections::VecDeque, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

#[derive(Debug, PartialEq)]
enum BufferState {
    BelowLowerBound,
    WithinRange,
    AboveUpperBound,
}

// A buffer that accumulates morsels until a threshold is reached
pub struct RowBasedBuffer {
    buffer: VecDeque<Arc<MicroPartition>>,
    curr_len: usize,
    lower_bound: usize,
    upper_bound: usize,
}

impl RowBasedBuffer {
    pub fn new(lower_bound: usize, upper_bound: usize) -> Self {
        assert!(lower_bound <= upper_bound);
        Self {
            buffer: VecDeque::new(),
            curr_len: 0,
            lower_bound,
            upper_bound,
        }
    }

    // Push a morsel to the buffer
    pub fn push(&mut self, part: &Arc<MicroPartition>) {
        self.curr_len += part.len();
        self.buffer.push_back(part.clone());
    }

    fn buffer_state(&self) -> BufferState {
        match (
            self.lower_bound <= self.curr_len,
            self.curr_len <= self.upper_bound,
        ) {
            (true, true) => BufferState::WithinRange,
            (true, false) => BufferState::AboveUpperBound,
            (false, true) => BufferState::BelowLowerBound,
            (false, false) => {
                unreachable!("Impossible to be below lower bound and above upper bound")
            }
        }
    }

    // Pop enough morsels that reach the threshold
    // - If the buffer currently has not enough morsels, return None
    // - If the buffer has exactly enough morsels, return the morsels
    // - If the buffer has more than enough morsels, return a vec of morsels, each correctly sized to the threshold.
    //   The remaining morsels will be pushed back to the buffer
    pub fn pop_enough(&mut self) -> DaftResult<Option<Vec<Arc<MicroPartition>>>> {
        match self.buffer_state() {
            BufferState::BelowLowerBound => Ok(None),
            BufferState::WithinRange => {
                if self.buffer.len() == 1 {
                    let part = self.buffer.pop_front().unwrap();
                    self.curr_len = 0;
                    Ok(Some(vec![part]))
                } else {
                    let chunk = MicroPartition::concat(std::mem::take(&mut self.buffer))?;
                    self.curr_len = 0;
                    Ok(Some(vec![chunk.into()]))
                }
            }
            BufferState::AboveUpperBound => {
                let num_ready_chunks = self.curr_len / self.upper_bound;
                let concated = MicroPartition::concat(std::mem::take(&mut self.buffer))?;
                let mut start = 0;
                let mut parts_to_return = Vec::with_capacity(num_ready_chunks);
                for _ in 0..num_ready_chunks {
                    let end = start + self.upper_bound;
                    let part = concated.slice(start, end)?;
                    parts_to_return.push(part.into());
                    start = end;
                }
                if start < concated.len() {
                    let part = concated.slice(start, concated.len())?;
                    self.curr_len = part.len();
                    self.buffer.push_back(part.into());
                } else {
                    self.curr_len = 0;
                }
                Ok(Some(parts_to_return))
            }
        }
    }

    // Pop all morsels in the buffer regardless of the threshold
    pub fn pop_all(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        assert!(self.curr_len < self.upper_bound);
        if self.buffer.is_empty() {
            Ok(None)
        } else {
            let concated = MicroPartition::concat(std::mem::take(&mut self.buffer))?;
            self.curr_len = 0;
            Ok(Some(concated.into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;
    use daft_writers::test::make_dummy_mp;

    use super::*;

    #[test]
    fn test_buffer_state_transitions() -> DaftResult<()> {
        let mut buffer = RowBasedBuffer::new(10, 20);

        assert_eq!(buffer.buffer_state(), BufferState::BelowLowerBound);

        // Add small chunk - should stay below lower bound
        buffer.push(&make_dummy_mp(5));
        assert_eq!(buffer.buffer_state(), BufferState::BelowLowerBound);
        assert!(buffer.pop_enough()?.is_none());

        // Add more to get within range
        buffer.push(&make_dummy_mp(10));
        assert_eq!(buffer.buffer_state(), BufferState::WithinRange);

        // Pop should return combined chunks
        let popped = buffer.pop_enough()?.unwrap();
        assert_eq!(popped.len(), 1);
        assert_eq!(popped[0].len(), 15);

        // Add chunks to exceed upper bound
        buffer.push(&make_dummy_mp(25));
        assert_eq!(buffer.buffer_state(), BufferState::AboveUpperBound);

        // Should return chunks of upper_bound size
        let popped = buffer.pop_enough()?.unwrap();
        assert_eq!(popped.len(), 1);
        assert_eq!(popped[0].len(), 20);

        // Remainder should be in buffer
        assert_eq!(buffer.curr_len, 5);
        assert_eq!(buffer.buffer.len(), 1);

        Ok(())
    }

    #[test]
    fn test_pop_all() -> DaftResult<()> {
        let mut buffer = RowBasedBuffer::new(10, 20);

        // Empty buffer returns None
        assert!(buffer.pop_all()?.is_none());

        // Add some chunks below upper bound
        buffer.push(&make_dummy_mp(5));
        buffer.push(&make_dummy_mp(5));

        // pop_all should return combined chunks
        let popped = buffer.pop_all()?.unwrap();
        assert_eq!(popped.len(), 10);
        assert!(buffer.buffer.is_empty());
        assert_eq!(buffer.curr_len, 0);

        Ok(())
    }
}
