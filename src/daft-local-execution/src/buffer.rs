use std::{collections::VecDeque, num::NonZeroUsize, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::pipeline::MorselSizeRequirement;

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
    upper_bound: NonZeroUsize,
}

impl RowBasedBuffer {
    pub fn new(lower_bound: usize, upper_bound: NonZeroUsize) -> Self {
        assert!(
            lower_bound <= upper_bound.get(),
            "lower_bound ({}) must be <= upper_bound ({}) for a RowBasedBuffer",
            lower_bound,
            upper_bound.get()
        );
        Self {
            buffer: VecDeque::new(),
            curr_len: 0,
            lower_bound,
            upper_bound,
        }
    }
    pub fn update_bounds(&mut self, morsel_size_requirement: MorselSizeRequirement) {
        let (lower_bound, upper_bound) = morsel_size_requirement.values();
        assert!(
            lower_bound <= upper_bound.get(),
            "lower_bound ({}) must be <= upper_bound ({}) for a RowBasedBuffer",
            lower_bound,
            upper_bound.get()
        );
        self.lower_bound = lower_bound;
        self.upper_bound = upper_bound;
    }

    // Push a morsel to the buffer
    pub fn push(&mut self, part: Arc<MicroPartition>) {
        self.curr_len += part.len();
        self.buffer.push_back(part);
    }

    // Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn buffer_state(&self) -> BufferState {
        match (
            self.lower_bound <= self.curr_len,
            self.curr_len <= self.upper_bound.get(),
        ) {
            (true, true) => BufferState::WithinRange,
            (true, false) => BufferState::AboveUpperBound,
            (false, true) => BufferState::BelowLowerBound,
            (false, false) => unreachable!(),
        }
    }

    // Pop all morsels in the buffer regardless of the threshold
    pub fn pop_all(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.buffer.is_empty() {
            Ok(None)
        } else {
            let concated = MicroPartition::concat(std::mem::take(&mut self.buffer))?;
            self.curr_len = 0;
            Ok(Some(concated.into()))
        }
    }
    pub fn next_batch_if_ready(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.buffer.is_empty() {
            Ok(None)
        } else {
            match self.buffer_state() {
                BufferState::BelowLowerBound => Ok(None),
                BufferState::WithinRange => {
                    // Return all data as one batch
                    if self.buffer.len() == 1 {
                        let part = self.buffer.pop_front().unwrap();
                        self.curr_len = 0;
                        Ok(Some(part))
                    } else {
                        let chunk = MicroPartition::concat(std::mem::take(&mut self.buffer))?;
                        self.curr_len = 0;
                        Ok(Some(chunk.into()))
                    }
                }
                BufferState::AboveUpperBound => {
                    // Return one batch of target size, keep rest
                    let concated = MicroPartition::concat(std::mem::take(&mut self.buffer))?;

                    let batch = concated.slice(0, self.upper_bound.get())?;

                    // Put remainder back if any
                    if self.upper_bound.get() < concated.len() {
                        let remainder = concated.slice(self.upper_bound.get(), concated.len())?;
                        self.curr_len = remainder.len();
                        self.buffer.push_back(remainder.into());
                    } else {
                        self.curr_len = 0;
                    }

                    Ok(Some(batch.into()))
                }
            }
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
        let mut buffer = RowBasedBuffer::new(10, NonZeroUsize::new(20).unwrap());

        assert_eq!(buffer.buffer_state(), BufferState::BelowLowerBound);

        // Add small chunk - should stay below lower bound
        buffer.push(make_dummy_mp(5).into());
        assert_eq!(buffer.buffer_state(), BufferState::BelowLowerBound);
        assert!(buffer.next_batch_if_ready()?.is_none());

        // Add more to get within range
        buffer.push(make_dummy_mp(10).into());
        assert_eq!(buffer.buffer_state(), BufferState::WithinRange);

        // Should return combined chunks as one batch
        let popped = buffer.next_batch_if_ready()?.unwrap();
        assert_eq!(popped.len(), 15);
        assert_eq!(buffer.buffer_state(), BufferState::BelowLowerBound);

        // Add chunks to exceed upper bound
        buffer.push(make_dummy_mp(25).into());
        assert_eq!(buffer.buffer_state(), BufferState::AboveUpperBound);

        // Should return one batch of upper_bound size
        let popped = buffer.next_batch_if_ready()?.unwrap();
        assert_eq!(popped.len(), 20);

        // Remainder should be in buffer
        assert_eq!(buffer.curr_len, 5);
        assert_eq!(buffer.buffer.len(), 1);

        Ok(())
    }

    #[test]
    fn test_pop_all() -> DaftResult<()> {
        let mut buffer = RowBasedBuffer::new(10, NonZeroUsize::new(20).unwrap());

        // Empty buffer returns None
        assert!(buffer.pop_all()?.is_none());

        // Add some chunks below upper bound
        buffer.push(make_dummy_mp(5).into());
        buffer.push(make_dummy_mp(5).into());

        // pop_all should return combined chunks
        let popped = buffer.pop_all()?.unwrap();
        assert_eq!(popped.len(), 10);
        assert!(buffer.buffer.is_empty());
        assert_eq!(buffer.curr_len, 0);

        Ok(())
    }

    #[test]
    fn test_single_empty_partition() -> DaftResult<()> {
        let mut buffer = RowBasedBuffer::new(0, NonZeroUsize::new(1).unwrap());
        buffer.push(Arc::new(MicroPartition::empty(None)));
        assert!(buffer.next_batch_if_ready()?.is_some());
        assert!(buffer.next_batch_if_ready()?.is_none());
        assert!(buffer.pop_all()?.is_none());
        Ok(())
    }

    #[test]
    fn test_multiple_empty_partitions() -> DaftResult<()> {
        let mut buffer = RowBasedBuffer::new(0, NonZeroUsize::new(1).unwrap());
        buffer.push(Arc::new(MicroPartition::empty(None)));
        buffer.push(Arc::new(MicroPartition::empty(None)));
        assert!(buffer.next_batch_if_ready()?.is_some());
        assert!(buffer.next_batch_if_ready()?.is_none());
        assert!(buffer.pop_all()?.is_none());
        Ok(())
    }

    #[test]
    fn test_multiple_empty_partitions_pop_all() -> DaftResult<()> {
        let mut buffer = RowBasedBuffer::new(0, NonZeroUsize::new(1).unwrap());
        buffer.push(Arc::new(MicroPartition::empty(None)));
        buffer.push(Arc::new(MicroPartition::empty(None)));
        assert!(buffer.pop_all()?.is_some());
        assert!(buffer.pop_all()?.is_none());
        Ok(())
    }
}
