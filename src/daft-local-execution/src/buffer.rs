use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::{Stream, StreamExt};

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

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    // Pop a single MicroPartition from the buffer
    // - If the buffer is below the lower bound, return None
    // - If the buffer is within range, return all buffered data as a single partition
    // - If the buffer is above the upper bound, return a partition of upper_bound size and keep remainder
    pub fn pop(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        match self.buffer_state() {
            BufferState::BelowLowerBound => Ok(None),
            BufferState::WithinRange => self.pop_all(),
            BufferState::AboveUpperBound => {
                let concated = MicroPartition::concat(std::mem::take(&mut self.buffer))?;
                let part_to_return = concated.slice(0, self.upper_bound)?;

                if concated.len() > self.upper_bound {
                    let remainder = concated.slice(self.upper_bound, concated.len())?;
                    self.curr_len = remainder.len();
                    self.buffer.push_back(remainder.into());
                } else {
                    self.curr_len = 0;
                }

                Ok(Some(part_to_return.into()))
            }
        }
    }

    // Pop all morsels in the buffer regardless of the threshold
    pub fn pop_all(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.buffer.is_empty() {
            Ok(None)
        } else if self.buffer.len() == 1 {
            let part = self.buffer.pop_front().unwrap();
            self.curr_len = 0;
            Ok(Some(part))
        } else {
            let chunk = MicroPartition::concat(std::mem::take(&mut self.buffer))?;
            self.curr_len = 0;
            Ok(Some(chunk.into()))
        }
    }
}

/// A streaming buffer that buffers MicroPartitions by morsel size range
pub struct MorselRangeBuffer<S> {
    input_stream: S,
    buffer: RowBasedBuffer,
    input_exhausted: bool,
}

impl<S> MorselRangeBuffer<S>
where
    S: Stream<Item = Arc<MicroPartition>> + Unpin,
{
    pub fn new(input_stream: S, lower_bound: usize, upper_bound: usize) -> Self {
        Self {
            input_stream: input_stream,
            buffer: RowBasedBuffer::new(lower_bound, upper_bound),
            input_exhausted: false,
        }
    }
}

impl<S> Stream for MorselRangeBuffer<S>
where
    S: Stream<Item = Arc<MicroPartition>> + Unpin,
{
    type Item = DaftResult<Arc<MicroPartition>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // First, try to pop from buffer if we have enough data
            if let Ok(Some(output)) = self.buffer.pop() {
                return Poll::Ready(Some(Ok(output)));
            }

            // If we've exhausted input and buffer is empty or can't produce more, we're done
            if self.input_exhausted {
                if self.buffer.is_empty() {
                    return Poll::Ready(None);
                } else {
                    // Force flush remaining data using pop_all
                    if let Ok(Some(remaining)) = self.buffer.pop_all() {
                        return Poll::Ready(Some(Ok(remaining)));
                    } else {
                        return Poll::Ready(None);
                    }
                }
            }

            // Try to get more input
            match self.input_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(input_partition)) => {
                    self.buffer.push(&input_partition);
                    // Continue the loop to try popping from buffer
                }
                Poll::Ready(None) => {
                    self.input_exhausted = true;
                    // Continue the loop to handle final flush
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

/// Buffer a stream of MicroPartitions by morsel size range
///
/// This function takes a stream of MicroPartitions and buffers them according to the specified
/// range, outputting appropriately sized partitions.
///
/// # Arguments
/// * `input_stream` - The input stream of MicroPartitions to buffer
/// * `lower_bound` - The minimum number of rows before outputting a partition
/// * `upper_bound` - The maximum number of rows in an output partition
///
/// # Returns
/// A stream of buffered MicroPartitions that respect the size constraints
pub fn buffer_by_morsel_range<S>(
    input_stream: S,
    lower_bound: usize,
    upper_bound: usize,
) -> MorselRangeBuffer<S>
where
    S: Stream<Item = Arc<MicroPartition>> + Send + Unpin,
{
    MorselRangeBuffer::new(input_stream, lower_bound, upper_bound)
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
        assert!(buffer.pop()?.is_none());

        // Add more to get within range
        buffer.push(&make_dummy_mp(10));
        assert_eq!(buffer.buffer_state(), BufferState::WithinRange);

        // Pop should return combined chunks
        let popped = buffer.pop()?.unwrap();
        assert_eq!(popped.len(), 15);

        // Add chunks to exceed upper bound
        buffer.push(&make_dummy_mp(25));
        assert_eq!(buffer.buffer_state(), BufferState::AboveUpperBound);

        // Should return chunk of upper_bound size
        let popped = buffer.pop()?.unwrap();
        assert_eq!(popped.len(), 20);

        // Remainder should be in buffer
        assert_eq!(buffer.curr_len, 5);
        assert_eq!(buffer.buffer.len(), 1);

        Ok(())
    }

    #[test]
    fn test_pop_with_single_partition() -> DaftResult<()> {
        let mut buffer = RowBasedBuffer::new(10, 20);

        // Empty buffer returns None
        assert!(buffer.pop()?.is_none());

        // Add single partition within range
        buffer.push(&make_dummy_mp(15));

        // pop should return the single partition directly
        let popped = buffer.pop()?.unwrap();
        assert_eq!(popped.len(), 15);
        assert!(buffer.buffer.is_empty());
        assert_eq!(buffer.curr_len, 0);

        Ok(())
    }

    #[test]
    fn test_pop_multiple_above_upper_bound() -> DaftResult<()> {
        let mut buffer = RowBasedBuffer::new(10, 20);

        // Add data that exceeds upper bound significantly
        buffer.push(&make_dummy_mp(45));
        assert_eq!(buffer.buffer_state(), BufferState::AboveUpperBound);

        // First pop should return upper_bound size
        let popped1 = buffer.pop()?.unwrap();
        assert_eq!(popped1.len(), 20);
        assert_eq!(buffer.curr_len, 25);

        // Second pop should return another upper_bound size
        let popped2 = buffer.pop()?.unwrap();
        assert_eq!(popped2.len(), 20);
        assert_eq!(buffer.curr_len, 5);

        // Third pop should return None (below lower bound)
        assert!(buffer.pop()?.is_none());

        Ok(())
    }

    #[test]
    fn test_pop_all() -> DaftResult<()> {
        let mut buffer = RowBasedBuffer::new(10, 20);

        // Empty buffer returns None
        assert!(buffer.pop_all()?.is_none());

        // Add some chunks below lower bound
        buffer.push(&make_dummy_mp(5));
        buffer.push(&make_dummy_mp(3));

        // pop_all should return combined chunks regardless of lower bound
        let popped = buffer.pop_all()?.unwrap();
        assert_eq!(popped.len(), 8);
        assert!(buffer.buffer.is_empty());
        assert_eq!(buffer.curr_len, 0);

        // Test with single partition
        buffer.push(&make_dummy_mp(7));
        let popped = buffer.pop_all()?.unwrap();
        assert_eq!(popped.len(), 7);
        assert!(buffer.buffer.is_empty());
        assert_eq!(buffer.curr_len, 0);

        Ok(())
    }
}
