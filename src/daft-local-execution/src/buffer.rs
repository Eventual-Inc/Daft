use std::{
    cmp::Ordering::*,
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
use futures::{Stream, StreamExt};

/// A buffer that accumulates morsels until a threshold is reached and yields individual MicroPartitions
struct RowBasedBuffer {
    buffer: VecDeque<Arc<MicroPartition>>,
    curr_len: usize,
    threshold: usize,
}

impl RowBasedBuffer {
    fn new(threshold: usize) -> Self {
        assert!(threshold > 0);
        Self {
            buffer: VecDeque::new(),
            curr_len: 0,
            threshold,
        }
    }

    fn push(&mut self, part: Arc<MicroPartition>) {
        self.curr_len += part.len();
        self.buffer.push_back(part);
    }

    fn pop_next(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        match self.curr_len.cmp(&self.threshold) {
            Less => Ok(None),
            Equal => {
                self.curr_len = 0;
                if self.buffer.len() == 1 {
                    let part = self.buffer.pop_front().unwrap();
                    Ok(Some(part))
                } else {
                    let chunk = MicroPartition::concat(std::mem::take(&mut self.buffer))?;
                    Ok(Some(chunk.into()))
                }
            }
            Greater => {
                let concated = MicroPartition::concat(std::mem::take(&mut self.buffer))?;
                let (part, remaining) = concated.split_at(self.threshold)?;
                self.curr_len = remaining.len();
                self.buffer.push_back(remaining.into());
                Ok(Some(part.into()))
            }
        }
    }

    fn pop_all(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.buffer.is_empty() {
            Ok(None)
        } else {
            let concated = MicroPartition::concat(std::mem::take(&mut self.buffer))?;
            self.curr_len = 0;
            Ok(Some(concated.into()))
        }
    }
}

/// Creates a buffered stream that accumulates morsels until they reach the specified threshold size.
///
/// # Arguments
///
/// * `stream` - A stream of MicroPartitions to buffer
/// * `threshold` - The target size in rows for each buffered chunk
///
/// # Returns
///
/// A stream that yields individual MicroPartitions, each containing approximately `threshold` rows
/// #
pub fn buffered_by_morsel_size<S>(
    stream: S,
    threshold: Option<usize>,
) -> impl Stream<Item = DaftResult<Arc<MicroPartition>>>
where
    S: Stream<Item = Arc<MicroPartition>> + Unpin,
{
    struct BufferedStream<S> {
        stream: S,
        buffer: Option<RowBasedBuffer>,
        finished: bool,
    }

    impl<S> Stream for BufferedStream<S>
    where
        S: Stream<Item = Arc<MicroPartition>> + Unpin,
    {
        type Item = DaftResult<Arc<MicroPartition>>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.finished {
                return Poll::Ready(None);
            }
            // If there is no buffer, just pass through the stream directly
            if self.buffer.is_none() {
                match self.stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(part)) => return Poll::Ready(Some(Ok(part))),
                    Poll::Ready(None) => {
                        self.finished = true;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                // First try to get a buffered chunk
                match self.buffer.as_mut().unwrap().pop_next() {
                    Ok(Some(part)) => return Poll::Ready(Some(Ok(part))),
                    Err(e) => return Poll::Ready(Some(Err(e))),
                    Ok(None) => (),
                }

                // If no buffered chunk is ready, try to get more items from the input stream
                loop {
                    match self.stream.poll_next_unpin(cx) {
                        Poll::Ready(Some(part)) => {
                            let buffer = self.buffer.as_mut().unwrap();
                            buffer.push(part);
                            // Check if we have enough data now
                            match buffer.pop_next() {
                                Ok(Some(part)) => return Poll::Ready(Some(Ok(part))),
                                Ok(None) => continue, // Continue polling the stream for more data
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            }
                        }
                        Poll::Ready(None) => {
                            // Stream is exhausted, and pop_next already returned None above
                            // Safe to call pop_all now
                            let buffer = self.buffer.as_mut().unwrap();
                            match buffer.pop_all() {
                                Ok(Some(part)) => return Poll::Ready(Some(Ok(part))),
                                Ok(None) => {
                                    self.finished = true;
                                    return Poll::Ready(None);
                                }
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            }
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }

    BufferedStream {
        stream,
        buffer: threshold.map(RowBasedBuffer::new),
        finished: false,
    }
}
