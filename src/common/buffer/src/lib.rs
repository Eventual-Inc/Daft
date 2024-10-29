use std::{cmp::Ordering::*, collections::VecDeque, sync::Arc};

use common_error::DaftResult;

#[allow(clippy::len_without_is_empty)]
pub trait Bufferable {
    fn len(&self) -> usize;
    fn slice(&self, start: usize, end: usize) -> DaftResult<Self>
    where
        Self: Sized;
    fn concat(parts: &[&Self]) -> DaftResult<Self>
    where
        Self: Sized;
}

impl<T> Bufferable for Arc<T>
where
    T: Bufferable,
{
    fn len(&self) -> usize {
        (**self).len()
    }

    fn concat(parts: &[&Self]) -> DaftResult<Self> {
        // Deref twice: once for the reference, once for the Arc
        let inner_parts: Vec<&T> = parts.iter().map(|p| p.as_ref()).collect();
        let concated = T::concat(&inner_parts)?;
        Ok(Self::new(concated))
    }

    fn slice(&self, start: usize, end: usize) -> DaftResult<Self> {
        let sliced = (**self).slice(start, end)?;
        Ok(Self::new(sliced))
    }
}

// A buffer that accumulates morsels until a threshold is reached
pub struct RowBasedBuffer<B: Bufferable> {
    pub buffer: VecDeque<B>,
    pub curr_len: usize,
    pub threshold: usize,
}

impl<B: Bufferable> RowBasedBuffer<B> {
    pub fn new(threshold: usize) -> Self {
        assert!(threshold > 0);
        Self {
            buffer: VecDeque::new(),
            curr_len: 0,
            threshold,
        }
    }

    // Push a morsel to the buffer
    pub fn push(&mut self, part: B) {
        self.curr_len += part.len();
        self.buffer.push_back(part);
    }

    // Pop enough morsels that reach the threshold
    // - If the buffer currently has not enough morsels, return None
    // - If the buffer has exactly enough morsels, return the morsels
    // - If the buffer has more than enough morsels, return a vec of morsels, each correctly sized to the threshold.
    //   The remaining morsels will be pushed back to the buffer
    pub fn pop_enough(&mut self) -> DaftResult<Option<Vec<B>>> {
        match self.curr_len.cmp(&self.threshold) {
            Less => Ok(None),
            Equal => {
                if self.buffer.len() == 1 {
                    let part = self.buffer.pop_front().unwrap();
                    self.curr_len = 0;
                    Ok(Some(vec![part]))
                } else {
                    let chunk =
                        B::concat(&std::mem::take(&mut self.buffer).iter().collect::<Vec<_>>())?;
                    self.curr_len = 0;
                    Ok(Some(vec![chunk]))
                }
            }
            Greater => {
                let num_ready_chunks = self.curr_len / self.threshold;
                let concated =
                    B::concat(&std::mem::take(&mut self.buffer).iter().collect::<Vec<_>>())?;
                let mut start = 0;
                let mut parts_to_return = Vec::with_capacity(num_ready_chunks);
                for _ in 0..num_ready_chunks {
                    let end = start + self.threshold;
                    let part = concated.slice(start, end)?;
                    parts_to_return.push(part);
                    start = end;
                }
                if start < concated.len() {
                    let part = concated.slice(start, concated.len())?;
                    self.curr_len = part.len();
                    self.buffer.push_back(part);
                } else {
                    self.curr_len = 0;
                }
                Ok(Some(parts_to_return))
            }
        }
    }

    // Pop all morsels in the buffer regardless of the threshold
    pub fn pop_all(&mut self) -> DaftResult<Option<B>> {
        assert!(self.curr_len < self.threshold);
        if self.buffer.is_empty() {
            Ok(None)
        } else {
            let concated = B::concat(&self.buffer.iter().collect::<Vec<_>>())?;
            self.curr_len = 0;
            Ok(Some(concated))
        }
    }
}
