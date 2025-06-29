use std::{
    fmt::Display,
    ops::{Range, RangeBounds},
};

use google_cloud_storage::http::objects::download::Range as GRange;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidGetRange {
    #[error("Wanted range starting at {requested}, but object was only {length} bytes long")]
    StartTooLarge { requested: usize, length: usize },

    #[error("Range started at {start} and ended at {end}")]
    Inconsistent { start: usize, end: usize },

    #[error("Range {requested} is larger than system memory limit {max}")]
    TooLarge { requested: usize, max: usize },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum GetRange {
    /// Request a specific range of bytes
    Bounded(Range<usize>),
    /// Request all bytes starting from a given byte offset
    Offset(usize),
    /// Request up to the last n bytes
    Suffix(usize),
}

impl GetRange {
    pub fn is_valid(&self) -> Result<(), InvalidGetRange> {
        if let Self::Bounded(r) = self {
            if r.end <= r.start {
                return Err(InvalidGetRange::Inconsistent {
                    start: r.start,
                    end: r.end,
                });
            }
        }
        Ok(())
    }
    pub fn as_range(&self, len: usize) -> Result<Range<usize>, InvalidGetRange> {
        self.is_valid()?;
        match self {
            Self::Bounded(r) => {
                if r.start >= len {
                    Err(InvalidGetRange::StartTooLarge {
                        requested: r.start,
                        length: len,
                    })
                } else if r.end > len {
                    Ok(r.start..len)
                } else {
                    Ok(r.clone())
                }
            }
            Self::Offset(o) => {
                if *o >= len {
                    Err(InvalidGetRange::StartTooLarge {
                        requested: *o,
                        length: len,
                    })
                } else {
                    Ok(*o..len)
                }
            }
            Self::Suffix(n) => Ok(len.saturating_sub(*n)..len),
        }
    }

    pub fn as_grange(&self) -> Result<(GRange, Option<usize>), InvalidGetRange> {
        self.is_valid()?;
        match self {
            Self::Bounded(r) => Ok((
                GRange(Some(r.start as u64), Some(r.end as u64)),
                Some(r.len()),
            )),
            Self::Offset(o) => Ok((GRange(Some(*o as u64), None), None)),
            Self::Suffix(n) => Ok((GRange(None, Some(*n as u64)), Some(*n))),
        }
    }
}

impl Display for GetRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bounded(r) => write!(f, "bytes={}-{}", r.start, r.end - 1),
            Self::Offset(o) => write!(f, "bytes={o}-"),
            Self::Suffix(n) => write!(f, "bytes=-{n}"),
        }
    }
}

#[allow(clippy::range_plus_one)]
impl<T: RangeBounds<usize>> From<T> for GetRange {
    fn from(value: T) -> Self {
        use std::ops::Bound::*;
        let first = match value.start_bound() {
            Included(i) => *i,
            Excluded(i) => i + 1,
            Unbounded => 0,
        };
        match value.end_bound() {
            Included(i) => Self::Bounded(first..(i + 1)),
            Excluded(i) => Self::Bounded(first..*i),
            Unbounded => Self::Offset(first),
        }
    }
}
