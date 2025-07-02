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

/// Inspired from the GetRange of arrow object store.
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

#[cfg(test)]
mod tests {
    use crate::range::GetRange;

    #[test]
    fn get_range_str() {
        assert_eq!(GetRange::Offset(0).to_string(), "bytes=0-");
        assert_eq!(GetRange::Bounded(10..19).to_string(), "bytes=10-18");
        assert_eq!(GetRange::Suffix(10).to_string(), "bytes=-10");
    }

    #[test]
    fn get_range_from() {
        assert_eq!(Into::<GetRange>::into(10..15), GetRange::Bounded(10..15),);
        assert_eq!(Into::<GetRange>::into(10..=15), GetRange::Bounded(10..16),);
        assert_eq!(Into::<GetRange>::into(10..), GetRange::Offset(10),);
        assert_eq!(Into::<GetRange>::into(..=15), GetRange::Bounded(0..16));
    }

    #[test]
    fn test_as_range() {
        let range = GetRange::Bounded(2..5);
        assert_eq!(range.as_range(5).unwrap(), 2..5);

        let range = range.as_range(4).unwrap();
        assert_eq!(range, 2..4);

        let range = GetRange::Bounded(3..3);
        let err = range.as_range(2).unwrap_err().to_string();
        assert_eq!(err, "Range started at 3 and ended at 3");

        let range = GetRange::Bounded(2..2);
        let err = range.as_range(3).unwrap_err().to_string();
        assert_eq!(err, "Range started at 2 and ended at 2");

        let range = GetRange::Suffix(3);
        assert_eq!(range.as_range(3).unwrap(), 0..3);
        assert_eq!(range.as_range(2).unwrap(), 0..2);

        let range = GetRange::Suffix(0);
        assert_eq!(range.as_range(0).unwrap(), 0..0);

        let range = GetRange::Offset(2);
        let err = range.as_range(2).unwrap_err().to_string();
        assert_eq!(
            err,
            "Wanted range starting at 2, but object was only 2 bytes long"
        );

        let err = range.as_range(1).unwrap_err().to_string();
        assert_eq!(
            err,
            "Wanted range starting at 2, but object was only 1 bytes long"
        );

        let range = GetRange::Offset(1);
        assert_eq!(range.as_range(2).unwrap(), 1..2);
    }

    #[test]
    fn test_as_grange() {
        use google_cloud_storage::http::objects::download::Range as GRange;
        fn assert_as_grange(
            actual: (GRange, Option<usize>),
            expected_range: GRange,
            expected_len: Option<usize>,
        ) {
            let (range, len) = actual;
            assert_eq!(range.0, expected_range.0);
            assert_eq!(range.1, expected_range.1);
            assert_eq!(len, expected_len)
        }

        let range = GetRange::Bounded(2..5);
        assert_as_grange(
            range.as_grange().unwrap(),
            GRange(Some(2), Some(5)),
            Some(3),
        );

        let range = GetRange::Bounded(3..3);
        assert!(range.as_grange().is_err());

        let range = GetRange::Suffix(3);
        assert_as_grange(range.as_grange().unwrap(), GRange(None, Some(3)), Some(3));

        let range = GetRange::Suffix(0);
        assert_as_grange(range.as_grange().unwrap(), GRange(None, Some(0)), Some(0));

        let range = GetRange::Offset(2);
        assert_as_grange(range.as_grange().unwrap(), GRange(Some(2), None), None);
    }
}
