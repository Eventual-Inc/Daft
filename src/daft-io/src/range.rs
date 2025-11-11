// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// # Borrowed and modified from [`arrow-rs-object-store`](https://github.com/apache/arrow-rs-object-store/blob/v0.12.2/src/util.rs#L193).

use std::{
    fmt::Display,
    ops::{Range, RangeBounds},
};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidGetRange {
    #[error("Wanted range starting at {requested}, but object was only {length} bytes long")]
    StartTooLarge { requested: usize, length: usize },

    #[error("Range end '{end}' is always expected to be greater than start '{start}'")]
    Inconsistent { start: usize, end: usize },

    #[error("Suffix range is not supported")]
    UnsupportedSuffixRange,
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
    pub fn validate(&self) -> Result<(), InvalidGetRange> {
        if let Self::Bounded(r) = self
            && r.end <= r.start
        {
            return Err(InvalidGetRange::Inconsistent {
                start: r.start,
                end: r.end,
            });
        }
        Ok(())
    }

    pub fn as_range(&self, len: usize) -> Result<Range<usize>, InvalidGetRange> {
        self.validate()?;
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
    use std::ops::Bound::{Excluded, Included, Unbounded};

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
        assert_eq!(Into::<GetRange>::into(..15), GetRange::Bounded(0..15));
        assert_eq!(Into::<GetRange>::into(..=15), GetRange::Bounded(0..16));
        assert_eq!(Into::<GetRange>::into(..), GetRange::Offset(0));
        assert_eq!(
            Into::<GetRange>::into((Excluded(10), Included(15))),
            GetRange::Bounded(11..16)
        );
        assert_eq!(
            Into::<GetRange>::into((Excluded(10), Excluded(15))),
            GetRange::Bounded(11..15)
        );
        assert_eq!(
            Into::<GetRange>::into((Excluded(10), Unbounded)),
            GetRange::Offset(11)
        );
    }

    #[test]
    fn test_as_range() {
        let range = GetRange::Bounded(2..5);
        assert_eq!(range.as_range(5).unwrap(), 2..5);

        let range = range.as_range(4).unwrap();
        assert_eq!(range, 2..4);

        let range = GetRange::Bounded(3..3);
        let err = range.as_range(2).unwrap_err().to_string();
        assert_eq!(
            err,
            "Range end '3' is always expected to be greater than start '3'"
        );

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
}
