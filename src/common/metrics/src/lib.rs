use std::{ops::Index, time::Duration};

use indicatif::{HumanBytes, HumanCount, HumanDuration, HumanFloatCount};
use serde::{Deserialize, Serialize};
pub use smallvec::smallvec;
use smallvec::SmallVec;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Stat {
    // Integer Representations
    Count(u64),
    Bytes(u64),
    // Float Representations
    Percent(f64),
    Float(f64),
    // Base Types
    Duration(Duration),
}

impl std::fmt::Display for Stat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count(value) => write!(f, "{}", HumanCount(*value)),
            Self::Bytes(value) => write!(f, "{}", HumanBytes(*value)),
            Self::Percent(value) => write!(f, "{:.2}%", *value),
            Self::Float(value) => write!(f, "{}", HumanFloatCount(*value)),
            Self::Duration(value) => write!(f, "{}", HumanDuration(*value)),
        }
    }
}

/// A sendable statistics snapshot of the metrics for a given node.
///
/// The general length of a snapshot is 3, because the 3 most common values are
/// 1. CPU Time in microseconds
/// 2. Rows Received
/// 3. Rows Emitted
///
/// This is intended to be lightweight for execution to generate while still
/// encoding to the same format as the receivable end.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatSnapshotSend(pub SmallVec<[(&'static str, Stat); 3]>);

impl StatSnapshotSend {
    pub fn names(&self) -> impl Iterator<Item = &'static str> + use<'_> {
        self.0.iter().map(|(name, _)| *name)
    }

    pub fn values(&self) -> impl Iterator<Item = &Stat> + use<'_> {
        self.0.iter().map(|(_, value)| value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &Stat)> + use<'_> {
        self.0.iter().map(|(name, value)| (*name, value))
    }
}

impl Index<usize> for StatSnapshotSend {
    type Output = (&'static str, Stat);
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IntoIterator for StatSnapshotSend {
    type Item = (&'static str, Stat);
    type IntoIter = smallvec::IntoIter<[(&'static str, Stat); 3]>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[macro_export(local_inner_macros)]
macro_rules! snapshot {
    ($($name:expr; $value:expr),* $(,)?) => {
        StatSnapshotSend(smallvec![
            $(($name, $value)),*
        ])
    };
}

/// A receivable statistics snapshot of the metrics for a given node.
///
/// This should match the format of the sendable snapshot, but is a different
/// type for deserialization
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatSnapshotRecv(Vec<(String, Stat)>);
