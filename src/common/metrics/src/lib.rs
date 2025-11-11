pub mod operator_metrics;
pub mod ops;
#[cfg(feature = "python")]
pub mod python;

use std::{ops::Index, slice, sync::Arc, time::Duration};

use indicatif::{HumanBytes, HumanCount, HumanDuration, HumanFloatCount};
pub use operator_metrics::{MetricsCollector, NoopMetricsCollector, OperatorMetrics};
#[cfg(feature = "python")]
use pyo3::types::PyModule;
#[cfg(feature = "python")]
use pyo3::{Bound, PyResult};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
pub use smallvec::smallvec;

// TODO: Make this global for all plans and executions

/// Unique identifier for a query.
pub type QueryID = Arc<str>;
/// String representation of a query plan
pub type QueryPlan = Arc<str>;
/// Unique identifier for a node in the execution plan.
pub type NodeID = usize;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
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
/// 2. Rows In
/// 3. Rows Emitted
///
/// This is intended to be lightweight for execution to generate while still
/// encoding to the same format as the receivable end.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatSnapshotSend(pub SmallVec<[(Arc<str>, Stat); 3]>);

impl StatSnapshotSend {
    pub fn names(&self) -> impl Iterator<Item = &str> + '_ {
        self.0.iter().map(|(name, _)| name.as_ref())
    }

    pub fn values(&self) -> impl Iterator<Item = &Stat> + '_ {
        self.0.iter().map(|(_, value)| value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Stat)> + '_ {
        self.0.iter().map(|(name, value)| (name.as_ref(), value))
    }
}

impl Index<usize> for StatSnapshotSend {
    type Output = (Arc<str>, Stat);
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IntoIterator for StatSnapshotSend {
    type Item = (Arc<str>, Stat);
    type IntoIter = smallvec::IntoIter<[(Arc<str>, Stat); 3]>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[macro_export(local_inner_macros)]
macro_rules! snapshot {
    ($($name:expr; $value:expr),* $(,)?) => {
        StatSnapshotSend(smallvec![
            $( (::std::sync::Arc::<str>::from($name), $value) ),*
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatSnapshotView(SmallVec<[(Arc<str>, Stat); 3]>);

impl From<StatSnapshotSend> for StatSnapshotView {
    fn from(snapshot: StatSnapshotSend) -> Self {
        Self(snapshot.0)
    }
}

impl StatSnapshotView {
    pub fn iter(&self) -> StatSnapshotViewIter<'_> {
        StatSnapshotViewIter {
            inner: self.0.iter(),
        }
    }
}

pub struct StatSnapshotViewIter<'a> {
    inner: slice::Iter<'a, (Arc<str>, Stat)>,
}

impl<'a> Iterator for StatSnapshotViewIter<'a> {
    type Item = (&'a str, &'a Stat);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(name, stat)| (name.as_ref(), stat))
    }
}

impl IntoIterator for StatSnapshotView {
    type Item = (Arc<str>, Stat);
    type IntoIter = smallvec::IntoIter<[(Arc<str>, Stat); 3]>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a StatSnapshotView {
    type Item = (&'a str, &'a Stat);
    type IntoIter = StatSnapshotViewIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    use pyo3::types::PyModuleMethods;

    use crate::python::{PyNodeInfo, PyOperatorMetrics, StatType};

    parent.add_class::<StatType>()?;
    parent.add_class::<PyNodeInfo>()?;
    parent.add_class::<PyOperatorMetrics>()?;
    Ok(())
}
