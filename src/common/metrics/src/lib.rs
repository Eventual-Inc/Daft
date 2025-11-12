pub mod operator_metrics;
pub mod ops;
#[cfg(feature = "python")]
pub mod python;

use std::{ops::Index, slice, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode)]
pub enum StatKeyType {
    Static(&'static str),
    Dynamic(Arc<str>),
}

impl StatKeyType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Static(name) => name,
            Self::Dynamic(name) => name.as_ref(),
        }
    }
}

impl From<&'static str> for StatKeyType {
    fn from(name: &'static str) -> Self {
        Self::Static(name)
    }
}

impl From<Arc<str>> for StatKeyType {
    fn from(name: Arc<str>) -> Self {
        Self::Dynamic(name)
    }
}

impl From<String> for StatKeyType {
    fn from(name: String) -> Self {
        Self::Dynamic(name.into())
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
#[derive(Debug, Clone, PartialEq, Serialize, Encode)]
pub struct StatSnapshotSend(pub SmallVec<[(StatKeyType, Stat); 3]>);

impl StatSnapshotSend {
    pub fn names(&self) -> impl Iterator<Item = &str> + '_ {
        self.0.iter().map(|(name, _)| name.as_str())
    }

    pub fn values(&self) -> impl Iterator<Item = &Stat> + '_ {
        self.0.iter().map(|(_, value)| value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Stat)> + '_ {
        self.0.iter().map(|(name, value)| (name.as_str(), value))
    }
}

impl Index<usize> for StatSnapshotSend {
    type Output = (StatKeyType, Stat);
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IntoIterator for StatSnapshotSend {
    type Item = (StatKeyType, Stat);
    type IntoIter = smallvec::IntoIter<[(StatKeyType, Stat); 3]>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[macro_export(local_inner_macros)]
macro_rules! snapshot {
    ($($name:expr; $value:expr),* $(,)?) => {
        StatSnapshotSend(smallvec![
            $( ($name.into(), $value) ),*
        ])
    };
}

/// A receivable statistics snapshot of the metrics for a given node.
///
/// This should match the format of the sendable snapshot, but is a different
/// type for deserialization
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct StatSnapshotRecv(Vec<(String, Stat)>);

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatSnapshotView(SmallVec<[(StatKeyType, Stat); 3]>);

impl StatSnapshotRecv {
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Stat)> + use<'_> {
        self.0.iter().map(|(name, value)| (name.as_str(), value))
    }
}

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
    inner: slice::Iter<'a, (StatKeyType, Stat)>,
}

impl<'a> Iterator for StatSnapshotViewIter<'a> {
    type Item = (&'a str, &'a Stat);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(name, stat)| (name.as_str(), stat))
    }
}

impl IntoIterator for StatSnapshotView {
    type Item = (StatKeyType, Stat);
    type IntoIter = smallvec::IntoIter<[(StatKeyType, Stat); 3]>;

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

// Common statistic names
pub const ROWS_IN_KEY: &str = "rows in";
pub const ROWS_OUT_KEY: &str = "rows out";
pub const CPU_US_KEY: &str = "cpu us";

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    use pyo3::types::PyModuleMethods;

    use crate::python::{PyNodeInfo, PyOperatorMetrics, StatType};

    parent.add_class::<StatType>()?;
    parent.add_class::<PyNodeInfo>()?;
    parent.add_class::<PyOperatorMetrics>()?;
    Ok(())
}
