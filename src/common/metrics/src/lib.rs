pub mod meters;
pub mod operator_metrics;
pub mod ops;
#[cfg(feature = "python")]
pub mod python;
pub mod snapshot;
pub mod spill;

use std::{ops::Index, sync::Arc, time::Duration};

use indicatif::{HumanBytes, HumanCount, HumanDuration, HumanFloatCount};
pub use meters::{Counter, Gauge, Meter, UpDownCounter, normalize_name};
pub use operator_metrics::{
    MetricsCollector, NoopMetricsCollector, OperatorCounter, OperatorMetrics,
};
#[cfg(feature = "python")]
use pyo3::types::PyModule;
#[cfg(feature = "python")]
use pyo3::{Bound, PyResult, pyclass};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
pub use snapshot::StatSnapshot;
pub use spill::SpillReporter;

/// Unique identifier for a query.
// TODO: Make this global for all plans and executions
pub type QueryID = Arc<str>;
/// String representation of a query plan
pub type QueryPlan = Arc<str>;
/// Unique identifier for a node in the execution plan.
pub type NodeID = usize;

#[cfg_attr(feature = "python", pyclass(module = "daft.daft", eq, from_py_object))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryEndState {
    Finished,
    Canceled,
    Failed,
}

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

impl Stat {
    pub fn into_f64_and_unit(self) -> (f64, Option<&'static str>) {
        match self {
            Self::Count(value) => (value as f64, None),
            Self::Bytes(value) => (value as f64, Some("bytes")),
            Self::Percent(value) => (value, Some("%")),
            Self::Float(value) => (value, None),
            Self::Duration(value) => (value.as_micros() as f64, Some("µs")),
        }
    }
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stats(pub SmallVec<[(Arc<str>, Stat); 7]>);

impl Stats {
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

impl Index<usize> for Stats {
    type Output = (Arc<str>, Stat);
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IntoIterator for Stats {
    type Item = (Arc<str>, Stat);
    type IntoIter = smallvec::IntoIter<[(Arc<str>, Stat); 7]>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

// Common statistic names
pub const BYTES_READ_KEY: &str = "bytes.read";
pub const BYTES_WRITTEN_KEY: &str = "bytes.written";
pub const BYTES_IN_KEY: &str = "bytes.in";
pub const BYTES_OUT_KEY: &str = "bytes.out";
pub const DURATION_KEY: &str = "duration";
pub const ROWS_IN_KEY: &str = "rows.in";
pub const ROWS_OUT_KEY: &str = "rows.out";
pub const ROWS_WRITTEN_KEY: &str = "rows.written";

// In-memory buffer size for blocking sinks (pre-spill high-water gauge)
pub const IN_MEMORY_BUFFER_BYTES_KEY: &str = "bytes.in_memory_buffer";

// Spill metrics
pub const SPILL_SOURCE_KEY: &str = "spill.source";
pub const SPILL_BYTES_WRITTEN_STAT_KEY: &str = "spill.bytes.written";
pub const SPILL_BYTES_READ_STAT_KEY: &str = "spill.bytes.read";
pub const SPILL_WRITE_DURATION_NS_STAT_KEY: &str = "spill.write.duration_ns";
pub const SPILL_READ_DURATION_NS_STAT_KEY: &str = "spill.read.duration_ns";
pub const SPILL_FILE_COUNT_STAT_KEY: &str = "spill.files.created";
pub const SPILL_FILES_RESIDENT_STAT_KEY: &str = "spill.files.resident";

// Join metrics
pub const JOIN_BUILD_ROWS_INSERTED_KEY: &str = "rows.join.build_inserted";
pub const JOIN_BUILD_BYTES_INSERTED_KEY: &str = "bytes.join.build_inserted";
pub const JOIN_PROBE_ROWS_IN_KEY: &str = "rows.join.probe_in";
pub const JOIN_PROBE_ROWS_OUT_KEY: &str = "rows.join.probe_out";
pub const JOIN_PROBE_BYTES_IN_KEY: &str = "bytes.join.probe_in";
pub const JOIN_PROBE_BYTES_OUT_KEY: &str = "bytes.join.probe_out";

// Task metrics
pub const TASK_ACTIVE_KEY: &str = "task.active";
pub const TASK_COMPLETED_KEY: &str = "task.completed";
pub const TASK_FAILED_KEY: &str = "task.failed";
pub const TASK_CANCELLED_KEY: &str = "task.cancelled";

// Execution attributes
pub const ATTR_EXECUTION_RUNNER: &str = "execution.runner";

// Query attributes
pub const ATTR_QUERY_ID: &str = "query.id";

// Node attributes
pub const ATTR_NODE_ORIGIN_ID: &str = "node.origin_id";
pub const ATTR_NODE_ID: &str = "node.id";
pub const ATTR_NODE_TYPE: &str = "node.type";
pub const ATTR_NODE_PHASE: &str = "node.phase";

// Process-level metrics
pub const PROCESS_JEMALLOC_ALLOCATED_KEY: &str = "process.memory.jemalloc.allocated";
pub const PROCESS_JEMALLOC_RESIDENT_KEY: &str = "process.memory.jemalloc.resident";
pub const PROCESS_RSS_KEY: &str = "process.memory.rss";
pub const PROCESS_CPU_PERCENT_KEY: &str = "process.cpu.percent";

// Units (UCUM)
pub const UNIT_ROWS: &str = "{row}";
pub const UNIT_BYTES: &str = "By";
pub const UNIT_MICROSECONDS: &str = "us";
pub const UNIT_TASKS: &str = "{task}";
pub const UNIT_PERCENT: &str = "%";

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    use pyo3::types::PyModuleMethods;

    use crate::python::{PyOperatorMetrics, StatType};

    parent.add_class::<StatType>()?;
    parent.add_class::<PyOperatorMetrics>()?;
    parent.add_class::<QueryEndState>()?;
    Ok(())
}
