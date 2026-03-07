#![feature(if_let_guard)]

mod expr_rewriter;
mod partitioning;
mod pushdowns;
#[cfg(feature = "python")]
pub mod python;
mod scan_operator;
mod scan_task;
mod sharder;
pub mod test;

use std::{fmt::Debug, hash::Hash, sync::Arc};

use common_display::{DisplayAs, DisplayLevel};
use common_error::DaftResult;
use daft_schema::schema::SchemaRef;
pub use expr_rewriter::{PredicateGroups, rewrite_predicate_for_partitioning};
pub use partitioning::{PartitionField, PartitionTransform};
pub use pushdowns::{Pushdowns, SupportsPushdownFilters};
#[cfg(feature = "python")]
pub use python::register_modules;
pub use scan_operator::{ScanOperator, ScanOperatorRef};
pub use scan_task::{SPLIT_AND_MERGE_PASS, ScanTaskLike, ScanTaskLikeRef};
use serde::{Deserialize, Serialize};
pub use sharder::{Sharder, ShardingStrategy};

/// Pre-computed estimated stats for use by the optimizer when scan tasks are lazy.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LazyTaskStats {
    pub estimated_num_tasks: usize,
    pub estimated_total_bytes: usize,
    pub estimated_total_rows: usize,
}

/// An owned iterator over an `Arc<Vec<T>>` that yields cloned items by index.
struct OwnedArcVecIter {
    tasks: Arc<Vec<ScanTaskLikeRef>>,
    index: usize,
}

impl Iterator for OwnedArcVecIter {
    type Item = DaftResult<ScanTaskLikeRef>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.tasks.len() {
            let item = self.tasks[self.index].clone();
            self.index += 1;
            Some(Ok(item))
        } else {
            None
        }
    }
}

/// Iterator type returned by `LazyTaskProducer`'s factory.
pub type ScanTaskIterator = Box<dyn Iterator<Item = DaftResult<ScanTaskLikeRef>> + Send>;

/// Factory type for producing scan task iterators.
pub type ScanTaskFactory = Arc<dyn Fn() -> DaftResult<ScanTaskIterator> + Send + Sync>;

/// A factory that produces a fresh scan task iterator each time it is called.
///
/// Each call may re-run the glob (lazy, paginated), so multiple consumers get
/// independent iteration. For the limit case, only one consumer (the executor)
/// actually iterates, and it stops early.
pub struct LazyTaskProducer {
    factory: ScanTaskFactory,
    pub estimated_stats: LazyTaskStats,
}

impl LazyTaskProducer {
    pub fn new(factory: ScanTaskFactory, estimated_stats: LazyTaskStats) -> Self {
        Self {
            factory,
            estimated_stats,
        }
    }

    /// Create a LazyTaskProducer from an already-materialized Vec of scan tasks.
    /// Used as backward-compatible default for scan operators that don't support lazy iteration.
    pub fn from_vec(tasks: Vec<ScanTaskLikeRef>) -> Self {
        let len = tasks.len();
        // Pre-compute stats from the actual tasks.
        let mut total_rows = 0usize;
        let mut total_bytes = 0usize;
        for task in &tasks {
            if let Some(num_rows) = task.num_rows() {
                total_rows += num_rows;
            } else if let Some(approx) = task.approx_num_rows(None) {
                total_rows += approx as usize;
            }
            total_bytes += task.estimate_in_memory_size_bytes(None).unwrap_or(0);
        }
        let tasks = Arc::new(tasks);
        Self {
            factory: Arc::new(move || {
                let tasks = tasks.clone();
                Ok(Box::new(OwnedArcVecIter { tasks, index: 0 }) as ScanTaskIterator)
            }),
            estimated_stats: LazyTaskStats {
                estimated_num_tasks: len,
                estimated_total_bytes: total_bytes,
                estimated_total_rows: total_rows,
            },
        }
    }

    /// Produce a fresh iterator of scan tasks.
    pub fn produce(&self) -> DaftResult<ScanTaskIterator> {
        (self.factory)()
    }

    /// Eagerly collect all scan tasks into a Vec.
    pub fn collect_tasks(&self) -> DaftResult<Vec<ScanTaskLikeRef>> {
        self.produce()?.collect()
    }
}

impl Debug for LazyTaskProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyTaskProducer")
            .field("estimated_stats", &self.estimated_stats)
            .finish_non_exhaustive()
    }
}

impl Clone for LazyTaskProducer {
    fn clone(&self) -> Self {
        Self {
            factory: self.factory.clone(),
            estimated_stats: self.estimated_stats.clone(),
        }
    }
}

impl PartialEq for LazyTaskProducer {
    fn eq(&self, other: &Self) -> bool {
        // Two LazyTaskProducers are considered equal if their estimated stats match.
        // This mirrors how ScanOperatorRef uses pointer equality for "sameness" checking
        // in the optimizer, but for lazy producers we can't compare closures, so we use stats.
        self.estimated_stats == other.estimated_stats
    }
}

impl Eq for LazyTaskProducer {}

impl Hash for LazyTaskProducer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.estimated_stats.hash(state);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScanState {
    Tasks(Arc<Vec<ScanTaskLikeRef>>),
    #[serde(
        serialize_with = "serialize_invalid",
        deserialize_with = "deserialize_invalid"
    )]
    Operator(ScanOperatorRef),
    #[serde(
        serialize_with = "serialize_lazy_invalid",
        deserialize_with = "deserialize_lazy_invalid"
    )]
    LazyTasks(Arc<LazyTaskProducer>),
}

fn serialize_invalid<S>(_: &ScanOperatorRef, _: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    Err(serde::ser::Error::custom(
        "ScanOperatorRef cannot be serialized",
    ))
}

fn deserialize_invalid<'de, D>(_: D) -> Result<ScanOperatorRef, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Err(serde::de::Error::custom(
        "ScanOperatorRef cannot be deserialized",
    ))
}

fn serialize_lazy_invalid<S>(_: &Arc<LazyTaskProducer>, _: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    Err(serde::ser::Error::custom(
        "LazyTaskProducer cannot be serialized",
    ))
}

fn deserialize_lazy_invalid<'de, D>(_: D) -> Result<Arc<LazyTaskProducer>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Err(serde::de::Error::custom(
        "LazyTaskProducer cannot be deserialized",
    ))
}

impl ScanState {
    pub fn multiline_display(&self) -> Vec<String> {
        match self {
            Self::Operator(scan_op) => scan_op.0.multiline_display(),
            Self::Tasks(scan_tasks) => {
                let mut result = vec![format!("Num Scan Tasks = {}", scan_tasks.len())];

                if scan_tasks.is_empty() {
                    return result;
                }

                // Display scan tasks similar to ScanTaskSource
                // Show pushdowns and schema from the first scan task
                let first_task = scan_tasks.first().unwrap();
                let pushdowns = first_task.pushdowns();
                if !pushdowns.is_empty() {
                    result.push(pushdowns.display_as(DisplayLevel::Compact));
                }

                let schema = first_task.schema();
                result.push(format!(
                    "Schema: {{{}}}",
                    schema.display_as(DisplayLevel::Compact)
                ));

                // List scan tasks using compact display (which includes sources/function names)
                result.push("Scan Tasks: [".to_string());
                for (i, scan_task) in scan_tasks.iter().enumerate() {
                    if i < 3 || i >= scan_tasks.len() - 3 {
                        result.push(scan_task.display_as(DisplayLevel::Compact));
                    } else if i == 3 {
                        result.push("...".to_string());
                    }
                }
                result.push("]".to_string());

                result
            }
            Self::LazyTasks(producer) => {
                let stats = &producer.estimated_stats;
                vec![format!(
                    "~{} Scan Tasks (estimated, lazily produced)",
                    stats.estimated_num_tasks
                )]
            }
        }
    }

    pub fn get_scan_op(&self) -> &ScanOperatorRef {
        match self {
            Self::Operator(scan_op) => scan_op,
            Self::Tasks(_) | Self::LazyTasks(_) => {
                panic!("Tried to get scan op from materialized physical scan info")
            }
        }
    }

    pub fn file_format_config(&self) -> Option<Arc<common_file_formats::FileFormatConfig>> {
        match self {
            Self::Operator(_) => None,
            Self::Tasks(scan_tasks) => scan_tasks.first().map(|t| t.file_format_config()),
            Self::LazyTasks(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PhysicalScanInfo {
    pub scan_state: ScanState,
    pub source_schema: SchemaRef,
    pub partitioning_keys: Vec<PartitionField>,
    pub pushdowns: Pushdowns,
}

impl PhysicalScanInfo {
    #[must_use]
    pub fn new(
        scan_op: ScanOperatorRef,
        source_schema: SchemaRef,
        partitioning_keys: Vec<PartitionField>,
        pushdowns: Pushdowns,
    ) -> Self {
        Self {
            scan_state: ScanState::Operator(scan_op),
            source_schema,
            partitioning_keys,
            pushdowns,
        }
    }

    #[must_use]
    pub fn with_pushdowns(&self, pushdowns: Pushdowns) -> Self {
        Self {
            scan_state: self.scan_state.clone(),
            source_schema: self.source_schema.clone(),
            partitioning_keys: self.partitioning_keys.clone(),
            pushdowns,
        }
    }

    #[must_use]
    pub fn with_scan_state(&self, scan_state: ScanState) -> Self {
        Self {
            scan_state,
            source_schema: self.source_schema.clone(),
            partitioning_keys: self.partitioning_keys.clone(),
            pushdowns: self.pushdowns.clone(),
        }
    }
}
