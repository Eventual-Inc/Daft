pub mod agg;
#[allow(unused)]
mod plan;
#[cfg(feature = "python")]
pub mod python;
mod results;
mod translate;
use std::{collections::HashMap, sync::Arc};

use common_scan_info::ScanTaskLikeRef;
use daft_micropartition::MicroPartitionRef;
#[cfg(feature = "python")]
pub use plan::{CatalogWrite, DataSink, DistributedActorPoolProject, LanceWrite};
pub use plan::{
    CommitWrite, Concat, CrossJoin, Dedup, EmptyScan, Explode, Filter, GlobScan, HashAggregate,
    HashJoin, InMemoryScan, IntoBatches, IntoPartitions, Limit, LocalNodeContext,
    LocalPhysicalPlan, LocalPhysicalPlanRef, MonotonicallyIncreasingId, PhysicalScan,
    PhysicalWrite, Pivot, Project, Repartition, Sample, SamplingMethod, Sort, SortMergeJoin, TopN,
    UDFProject, UnGroupedAggregate, Unpivot, VLLMProject, WindowOrderByOnly,
    WindowPartitionAndDynamicFrame, WindowPartitionAndOrderBy, WindowPartitionOnly,
};
#[cfg(feature = "python")]
pub use python::{PyLocalPhysicalPlan, register_modules};
pub use results::ExecutionEngineFinalResult;
pub use translate::translate;
use serde::{Deserialize, Serialize};

pub type InputId = u32;
pub type SourceId = u32;

#[derive(Default)]
pub(crate) struct SourceIdCounter {
    counter: SourceId,
}

impl SourceIdCounter {
    pub fn next(&mut self) -> SourceId {
        self.counter += 1;
        self.counter
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnresolvedInput {
    ScanTask(Arc<Vec<ScanTaskLikeRef>>),
    InMemory { cache_key: String },
    GlobPaths(Arc<Vec<String>>),
}

#[derive(Debug, Clone)]
pub enum ResolvedInput {
    ScanTasks(Vec<ScanTaskLikeRef>),
    InMemoryPartitions(Vec<MicroPartitionRef>),
    GlobPaths(Vec<String>),
}

/// Result of translating a logical plan, containing both the physical plan and unresolved inputs
#[derive(Debug, Clone)]
pub struct TranslationResult {
    pub plan: LocalPhysicalPlanRef,
    pub unresolved_inputs: HashMap<String, UnresolvedInput>,
}
