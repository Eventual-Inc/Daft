pub mod agg;
#[allow(unused)]
mod plan;
#[cfg(feature = "python")]
pub mod python;
mod results;
mod translate;
use common_scan_info::ScanTaskLikeRef;
use daft_micropartition::MicroPartitionRef;
#[cfg(feature = "python")]
pub use plan::{CatalogWrite, DataSink, DistributedActorPoolProject, LanceWrite};
pub use plan::{
    CommitWrite, Concat, CrossJoin, Dedup, Explode, Filter, GlobScan, HashAggregate, HashJoin,
    InMemoryScan, IntoBatches, IntoPartitions, Limit, LocalNodeContext, LocalPhysicalPlan,
    LocalPhysicalPlanRef, MonotonicallyIncreasingId, PhysicalScan, PhysicalWrite, Pivot, Project,
    Repartition, Sample, SamplingMethod, Sort, SortMergeJoin, TopN, UDFProject, UnGroupedAggregate,
    Unpivot, VLLMProject, WindowOrderByOnly, WindowPartitionAndDynamicFrame,
    WindowPartitionAndOrderBy, WindowPartitionOnly,
};
#[cfg(feature = "python")]
pub use python::{PyLocalPhysicalPlan, register_modules};
pub use results::ExecutionEngineFinalResult;
use serde::{Deserialize, Serialize};
pub use translate::translate;

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
pub enum Input {
    ScanTasks(Vec<ScanTaskLikeRef>),
    GlobPaths(Vec<String>),
    #[serde(skip)]
    InMemory(Vec<MicroPartitionRef>),
}
