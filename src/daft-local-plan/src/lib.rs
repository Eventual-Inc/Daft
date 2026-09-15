pub mod agg;
#[allow(unused)]
mod plan;
#[cfg(feature = "python")]
pub mod python;
mod results;
mod translate;
use daft_micropartition::MicroPartitionRef;
use daft_scan::ScanTaskRef;
#[cfg(feature = "celeborn")]
pub use plan::CelebornShuffleReadInput;
pub use plan::{
    AsofJoin, CommitWrite, Concat, CrossJoin, Dedup, Explode, Filter, FlightShuffleReadInput,
    GatherWrite, GlobScan, HashAggregate, HashJoin, InMemoryScan, IntoBatches, IntoPartitions,
    Limit, LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef, MonotonicallyIncreasingId,
    PhysicalScan, PhysicalWrite, Pivot, PlaceholderScan, Project, RepartitionWrite, Sample,
    SamplingMethod, ShuffleBackend, ShuffleRead, ShuffleReadBackend, Sort, SortMergeJoin,
    StageCheckpointKeys, TopN, UDFProject, UnGroupedAggregate, Unpivot, VLLMProject,
    WindowOrderByOnly, WindowPartitionAndDynamicFrame, WindowPartitionAndOrderBy,
    WindowPartitionOnly,
};
#[cfg(feature = "python")]
pub use plan::{CatalogWrite, DataSink, DistributedActorPoolProject, DistributedLimit, LanceWrite};
#[cfg(feature = "python")]
pub use python::{PyLocalPhysicalPlan, register_modules};
pub use results::ExecutionStats;
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
    ScanTasks(Vec<ScanTaskRef>),
    GlobPaths(Vec<String>),
    FlightShuffle(Vec<FlightShuffleReadInput>),
    #[cfg(feature = "celeborn")]
    CelebornShuffle(Vec<CelebornShuffleReadInput>),
    #[serde(skip)]
    InMemory(Vec<MicroPartitionRef>),
}
