pub mod agg;
#[allow(unused)]
mod plan;
#[cfg(feature = "python")]
pub mod python;
mod results;
mod translate;
use daft_micropartition::MicroPartitionRef;
use daft_scan::ScanTaskRef;
#[cfg(feature = "python")]
pub use plan::{CatalogWrite, DataSink, DistributedActorPoolProject, LanceWrite};
pub use plan::{
    CommitWrite, Concat, CrossJoin, Dedup, Explode, Filter, FlightShuffleReadInput, GlobScan,
    HashAggregate, HashJoin, InMemoryScan, IntoBatches, IntoPartitions, Limit, LocalNodeContext,
    LocalPhysicalPlan, LocalPhysicalPlanRef, MonotonicallyIncreasingId, PhysicalScan,
    PhysicalWrite, Pivot, PlaceholderScan, Project, Repartition, Sample, SamplingMethod,
    ShuffleRead, ShuffleReadBackend, ShuffleWrite, ShuffleWriteBackend, ShuffleWriteSpec, Sort,
    SortMergeJoin, TopN, UDFProject, UnGroupedAggregate, Unpivot, VLLMProject, WindowOrderByOnly,
    WindowPartitionAndDynamicFrame, WindowPartitionAndOrderBy, WindowPartitionOnly,
};
#[cfg(feature = "python")]
pub use python::{PyExchangeWriteInfo, PyLocalPhysicalPlan, register_modules};
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
    #[serde(skip)]
    InMemory(Vec<MicroPartitionRef>),
}
