pub mod agg;
#[allow(unused)]
mod plan;
#[cfg(feature = "python")]
mod python;
mod translate;
use std::{collections::HashMap, sync::Arc};

use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use daft_core::prelude::SchemaRef;
use daft_logical_plan::InMemoryInfo;
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
use serde::{Deserialize, Serialize};
pub use translate::translate;

/// Type of input expected by a source, containing the actual input data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InputType {
    ScanTask(Arc<Vec<ScanTaskLikeRef>>),
    InMemory(InMemoryInfo),
    GlobPaths(Arc<Vec<String>>),
}

/// Specification for an input source in the plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputSpec {
    pub source_id: String,
    pub input_type: InputType,
    pub schema: SchemaRef,
    pub pushdowns: Option<Pushdowns>,
    pub io_config: Option<common_io_config::IOConfig>,
}

/// Result of translating a logical plan, containing both the physical plan and input specifications
#[derive(Debug, Clone)]
pub struct TranslationResult {
    pub plan: crate::LocalPhysicalPlanRef,
    pub input_specs: HashMap<String, InputSpec>,
}
