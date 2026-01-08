use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::get_compute_pool_num_threads;
use daft_micropartition::MicroPartition;

use crate::{
    dynamic_batching::BatchingStrategy, pipeline::{MorselSizeRequirement, NodeName}, runtime_stats::RuntimeStats, ExecutionTaskSpawner, OperatorOutput
};

/// Result of probing a single morsel
pub enum ProbeOutput {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    HasMoreOutput(Arc<MicroPartition>),
}

/// Result of finalizing probe phase
pub enum ProbeFinalizeOutput<Op: JoinOperator> {
    HasMoreOutput {
        states: Vec<Op::ProbeState>,
        output: Option<Arc<MicroPartition>>,
    },
    Finished(Option<Arc<MicroPartition>>),
}

pub(crate) type BuildStateResult<Op> = OperatorOutput<DaftResult<<Op as JoinOperator>::BuildState>>;
pub(crate) type FinalizeBuildResult<Op> =
    OperatorOutput<DaftResult<Arc<<Op as JoinOperator>::FinalizedBuildState>>>;
pub(crate) type ProbeResult<Op> =
    OperatorOutput<DaftResult<(<Op as JoinOperator>::ProbeState, ProbeOutput)>>;
pub(crate) type ProbeFinalizeResult<Op> = OperatorOutput<DaftResult<ProbeFinalizeOutput<Op>>>;

use std::sync::Arc;

/// Trait for join operators that handle both build and probe phases
pub(crate) trait JoinOperator: Send + Sync {
    /// State used during the build phase
    type BuildState: Send + Sync + Unpin;

    /// Finalized build state that can be shared with probe workers
    type FinalizedBuildState: Send + Sync + Clone;

    /// State used during the probe phase (contains the finalized build state)
    type ProbeState: Send + Sync + Unpin;

    /// Batching strategy for probe phase
    type BatchingStrategy: BatchingStrategy + 'static;

    /// Add a morsel to the build state
    fn build_state(
        &self,
        input: Arc<MicroPartition>,
        state: Self::BuildState,
        spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self>
    where
        Self: Sized;

    /// Finalize the build state and create the finalized build state
    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self>
    where
        Self: Sized;

    /// Create a new build state
    fn make_build_state(&self) -> DaftResult<Self::BuildState>;

    /// Create a probe state from the finalized build state
    fn make_probe_state(
        &self,
        finalized_build_state: Arc<Self::FinalizedBuildState>,
    ) -> DaftResult<Self::ProbeState>;

    /// Probe a morsel against the built state
    fn probe(
        &self,
        input: Arc<MicroPartition>,
        state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self>
    where
        Self: Sized;

    /// Finalize the probe phase (for joins that need finalization like outer joins)
    fn finalize_probe(
        &self,
        states: Vec<Self::ProbeState>,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult<Self>
    where
        Self: Sized;

    /// Name of the operator
    fn name(&self) -> NodeName;

    /// Type of the operator
    fn op_type(&self) -> NodeType;

    /// Multiline display for visualization
    fn multiline_display(&self) -> Vec<String>;

    /// Create runtime stats
    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(crate::runtime_stats::DefaultRuntimeStats::new(id))
    }

    /// Maximum number of concurrent probe workers
    fn max_probe_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    /// Morsel size requirement for probe phase
    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }

    /// Batching strategy for probe phase
    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy>;

    /// Whether this join needs finalization after probe phase
    fn needs_probe_finalization(&self) -> bool {
        false
    }
}
