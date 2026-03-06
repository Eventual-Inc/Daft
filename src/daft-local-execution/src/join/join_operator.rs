use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeInfo, NodeType};
use common_runtime::get_compute_pool_num_threads;
use daft_micropartition::MicroPartition;
use opentelemetry::metrics::Meter;

use crate::{
    ExecutionTaskSpawner, OperatorOutput,
    channel::Sender,
    pipeline::{MorselSizeRequirement, NodeName},
    runtime_stats::RuntimeStats,
};

/// Result of probing a single morsel
pub(crate) enum ProbeOutput {
    NeedMoreInput(Option<Arc<MicroPartition>>),
    HasMoreOutput {
        input: Arc<MicroPartition>,
        output: Arc<MicroPartition>,
    },
}

pub(crate) type BuildStateResult<Op> = OperatorOutput<DaftResult<<Op as JoinOperator>::BuildState>>;
pub(crate) type FinalizeBuildResult<Op> = DaftResult<<Op as JoinOperator>::FinalizedBuildState>;
pub(crate) type ProbeResult<Op> =
    OperatorOutput<DaftResult<(<Op as JoinOperator>::ProbeState, ProbeOutput)>>;
pub(crate) type ProbeFinalizeResult = OperatorOutput<DaftResult<()>>;

pub(crate) trait JoinOperator: Send + Sync {
    /// State used during the build phase
    type BuildState: Send + Sync + Unpin;

    /// Finalized build state that can be shared with probe workers
    type FinalizedBuildState: Send + Sync + Clone;

    /// State used during the probe phase (contains the finalized build state)
    type ProbeState: Send + Sync + Unpin;

    /// Add a morsel to the build state
    fn build(
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
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState;

    /// Probe a morsel against the built state
    fn probe(
        &self,
        input: Arc<MicroPartition>,
        state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self>
    where
        Self: Sized;

    /// Finalize the probe phase (for joins that need finalization like outer joins).
    /// Streams results directly to the output sender to avoid holding all finalized
    /// blocks in memory at once (prevents OOM for large build-side data).
    fn finalize_probe(
        &self,
        states: Vec<Self::ProbeState>,
        spawner: &ExecutionTaskSpawner,
        sender: Sender<Arc<MicroPartition>>,
        runtime_stats: Arc<dyn RuntimeStats>,
    ) -> ProbeFinalizeResult;

    /// Name of the operator
    fn name(&self) -> NodeName;

    /// Type of the operator
    fn op_type(&self) -> NodeType;

    /// Multiline display for visualization
    fn multiline_display(&self) -> Vec<String>;

    /// Create runtime stats
    fn make_runtime_stats(&self, meter: &Meter, node_info: &NodeInfo) -> Arc<dyn RuntimeStats> {
        Arc::new(crate::runtime_stats::DefaultRuntimeStats::new(
            meter, node_info,
        ))
    }

    /// Maximum number of concurrent probe workers
    fn max_probe_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }

    /// Morsel size requirement for probe phase
    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }

    /// Whether this join needs finalization after probe phase
    fn needs_probe_finalization(&self) -> bool;
}
