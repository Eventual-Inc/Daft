use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::get_compute_pool_num_threads;
use daft_micropartition::MicroPartition;
use tokio::sync::watch;

use crate::{
    ExecutionTaskSpawner, OperatorOutput,
    pipeline::{MorselSizeRequirement, NodeName},
    runtime_stats::RuntimeStats,
};

/// Result of probing a morsel against the built state.
#[derive(Debug)]
pub(crate) enum ProbeOutput {
    /// Probe needs more input to continue processing.
    NeedMoreInput(Option<Arc<MicroPartition>>),

    /// Probe has more output to produce using the same input.
    HasMoreOutput {
        input: Arc<MicroPartition>,
        output: Option<Arc<MicroPartition>>,
    },
}

impl ProbeOutput {
    pub(crate) fn output(&self) -> Option<&Arc<MicroPartition>> {
        match self {
            Self::NeedMoreInput(mp) => mp.as_ref(),
            Self::HasMoreOutput { output, .. } => output.as_ref(),
        }
    }
}

pub(crate) type BuildStateResult<Op> = OperatorOutput<DaftResult<<Op as JoinOperator>::BuildState>>;
pub(crate) type FinalizeBuildResult<Op> = DaftResult<<Op as JoinOperator>::FinalizedBuildState>;
pub(crate) type ProbeResult<Op> =
    OperatorOutput<DaftResult<(<Op as JoinOperator>::ProbeState, ProbeOutput)>>;
pub(crate) type ProbeFinalizeResult = OperatorOutput<DaftResult<Option<Arc<MicroPartition>>>>;

pub(crate) trait JoinOperator: Send + Sync {
    type BuildState: Send + Sync + Unpin;
    type FinalizedBuildState: Send + Sync + Clone;
    type ProbeState: Send + Sync + Unpin;

    fn build(
        &self,
        input: Arc<MicroPartition>,
        state: Self::BuildState,
        spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self>
    where
        Self: Sized;

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self>
    where
        Self: Sized;

    fn make_build_state(&self) -> DaftResult<Self::BuildState>;

    fn make_probe_state(
        &self,
        receiver: watch::Receiver<Option<Self::FinalizedBuildState>>,
    ) -> Self::ProbeState;

    fn probe(
        &self,
        input: Arc<MicroPartition>,
        state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self>
    where
        Self: Sized;

    fn finalize_probe(
        &self,
        states: Vec<Self::ProbeState>,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult;

    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn multiline_display(&self) -> Vec<String>;
    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(crate::runtime_stats::DefaultRuntimeStats::new(id))
    }
    fn max_concurrency(&self) -> usize {
        get_compute_pool_num_threads()
    }
    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }
}
