use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinType, prelude::SchemaRef};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use tokio::sync::broadcast;
use tracing::Span;

use crate::{
    ExecutionTaskSpawner,
    join::join_operator::{
        BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeResult,
    },
    pipeline::NodeName,
    pipeline_execution::OperatorExecutionOutput,
};

pub(crate) struct SortMergeJoinBuildState {
    tables: Vec<Arc<MicroPartition>>,
}

pub(crate) enum SortMergeJoinProbeState {
    Uninitialized(broadcast::Receiver<Vec<Arc<MicroPartition>>>),
    Initialized {
        build_contents: Vec<Arc<MicroPartition>>,
        probe_contents: Vec<Arc<MicroPartition>>,
    },
}

impl SortMergeJoinProbeState {
    /// Initialize the state by awaiting the receiver if uninitialized.
    /// Returns the initialized state.
    pub(crate) async fn initialize(self) -> common_error::DaftResult<Self> {
        match self {
            SortMergeJoinProbeState::Uninitialized(mut receiver) => {
                let finalized = receiver.recv().await.map_err(|e| {
                    common_error::DaftError::ValueError(format!(
                        "Failed to receive finalized build state: {}",
                        e
                    ))
                })?;

                Ok(SortMergeJoinProbeState::Initialized {
                    build_contents: finalized,
                    probe_contents: Vec::new(),
                })
            }
            SortMergeJoinProbeState::Initialized { .. } => Ok(self),
        }
    }

    /// Extract build_contents and probe_contents from an Initialized state.
    /// Panics if the state is Uninitialized.
    pub(crate) fn into_initialized(self) -> (Vec<Arc<MicroPartition>>, Vec<Arc<MicroPartition>>) {
        match self {
            SortMergeJoinProbeState::Initialized {
                build_contents,
                probe_contents,
            } => (build_contents, probe_contents),
            SortMergeJoinProbeState::Uninitialized(_) => {
                panic!("State must be initialized before extracting fields")
            }
        }
    }

    /// Get a mutable reference to probe_contents if initialized.
    pub(crate) fn probe_contents_mut(&mut self) -> Option<&mut Vec<Arc<MicroPartition>>> {
        match self {
            SortMergeJoinProbeState::Initialized { probe_contents, .. } => Some(probe_contents),
            SortMergeJoinProbeState::Uninitialized(_) => None,
        }
    }
}

pub struct SortMergeJoinOperator {
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_type: JoinType,
}

impl SortMergeJoinOperator {
    pub fn new(
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        join_type: JoinType,
    ) -> Self {
        Self {
            left_on,
            right_on,
            left_schema,
            right_schema,
            join_type,
        }
    }
}

impl JoinOperator for SortMergeJoinOperator {
    type BuildState = SortMergeJoinBuildState;
    type FinalizedBuildState = Vec<Arc<MicroPartition>>;
    type ProbeState = SortMergeJoinProbeState;

    fn build(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::BuildState,
        _spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self> {
        if !input.is_empty() {
            state.tables.push(input);
        }
        Ok(state).into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        Ok(state.tables).into()
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        Ok(SortMergeJoinBuildState { tables: Vec::new() })
    }

    fn make_probe_state(
        &self,
        receiver: broadcast::Receiver<Self::FinalizedBuildState>,
    ) -> Self::ProbeState {
        SortMergeJoinProbeState::Uninitialized(receiver)
    }

    fn probe(
        &self,
        input: Arc<MicroPartition>,
        state: Self::ProbeState,
        _spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        // For sort merge join, probe is synchronous, so we need to handle initialization differently
        // Since we can't await in a non-async function, we'll need to spawn a task or handle it synchronously
        // But sort_merge_join doesn't spawn tasks, so we need a different approach

        // Actually, looking at the implementation, probe is called synchronously and returns immediately.
        // The initialization needs to happen before we can use the state. Since we can't await here,
        // we'll need to make probe async or handle initialization differently.

        // For now, let's make probe spawn a task for initialization if needed, even though it's simple
        // Or we can use a blocking approach with tokio::runtime::Handle::current().block_on()
        // But that's not ideal. Let's check if we can make the state initialization lazy in a different way.

        // Actually, the simplest is to use tokio::task::block_in_place or handle.current().block_on()
        // But that's blocking. The better approach is to spawn a task even for this simple case.

        // Let's use a simpler approach: spawn a task that initializes and then processes
        let spawner = _spawner.clone();
        spawner
            .spawn(
                async move {
                    // Initialize state if needed
                    let mut state = state.initialize().await?;

                    // Add input to probe_contents if not empty
                    if let Some(probe_contents) = state.probe_contents_mut() {
                        if !input.is_empty() {
                            probe_contents.push(input);
                        }
                    }

                    Ok((state, OperatorExecutionOutput::NeedMoreInput(None)))
                },
                Span::current(),
            )
            .into()
    }

    fn finalize_probe(
        &self,
        states: Vec<Self::ProbeState>,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult {
        let state = states
            .into_iter()
            .next()
            .expect("Expect exactly one state for SortMergeJoin probe finalize");

        // Extract Initialized state
        let (build_contents, probe_contents) = state.into_initialized();

        let left_on = self.left_on.clone();
        let right_on = self.right_on.clone();
        let left_schema = self.left_schema.clone();
        let right_schema = self.right_schema.clone();
        let join_type = self.join_type;

        spawner
            .spawn(
                async move {
                    let left_mp = MicroPartition::concat_or_empty(&build_contents, left_schema)?;
                    let right_mp = MicroPartition::concat_or_empty(&probe_contents, right_schema)?;

                    // TODO: Handle pre-sorted?
                    let joined = left_mp
                        .sort_merge_join(&right_mp, &left_on, &right_on, join_type, false)?;
                    Ok(Some(Arc::new(joined)))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Sort Merge Join".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::SortMergeJoinProbe
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["Sort Merge Join".to_string()]
    }

    fn max_probe_concurrency(&self) -> usize {
        1
    }
}
