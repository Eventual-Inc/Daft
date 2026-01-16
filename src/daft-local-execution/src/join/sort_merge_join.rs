use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::join::JoinType;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use tracing::Span;

use crate::{
    ExecutionTaskSpawner,
    join::join_operator::{
        BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeOutput,
        ProbeResult,
    },
    pipeline::NodeName,
};

pub struct SortMergeJoinBuildState {
    tables: Vec<Arc<MicroPartition>>,
}

pub(crate) struct SortMergeJoinProbeState {
    build_contents: Vec<Arc<MicroPartition>>,
    probe_contents: Vec<Arc<MicroPartition>>,
}

pub struct SortMergeJoinOperator {
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    join_type: JoinType,
}

impl SortMergeJoinOperator {
    pub fn new(left_on: Vec<BoundExpr>, right_on: Vec<BoundExpr>, join_type: JoinType) -> Self {
        Self {
            left_on,
            right_on,
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
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        SortMergeJoinProbeState {
            build_contents: finalized_build_state,
            probe_contents: Vec::new(),
        }
    }

    fn probe(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::ProbeState,
        _spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        if !input.is_empty() {
            state.probe_contents.push(input);
        }
        Ok((state, ProbeOutput::NeedMoreInput(None))).into()
    }

    fn finalize_probe(
        &self,
        states: Vec<Self::ProbeState>,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult {
        let state = states
            .into_iter()
            .next()
            .expect("Expect exactly one state for SortMergeJoin");
        let left_on = self.left_on.clone();
        let right_on = self.right_on.clone();
        let join_type = self.join_type;

        spawner
            .spawn(
                async move {
                    let left_mp = MicroPartition::concat(&state.build_contents)?;
                    let right_mp = MicroPartition::concat(&state.probe_contents)?;

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

    fn needs_probe_finalization(&self) -> bool {
        true
    }
}
