use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinType, prelude::SchemaRef};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::{Span, info_span};

use crate::{
    ExecutionTaskSpawner,
    dynamic_batching::StaticBatchingStrategy,
    join::join_operator::{
        BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeOutput, ProbeOutput, ProbeFinalizeResult,
        ProbeResult,
    },
    pipeline::NodeName,
};

pub struct SortMergeJoinBuildState {
    tables: Vec<RecordBatch>,
}

pub struct SortMergeJoinOperator {
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    left_schema: SchemaRef,
    join_type: JoinType,
}

impl SortMergeJoinOperator {
    pub fn new(
        left_on: Vec<BoundExpr>,
        right_on: Vec<BoundExpr>,
        left_schema: SchemaRef,
        join_type: JoinType,
    ) -> Self {
        Self {
            left_on,
            right_on,
            left_schema,
            join_type,
        }
    }
}

impl JoinOperator for SortMergeJoinOperator {
    type BuildState = SortMergeJoinBuildState;
    type FinalizedBuildState = Vec<RecordBatch>;
    type ProbeState = SortMergeJoinProbeState;
    type BatchingStrategy = StaticBatchingStrategy;

    fn build_state(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::BuildState,
        spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self> {
        spawner
            .spawn(
                async move {
                    if !input.is_empty() {
                        state.tables.extend(input.record_batches().iter().cloned());
                    }
                    Ok(state)
                },
                info_span!("SortMergeJoinOperator::build_state"),
            )
            .into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        Ok(Arc::new(state.tables)).into()
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        Ok(SortMergeJoinBuildState { tables: Vec::new() })
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Arc<Self::FinalizedBuildState>,
    ) -> DaftResult<Self::ProbeState> {
        Ok(SortMergeJoinProbeState {
            build_contents: finalized_build_state,
            probe_contents: Vec::new(),
            left_on: self.left_on.clone(),
            right_on: self.right_on.clone(),
            left_schema: self.left_schema.clone(),
            join_type: self.join_type,
        })
    }

    fn probe(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        spawner
            .spawn(
                async move {
                    // Collect all probe inputs for finalization
                    state.probe_contents.push(input);
                    Ok((state, ProbeOutput::NeedMoreInput(None)))
                },
                Span::current(),
            )
            .into()
    }

    fn finalize_probe(
        &self,
        states: Vec<Self::ProbeState>,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult<Self> {
        debug_assert_eq!(states.len(), 1);
        let state = states.into_iter().next().expect("Expect exactly one state");
        let left_on = state.left_on.clone();
        let right_on = state.right_on.clone();
        let left_schema = state.left_schema.clone();
        let join_type = state.join_type;

        spawner
            .spawn(
                async move {
                    let left_mp = MicroPartition::new_loaded(
                        left_schema.clone(),
                        state.build_contents.clone(),
                        None,
                    );
                    let right_mp = MicroPartition::concat(state.probe_contents.iter())?;

                    // TODO: Handle pre-sorted?
                    let joined = left_mp
                        .sort_merge_join(&right_mp, &left_on, &right_on, join_type, false)?;
                    Ok(ProbeFinalizeOutput::Finished(Some(Arc::new(joined))))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "SortMergeJoin".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::SortMergeJoinProbe
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["SortMergeJoin".to_string()]
    }

    fn max_probe_concurrency(&self) -> usize {
        1
    }

    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }

    fn needs_probe_finalization(&self) -> bool {
        true
    }
}

pub(crate) struct SortMergeJoinProbeState {
    build_contents: Arc<Vec<RecordBatch>>,
    probe_contents: Vec<Arc<MicroPartition>>,
    left_on: Vec<BoundExpr>,
    right_on: Vec<BoundExpr>,
    left_schema: SchemaRef,
    join_type: JoinType,
}
