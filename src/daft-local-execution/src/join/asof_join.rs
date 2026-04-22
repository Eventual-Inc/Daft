use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::Span;

use crate::{
    ExecutionTaskSpawner,
    join::join_operator::{
        BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeOutput,
        ProbeResult,
    },
    pipeline::NodeName,
};

pub(crate) struct AsofJoinBuildState {
    tables: Vec<MicroPartition>,
}

pub(crate) struct AsofJoinProbeState {
    build_contents: Vec<MicroPartition>,
    probe_contents: Vec<MicroPartition>,
}

pub struct AsofJoinOperator {
    left_by: Vec<BoundExpr>,
    right_by: Vec<BoundExpr>,
    left_on: BoundExpr,
    right_on: BoundExpr,
    right_sentinel: Option<RecordBatch>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
}

impl AsofJoinOperator {
    pub fn new(
        left_by: Vec<BoundExpr>,
        right_by: Vec<BoundExpr>,
        left_on: BoundExpr,
        right_on: BoundExpr,
        right_sentinel: Option<RecordBatch>,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> Self {
        Self {
            left_by,
            right_by,
            left_on,
            right_on,
            right_sentinel,
            left_schema,
            right_schema,
        }
    }
}

impl JoinOperator for AsofJoinOperator {
    type BuildState = AsofJoinBuildState;
    type FinalizedBuildState = Vec<MicroPartition>;
    type ProbeState = AsofJoinProbeState;

    fn build(
        &self,
        input: MicroPartition,
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
        Ok(AsofJoinBuildState { tables: Vec::new() })
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        AsofJoinProbeState {
            build_contents: finalized_build_state,
            probe_contents: Vec::new(),
        }
    }

    fn probe(
        &self,
        input: MicroPartition,
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
            .expect("Expect exactly one state for AsofJoin probe finalize");
        let left_by = self.left_by.clone();
        let right_by = self.right_by.clone();
        let left_on = self.left_on.clone();
        let right_on = self.right_on.clone();
        let right_sentinel = self.right_sentinel.clone();
        let left_schema = self.left_schema.clone();
        let right_schema = self.right_schema.clone();

        spawner
            .spawn(
                async move {
                    let left_mp =
                        MicroPartition::concat_or_empty(state.build_contents, left_schema)?;

                    // If a sentinel row exists, append it to probe_contents before concat.
                    let right_mp = if let Some(sentinel) = right_sentinel {
                        let sentinel_mp = MicroPartition::new_loaded(
                            right_schema.clone(),
                            Arc::new(vec![sentinel]),
                            None,
                        );
                        let mut parts = state.probe_contents;
                        parts.push(sentinel_mp);
                        MicroPartition::concat_or_empty(parts, right_schema)?
                    } else {
                        MicroPartition::concat_or_empty(state.probe_contents, right_schema)?
                    };

                    let joined =
                        left_mp.asof_join(&right_mp, &left_by, &right_by, &left_on, &right_on)?;
                    Ok(Some(joined))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Asof Join".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::AsofJoin
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["Asof Join".to_string()]
    }

    fn max_probe_concurrency(&self) -> usize {
        1
    }

    fn needs_probe_finalization(&self) -> bool {
        true
    }
}
