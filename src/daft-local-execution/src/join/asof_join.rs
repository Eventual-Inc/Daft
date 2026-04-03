use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{join::JoinDirection, prelude::SchemaRef};
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

pub(crate) struct AsofJoinBuildState {
    tables: Vec<MicroPartition>,
}

pub(crate) struct AsofJoinProbeState {
    sorted_build: MicroPartition,
}

pub struct AsofJoinOperator {
    left_on: BoundExpr,
    right_on: BoundExpr,
    left_by: Vec<BoundExpr>,
    right_by: Vec<BoundExpr>,
    right_schema: SchemaRef,
    output_schema: SchemaRef,
    direction: JoinDirection,
    allow_exact_matches: bool,
}

impl AsofJoinOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left_on: BoundExpr,
        right_on: BoundExpr,
        left_by: Vec<BoundExpr>,
        right_by: Vec<BoundExpr>,
        right_schema: SchemaRef,
        output_schema: SchemaRef,
        direction: JoinDirection,
        allow_exact_matches: bool,
    ) -> Self {
        Self {
            left_on,
            right_on,
            left_by,
            right_by,
            right_schema,
            output_schema,
            direction,
            allow_exact_matches,
        }
    }
}

impl JoinOperator for AsofJoinOperator {
    type BuildState = AsofJoinBuildState;
    type FinalizedBuildState = MicroPartition;
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
        let right_schema = self.right_schema.clone();
        let right_by = self.right_by.clone();
        let right_on = self.right_on.clone();

        // Concat all build batches and sort by (by_keys, on_key) ascending.
        let combined = MicroPartition::concat_or_empty(state.tables, right_schema)?;
        if combined.is_empty() {
            return Ok(combined);
        }

        let sort_keys: Vec<BoundExpr> = right_by
            .iter()
            .chain(std::iter::once(&right_on))
            .cloned()
            .collect();
        let n = sort_keys.len();
        let descending = vec![false; n];
        let nulls_first = vec![false; n];
        let sorted = combined.sort(&sort_keys, &descending, &nulls_first)?;
        Ok(sorted)
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        Ok(AsofJoinBuildState { tables: Vec::new() })
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        AsofJoinProbeState {
            sorted_build: finalized_build_state,
        }
    }

    fn probe(
        &self,
        input: MicroPartition,
        state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        if input.is_empty() {
            let empty = MicroPartition::empty(Some(self.output_schema.clone()));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        let left_on = self.left_on.clone();
        let right_on = self.right_on.clone();
        let left_by = self.left_by.clone();
        let right_by = self.right_by.clone();
        let direction = self.direction;
        let allow_exact_matches = self.allow_exact_matches;
        let sorted_build = state.sorted_build.clone();

        spawner
            .spawn(
                async move {
                    let output = {
                        // Sort this probe morsel by (by_keys, on_key) ascending.
                        let sort_keys: Vec<BoundExpr> = left_by
                            .iter()
                            .chain(std::iter::once(&left_on))
                            .cloned()
                            .collect();
                        let n = sort_keys.len();
                        let descending = vec![false; n];
                        let nulls_first = vec![false; n];
                        let sorted_input = input.sort(&sort_keys, &descending, &nulls_first)?;

                        // Merge against the pre-sorted build side.
                        sorted_input.asof_join(
                            &state.sorted_build,
                            left_on,
                            right_on,
                            &left_by,
                            &right_by,
                            direction,
                            allow_exact_matches,
                            true,
                        )?
                    };
                    Ok((
                        AsofJoinProbeState { sorted_build },
                        ProbeOutput::NeedMoreInput(Some(output)),
                    ))
                },
                Span::current(),
            )
            .into()
    }

    fn finalize_probe(
        &self,
        _states: Vec<Self::ProbeState>,
        _spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult {
        Ok(None).into()
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

    fn needs_probe_finalization(&self) -> bool {
        false
    }
}
