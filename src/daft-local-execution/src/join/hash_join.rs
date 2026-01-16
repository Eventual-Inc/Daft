use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{ProbeState, ProbeableBuilder, RecordBatch, make_probeable_builder};
use indexmap::IndexSet;
use itertools::Itertools;
use tracing::{Span, info_span};

use crate::{
    ExecutionTaskSpawner,
    join::{
        anti_semi_join::{finalize_anti_semi, probe_anti_semi, probe_anti_semi_with_bitmap},
        index_bitmap::IndexBitmapBuilder,
        inner_join::probe_inner,
        join_operator::{
            BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeOutput,
            ProbeResult,
        },
        left_right_join::{
            finalize_left, finalize_right, probe_left_right, probe_left_right_with_bitmap,
        },
        outer_join::{finalize_outer, probe_outer},
    },
    pipeline::NodeName,
};

pub(crate) struct HashJoinBuildState {
    probe_table_builder: Box<dyn ProbeableBuilder>,
    tables: Vec<RecordBatch>,
}

pub(crate) struct HashJoinProbeState {
    pub(crate) probe_state: ProbeState,
    pub(crate) bitmap_builder: Option<IndexBitmapBuilder>,
}

impl HashJoinBuildState {
    fn new(
        key_schema: &SchemaRef,
        nulls_equal_aware: Option<&Vec<bool>>,
        track_indices: bool,
    ) -> DaftResult<Self> {
        Ok(Self {
            probe_table_builder: make_probeable_builder(
                key_schema.clone(),
                nulls_equal_aware,
                track_indices,
            )?,
            tables: Vec::new(),
        })
    }

    fn add_tables(
        &mut self,
        input: &Arc<MicroPartition>,
        params: &HashJoinParams,
    ) -> DaftResult<()> {
        let input_tables = input.record_batches();
        if input_tables.is_empty() {
            let empty_table = RecordBatch::empty(Some(input.schema()));
            let join_keys = empty_table.eval_expression_list(&params.build_on)?;
            self.probe_table_builder.add_table(&join_keys)?;
            self.tables.push(empty_table);
        } else {
            for table in input_tables {
                self.tables.push(table.clone());
                let join_keys = table.eval_expression_list(&params.build_on)?;
                self.probe_table_builder.add_table(&join_keys)?;
            }
        }
        Ok(())
    }

    fn finalize(self) -> ProbeState {
        let pt = self.probe_table_builder.build();
        ProbeState::new(pt, self.tables)
    }
}

pub(crate) struct HashJoinParams {
    pub key_schema: SchemaRef,
    pub build_on: Vec<BoundExpr>,
    pub probe_on: Vec<BoundExpr>,
    pub nulls_equal_aware: Option<Vec<bool>>,
    pub track_indices: bool,
    pub join_type: JoinType,
    pub build_on_left: bool,
    pub left_schema: SchemaRef,
    pub right_schema: SchemaRef,
    pub common_join_cols: IndexSet<String>,
    pub output_schema: SchemaRef,
}

pub struct HashJoinOperator {
    params: Arc<HashJoinParams>,
}

impl HashJoinOperator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key_schema: SchemaRef,
        build_on: Vec<BoundExpr>,
        probe_on: Vec<BoundExpr>,
        nulls_equal_aware: Option<Vec<bool>>,
        track_indices: bool,
        join_type: JoinType,
        build_on_left: bool,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        common_join_cols: IndexSet<String>,
        output_schema: SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            params: Arc::new(HashJoinParams {
                key_schema,
                build_on,
                probe_on,
                nulls_equal_aware,
                track_indices,
                join_type,
                build_on_left,
                left_schema,
                right_schema,
                common_join_cols,
                output_schema,
            }),
        })
    }
}

impl JoinOperator for HashJoinOperator {
    type BuildState = HashJoinBuildState;
    type FinalizedBuildState = ProbeState;
    type ProbeState = HashJoinProbeState;

    fn build(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::BuildState,
        spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self> {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    state.add_tables(&input, &params)?;
                    Ok(state)
                },
                info_span!("HashJoinOperator::build_state"),
            )
            .into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        let finalized_probe_state = state.finalize();
        Ok(finalized_probe_state).into()
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        HashJoinBuildState::new(
            &self.params.key_schema,
            self.params.nulls_equal_aware.as_ref(),
            self.params.track_indices,
        )
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        let record_batches = finalized_build_state.get_record_batches().to_vec();
        HashJoinProbeState {
            probe_state: finalized_build_state,
            bitmap_builder: if self.needs_bitmap() {
                Some(IndexBitmapBuilder::new(&record_batches))
            } else {
                None
            },
        }
    }

    fn probe(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(
                self.params.output_schema.clone(),
            )));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        let needs_bitmap = self.needs_bitmap();
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    let result = match params.join_type {
                        JoinType::Inner => probe_inner(&input, &state.probe_state, &params)?,
                        JoinType::Left | JoinType::Right if needs_bitmap => {
                            probe_left_right_with_bitmap(
                                &input,
                                state.bitmap_builder.as_mut().expect("bitmap should be set"),
                                &state.probe_state,
                                &params,
                            )?
                        }
                        JoinType::Left | JoinType::Right => {
                            probe_left_right(&input, &state.probe_state, &params)?
                        }
                        JoinType::Outer => probe_outer(
                            &input,
                            &state.probe_state,
                            state.bitmap_builder.as_mut(),
                            &params,
                        )?,
                        JoinType::Anti | JoinType::Semi if needs_bitmap => {
                            probe_anti_semi_with_bitmap(
                                &input,
                                state.bitmap_builder.as_mut().expect("bitmap should be set"),
                                &state.probe_state,
                                &params,
                            )?;
                            // When using bitmap, we don't return data from probe - finalize will produce it
                            return Ok((state, ProbeOutput::NeedMoreInput(None)));
                        }
                        JoinType::Anti | JoinType::Semi => {
                            probe_anti_semi(&input, &state.probe_state, &params)?
                        }
                    };
                    Ok((state, ProbeOutput::NeedMoreInput(Some(result))))
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
        let needs_bitmap = self.needs_bitmap();
        if !needs_bitmap {
            return Ok(None).into();
        }

        let params = self.params.clone();
        match self.params.join_type {
            JoinType::Outer => spawner
                .spawn(
                    async move {
                        let output = finalize_outer(states, &params).await?;
                        Ok(output)
                    },
                    Span::current(),
                )
                .into(),
            JoinType::Left => spawner
                .spawn(
                    async move {
                        let output = finalize_left(states, &params).await?;
                        Ok(output)
                    },
                    Span::current(),
                )
                .into(),
            JoinType::Right => spawner
                .spawn(
                    async move {
                        let output = finalize_right(states, &params).await?;
                        Ok(output)
                    },
                    Span::current(),
                )
                .into(),
            JoinType::Anti | JoinType::Semi if self.params.build_on_left => {
                let is_semi = self.params.join_type == JoinType::Semi;
                spawner
                    .spawn(
                        async move {
                            let output = finalize_anti_semi(states, is_semi).await?;
                            Ok(output)
                        },
                        Span::current(),
                    )
                    .into()
            }
            _ => Ok(None).into(),
        }
    }

    fn name(&self) -> NodeName {
        match self.params.join_type {
            JoinType::Inner => "Hash Join (Inner)".into(),
            JoinType::Left => "Hash Join (Left)".into(),
            JoinType::Right => "Hash Join (Right)".into(),
            JoinType::Outer => "Hash Join (Outer)".into(),
            JoinType::Anti => "Hash Join (Anti)".into(),
            JoinType::Semi => "Hash Join (Semi)".into(),
        }
    }

    fn op_type(&self) -> NodeType {
        // Use existing node types for now
        match self.params.join_type {
            JoinType::Inner => NodeType::InnerHashJoinProbe,
            JoinType::Left | JoinType::Right | JoinType::Outer => NodeType::OuterHashJoinProbe,
            JoinType::Anti | JoinType::Semi => NodeType::AntiSemiHashJoinProbe,
        }
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!("Hash Join ({:?}):", self.params.join_type));
        display.push(format!("Build on left: {}", self.params.build_on_left));
        display.push(format!("Track Indices: {}", self.params.track_indices));
        display.push(format!(
            "Key Schema: {}",
            self.params.key_schema.short_string()
        ));
        if let Some(null_equals_nulls) = &self.params.nulls_equal_aware {
            display.push(format!(
                "Null equals Nulls = [{}]",
                null_equals_nulls.iter().map(|b| b.to_string()).join(", ")
            ));
        }
        display
    }

    fn needs_probe_finalization(&self) -> bool {
        matches!(
            self.params.join_type,
            JoinType::Outer | JoinType::Left | JoinType::Right
        ) || (matches!(self.params.join_type, JoinType::Anti | JoinType::Semi)
            && self.params.build_on_left)
    }
}

// Helper methods
impl HashJoinOperator {
    fn needs_bitmap(&self) -> bool {
        matches!(self.params.join_type, JoinType::Outer)
            || (self.params.join_type == JoinType::Right && !self.params.build_on_left)
            || (self.params.join_type == JoinType::Left && self.params.build_on_left)
            || (matches!(self.params.join_type, JoinType::Anti | JoinType::Semi)
                && self.params.build_on_left)
    }
}
