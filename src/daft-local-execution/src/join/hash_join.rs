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
use tokio::sync::broadcast;
use tracing::{Span, info_span};

use crate::{
    ExecutionTaskSpawner,
    join::{
        anti_semi_join::{finalize_anti_semi, probe_anti_semi, probe_anti_semi_with_bitmap},
        index_bitmap::IndexBitmapBuilder,
        inner_join::probe_inner,
        join_operator::{
            BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeResult,
        },
        left_right_join::{
            finalize_left, finalize_right, probe_left_right, probe_left_right_with_bitmap,
        },
        outer_join::{finalize_outer, probe_outer},
    },
    pipeline::NodeName,
    pipeline_execution::OperatorExecutionOutput,
};

pub(crate) struct HashJoinBuildState {
    probe_table_builder: Box<dyn ProbeableBuilder>,
    tables: Vec<RecordBatch>,
}

pub(crate) enum HashJoinProbeState {
    Uninitialized(broadcast::Receiver<ProbeState>),
    Initialized {
        probe_state: ProbeState,
        bitmap_builder: Option<IndexBitmapBuilder>,
    },
}

impl HashJoinProbeState {
    /// Initialize the state by awaiting the receiver if uninitialized.
    /// Returns the initialized state.
    pub(crate) async fn initialize(self, needs_bitmap: bool) -> common_error::DaftResult<Self> {
        match self {
            HashJoinProbeState::Uninitialized(mut receiver) => {
                let finalized = receiver.recv().await.map_err(|e| {
                    common_error::DaftError::ValueError(format!(
                        "Failed to receive finalized build state: {}",
                        e
                    ))
                })?;

                let record_batches = finalized.get_record_batches().to_vec();
                Ok(HashJoinProbeState::Initialized {
                    probe_state: finalized,
                    bitmap_builder: if needs_bitmap {
                        Some(IndexBitmapBuilder::new(&record_batches))
                    } else {
                        None
                    },
                })
            }
            HashJoinProbeState::Initialized { .. } => Ok(self),
        }
    }

    /// Extract the probe_state and bitmap_builder from an Initialized state.
    /// Panics if the state is Uninitialized.
    pub(crate) fn into_initialized(self) -> (ProbeState, Option<IndexBitmapBuilder>) {
        match self {
            HashJoinProbeState::Initialized {
                probe_state,
                bitmap_builder,
            } => (probe_state, bitmap_builder),
            HashJoinProbeState::Uninitialized(_) => {
                panic!("State must be initialized before extracting fields")
            }
        }
    }
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

    /// Whether the probe phase needs a bitmap to keep track of matched rows.
    fn needs_bitmap(&self) -> bool {
        matches!(self.params.join_type, JoinType::Outer)
            || (self.params.join_type == JoinType::Right && !self.params.build_on_left)
            || (self.params.join_type == JoinType::Left && self.params.build_on_left)
            || (matches!(self.params.join_type, JoinType::Anti | JoinType::Semi)
                && self.params.build_on_left)
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
        receiver: broadcast::Receiver<Self::FinalizedBuildState>,
    ) -> Self::ProbeState {
        HashJoinProbeState::Uninitialized(receiver)
    }

    fn probe(
        &self,
        input: Arc<MicroPartition>,
        state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(
                self.params.output_schema.clone(),
            )));
            // If state is Uninitialized, we still need to initialize it for the next call
            // But for empty input, we can return early. The state will be initialized on next call.
            let state = match state {
                HashJoinProbeState::Uninitialized(_) => {
                    // Can't initialize here without await, so we'll return the uninitialized state
                    // It will be initialized on the next non-empty probe call
                    state
                }
                HashJoinProbeState::Initialized { .. } => state,
            };
            return Ok((state, OperatorExecutionOutput::NeedMoreInput(Some(empty)))).into();
        }

        let needs_bitmap = self.needs_bitmap();
        let params = self.params.clone();

        // Move state into async closure where we can await
        spawner
            .spawn(
                async move {
                    // Initialize state if needed
                    let state = state.initialize(needs_bitmap).await?;
                    let (probe_state, mut bitmap_builder) = state.into_initialized();

                    let result = match params.join_type {
                        JoinType::Inner => probe_inner(&input, &probe_state, &params)?,
                        JoinType::Left | JoinType::Right if needs_bitmap => {
                            probe_left_right_with_bitmap(
                                &input,
                                bitmap_builder.as_mut().expect("Bitmap builder should be set for left or right joins with bitmap"),
                                &probe_state,
                                &params,
                            )?
                        }
                        JoinType::Left | JoinType::Right => {
                            probe_left_right(&input, &probe_state, &params)?
                        }
                        JoinType::Outer => probe_outer(
                            &input,
                            &probe_state,
                            bitmap_builder.as_mut(),
                            &params,
                        )?,
                        JoinType::Anti | JoinType::Semi if needs_bitmap => {
                            probe_anti_semi_with_bitmap(
                                &input,
                                bitmap_builder.as_mut().expect("Bitmap builder should be set for anti or semi joins with bitmap"),
                                &probe_state,
                                &params,
                            )?;
                            // When using bitmap, we don't return data from probe - finalize will produce it
                            return Ok((
                                HashJoinProbeState::Initialized {
                                    probe_state,
                                    bitmap_builder,
                                },
                                OperatorExecutionOutput::NeedMoreInput(None),
                            ));
                        }
                        JoinType::Anti | JoinType::Semi => {
                            probe_anti_semi(&input, &probe_state, &params)?
                        }
                    };
                    Ok((
                        HashJoinProbeState::Initialized {
                            probe_state,
                            bitmap_builder,
                        },
                        OperatorExecutionOutput::NeedMoreInput(Some(result)),
                    ))
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
        // For joins that don't need finalization (e.g., inner joins), do nothing
        if !self.needs_bitmap() {
            return Ok(None).into();
        }

        // All states should be initialized by this point since they've all been through probe calls
        // The finalize functions will handle the enum

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
            JoinType::Left => {
                debug_assert!(
                    self.params.build_on_left,
                    "Hash join finalize left should only be called if the build side is the left side"
                );
                spawner
                    .spawn(
                        async move {
                            let output = finalize_left(states, &params).await?;
                            Ok(output)
                        },
                        Span::current(),
                    )
                    .into()
            }
            JoinType::Right => {
                debug_assert!(
                    !self.params.build_on_left,
                    "Hash join finalize right should only be called if the build side is the right side"
                );
                spawner
                    .spawn(
                        async move {
                            let output = finalize_right(states, &params).await?;
                            Ok(output)
                        },
                        Span::current(),
                    )
                    .into()
            }
            JoinType::Anti | JoinType::Semi => {
                debug_assert!(
                    self.params.build_on_left,
                    "Hash join finalize anti or semi should only be called if the build side is the left side"
                );
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
            _ => unreachable!(
                "Hash join probe phase should not need finalization for join types other than outer, left, right, anti, semi"
            ),
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
}
