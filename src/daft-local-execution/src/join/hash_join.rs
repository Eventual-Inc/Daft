use std::sync::Arc;

use daft_core::prelude::bitmap::{Bitmap, MutableBitmap};
use daft_core::prelude::{IntoSeries, Schema, SchemaRef};
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{ProbeState, ProbeableBuilder, RecordBatch, make_probeable_builder};
use indexmap::IndexSet;
use itertools::Itertools;
use tracing::{info_span, Span};

use crate::{
    ExecutionTaskSpawner,
    dynamic_batching::StaticBatchingStrategy,
    join::join_operator::{
        BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeOutput, ProbeOutput,
        ProbeResult,
    },
    pipeline::{MorselSizeRequirement, NodeName},
};

// Reuse ProbeTableState from hash_join_build.rs
pub(crate) struct ProbeTableState {
    probe_table_builder: Box<dyn ProbeableBuilder>,
    projection: Vec<BoundExpr>,
    tables: Vec<RecordBatch>,
}

impl ProbeTableState {
    fn new(
        key_schema: &SchemaRef,
        projection: Vec<BoundExpr>,
        nulls_equal_aware: Option<&Vec<bool>>,
        track_indices: bool,
    ) -> DaftResult<Self> {
        Ok(Self {
            probe_table_builder: make_probeable_builder(
                key_schema.clone(),
                nulls_equal_aware,
                track_indices,
            )?,
            projection,
            tables: Vec::new(),
        })
    }

    fn add_tables(&mut self, input: &Arc<MicroPartition>) -> DaftResult<()> {
        let input_tables = input.record_batches();
        if input_tables.is_empty() {
            let empty_table = RecordBatch::empty(Some(input.schema()));
            let join_keys = empty_table.eval_expression_list(&self.projection)?;
            self.probe_table_builder.add_table(&join_keys)?;
            self.tables.push(empty_table);
        } else {
            for table in input_tables {
                self.tables.push(table.clone());
                let join_keys = table.eval_expression_list(&self.projection)?;
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

pub struct HashJoinOperator {
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
}

impl HashJoinOperator {
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
        })
    }
}

impl JoinOperator for HashJoinOperator {
    type BuildState = ProbeTableState;
    type FinalizedBuildState = ProbeState;
    type ProbeState = HashJoinProbeState;
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
                    state.add_tables(&input)?;
                    Ok(state)
                },
                info_span!("HashJoinOperator::build_state"),
            )
            .into()
    }

    fn finalize_build(
        &self,
        state: Self::BuildState,
    ) -> FinalizeBuildResult<Self> {
        let finalized_probe_state = state.finalize();
        Ok(Arc::new(finalized_probe_state)).into()
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        ProbeTableState::new(
            &self.key_schema,
            self.build_on.clone(),
            self.nulls_equal_aware.as_ref(),
            self.track_indices,
        )
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Arc<Self::FinalizedBuildState>,
    ) -> DaftResult<Self::ProbeState> {
        let record_batches = finalized_build_state.get_record_batches().to_vec();
        Ok(HashJoinProbeState {
            probe_state: finalized_build_state,
            bitmap_builder: if self.needs_bitmap() {
                Some(IndexBitmapBuilder::new(&record_batches))
            } else {
                None
            },
        })
    }

    fn probe(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        if input.is_empty() {
            // Match old code: use output_schema for all join types
            // For anti/semi joins, output_schema is the probe side schema
            let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        let params = HashJoinProbeParams {
            probe_on: self.probe_on.clone(),
            join_type: self.join_type,
            build_on_left: self.build_on_left,
            common_join_cols: self.common_join_cols.clone(),
            left_schema: self.left_schema.clone(),
            right_schema: self.right_schema.clone(),
            output_schema: self.output_schema.clone(),
        };

        let needs_bitmap = self.needs_bitmap();
        spawner
            .spawn(
                async move {
                    let result = match params.join_type {
                        JoinType::Inner => {
                            Self::probe_inner(&input, &state.probe_state, &params)?
                        }
                        JoinType::Left | JoinType::Right if needs_bitmap => {
                            Self::probe_left_right_with_bitmap(
                                &input,
                                state.bitmap_builder.as_mut().expect("bitmap should be set"),
                                &state.probe_state,
                                &params,
                            )?
                        }
                        JoinType::Left | JoinType::Right => {
                            Self::probe_left_right(&input, &state.probe_state, &params)?
                        }
                        JoinType::Outer => {
                            Self::probe_outer(
                                &input,
                                &state.probe_state,
                                state.bitmap_builder.as_mut(),
                                &params,
                            )?
                        }
                        JoinType::Anti | JoinType::Semi if needs_bitmap => {
                            Self::probe_anti_semi_with_bitmap(
                                &input,
                                state.bitmap_builder.as_mut().expect("bitmap should be set"),
                                &state.probe_state,
                                &params,
                            )?;
                            // When using bitmap, we don't return data from probe - finalize will produce it
                            return Ok((state, ProbeOutput::NeedMoreInput(None)));
                        }
                        JoinType::Anti | JoinType::Semi => {
                            Self::probe_anti_semi(&input, &state.probe_state, &params)?
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
    ) -> crate::join::join_operator::ProbeFinalizeResult<Self> {
        let needs_bitmap = self.needs_bitmap();
        if !needs_bitmap {
            return Ok(ProbeFinalizeOutput::Finished(None)).into();
        }

        match self.join_type {
            JoinType::Outer => {
                let params = HashJoinProbeParams {
                    probe_on: self.probe_on.clone(),
                    join_type: self.join_type,
                    build_on_left: self.build_on_left,
                    common_join_cols: self.common_join_cols.clone(),
                    left_schema: self.left_schema.clone(),
                    right_schema: self.right_schema.clone(),
                    output_schema: self.output_schema.clone(),
                };
                spawner
                    .spawn(
                        async move {
                            let output = Self::finalize_outer(states, &params).await?;
                            Ok(ProbeFinalizeOutput::Finished(output))
                        },
                        Span::current(),
                    )
                    .into()
            }
            JoinType::Left => {
                let params = HashJoinProbeParams {
                    probe_on: self.probe_on.clone(),
                    join_type: self.join_type,
                    build_on_left: self.build_on_left,
                    common_join_cols: self.common_join_cols.clone(),
                    left_schema: self.left_schema.clone(),
                    right_schema: self.right_schema.clone(),
                    output_schema: self.output_schema.clone(),
                };
                spawner
                    .spawn(
                        async move {
                            let output = Self::finalize_left(states, &params).await?;
                            Ok(ProbeFinalizeOutput::Finished(output))
                        },
                        Span::current(),
                    )
                    .into()
            }
            JoinType::Right => {
                let params = HashJoinProbeParams {
                    probe_on: self.probe_on.clone(),
                    join_type: self.join_type,
                    build_on_left: self.build_on_left,
                    common_join_cols: self.common_join_cols.clone(),
                    left_schema: self.left_schema.clone(),
                    right_schema: self.right_schema.clone(),
                    output_schema: self.output_schema.clone(),
                };
                spawner
                    .spawn(
                        async move {
                            let output = Self::finalize_right(states, &params).await?;
                            Ok(ProbeFinalizeOutput::Finished(output))
                        },
                        Span::current(),
                    )
                    .into()
            }
            JoinType::Anti | JoinType::Semi if self.build_on_left => {
                let is_semi = self.join_type == JoinType::Semi;
                spawner
                    .spawn(
                        async move {
                            let output = Self::finalize_anti_semi(states, is_semi).await?;
                            Ok(ProbeFinalizeOutput::Finished(output))
                        },
                        Span::current(),
                    )
                    .into()
            }
            _ => Ok(ProbeFinalizeOutput::Finished(None)).into(),
        }
    }

    fn name(&self) -> NodeName {
        match self.join_type {
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
        match self.join_type {
            JoinType::Inner => NodeType::InnerHashJoinProbe,
            JoinType::Left | JoinType::Right | JoinType::Outer => NodeType::OuterHashJoinProbe,
            JoinType::Anti | JoinType::Semi => NodeType::AntiSemiHashJoinProbe,
        }
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!("Hash Join ({:?}):", self.join_type));
        display.push(format!("Build on left: {}", self.build_on_left));
        display.push(format!("Track Indices: {}", self.track_indices));
        display.push(format!("Key Schema: {}", self.key_schema.short_string()));
        if let Some(null_equals_nulls) = &self.nulls_equal_aware {
            display.push(format!(
                "Null equals Nulls = [{}]",
                null_equals_nulls.iter().map(|b| b.to_string()).join(", ")
            ));
        }
        display
    }

    fn max_probe_concurrency(&self) -> usize {
        common_runtime::get_compute_pool_num_threads()
    }

    fn morsel_size_requirement(&self) -> Option<MorselSizeRequirement> {
        None
    }

    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }

    fn needs_probe_finalization(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Outer | JoinType::Left | JoinType::Right
        ) || (matches!(self.join_type, JoinType::Anti | JoinType::Semi) && self.build_on_left)
    }
}

// Helper methods and types
impl HashJoinOperator {
    fn needs_bitmap(&self) -> bool {
        matches!(self.join_type, JoinType::Outer)
            || (self.join_type == JoinType::Right && !self.build_on_left)
            || (self.join_type == JoinType::Left && self.build_on_left)
            || (matches!(self.join_type, JoinType::Anti | JoinType::Semi) && self.build_on_left)
    }

    fn probe_inner(
        input: &Arc<MicroPartition>,
        probe_state: &Arc<ProbeState>,
        params: &HashJoinProbeParams,
    ) -> DaftResult<Arc<MicroPartition>> {
        // Implementation from InnerHashJoinProbeOperator
        use daft_core::prelude::UInt64Array;
        use daft_recordbatch::{GrowableRecordBatch, get_columns_by_name};

        let build_side_tables = probe_state.get_record_batches().iter().collect::<Vec<_>>();
        const DEFAULT_GROWABLE_SIZE: usize = 20;

        let input_tables = input.record_batches();
        let result_tables = input_tables
            .iter()
            .map(|input_table| {
                let mut build_side_growable = GrowableRecordBatch::new(
                    &build_side_tables,
                    false,
                    DEFAULT_GROWABLE_SIZE,
                )?;
                let mut probe_side_idxs = Vec::new();

                let join_keys = input_table.eval_expression_list(&params.probe_on)?;
                let idx_iter = probe_state.probe_indices(&join_keys)?;
                for (probe_row_idx, inner_iter) in idx_iter.enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_rb_idx, build_row_idx) in inner_iter {
                            build_side_growable.extend(
                                build_rb_idx as usize,
                                build_row_idx as usize,
                                1,
                            );
                            probe_side_idxs.push(probe_row_idx as u64);
                        }
                    }
                }

                let build_side_table = build_side_growable.build()?;
                let probe_side_table = {
                    let indices_arr = UInt64Array::from(("", probe_side_idxs));
                    input_table.take(&indices_arr)?
                };

                let (left_table, right_table) = if params.build_on_left {
                    (build_side_table, probe_side_table)
                } else {
                    (probe_side_table, build_side_table)
                };

                let common_join_keys: Vec<String> = params.common_join_cols.iter().cloned().collect();
                let left_non_join_columns: Vec<String> = params
                    .left_schema
                    .field_names()
                    .filter(|c| !params.common_join_cols.contains(*c))
                    .map(ToString::to_string)
                    .collect();
                let right_non_join_columns: Vec<String> = params
                    .right_schema
                    .field_names()
                    .filter(|c| !params.common_join_cols.contains(*c))
                    .map(ToString::to_string)
                    .collect();

                let join_keys_table = get_columns_by_name(&left_table, &common_join_keys)?;
                let left_non_join_columns =
                    get_columns_by_name(&left_table, &left_non_join_columns)?;
                let right_non_join_columns =
                    get_columns_by_name(&right_table, &right_non_join_columns)?;
                let final_table = join_keys_table
                    .union(&left_non_join_columns)?
                    .union(&right_non_join_columns)?;
                Ok(final_table)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(Arc::new(MicroPartition::new_loaded(
            params.output_schema.clone(),
            Arc::new(result_tables),
            None,
        )))
    }

    fn probe_left_right_with_bitmap(
        input: &Arc<MicroPartition>,
        bitmap_builder: &mut IndexBitmapBuilder,
        probe_state: &Arc<ProbeState>,
        params: &HashJoinProbeParams,
    ) -> DaftResult<Arc<MicroPartition>> {
        use daft_core::prelude::UInt64Array;
        use daft_recordbatch::{GrowableRecordBatch, get_columns_by_name};

        let build_side_tables = probe_state.get_record_batches().iter().collect::<Vec<_>>();

        let final_tables = input
            .record_batches()
            .iter()
            .map(|input_table| {
                let mut build_side_growable = GrowableRecordBatch::new(
                    &build_side_tables,
                    false,
                    build_side_tables.iter().map(|table| table.len()).sum(),
                )?;
                let mut probe_side_idxs = Vec::new();

                let join_keys = input_table.eval_expression_list(&params.probe_on)?;
                let idx_iter = probe_state.probe_indices(&join_keys)?;

                for (probe_row_idx, inner_iter) in idx_iter.enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_table_idx, build_row_idx) in inner_iter {
                            bitmap_builder
                                .mark_used(build_table_idx as usize, build_row_idx as usize);
                            build_side_growable.extend(
                                build_table_idx as usize,
                                build_row_idx as usize,
                                1,
                            );
                            probe_side_idxs.push(probe_row_idx as u64);
                        }
                    }
                }

                let build_side_table = build_side_growable.build()?;
                let probe_side_table = {
                    let indices_arr = UInt64Array::from(("", probe_side_idxs));
                    input_table.take(&indices_arr)?
                };

                let common_join_keys: Vec<String> = params.common_join_cols.iter().cloned().collect();
                let left_non_join_columns: Vec<String> = params
                    .left_schema
                    .field_names()
                    .filter(|c| !params.common_join_cols.contains(*c))
                    .map(ToString::to_string)
                    .collect();
                let right_non_join_columns: Vec<String> = params
                    .right_schema
                    .field_names()
                    .filter(|c| !params.common_join_cols.contains(*c))
                    .map(ToString::to_string)
                    .collect();

                let final_table = if params.join_type == JoinType::Left {
                    let join_table = get_columns_by_name(&build_side_table, &common_join_keys)?;
                    let left = get_columns_by_name(&build_side_table, &left_non_join_columns)?;
                    let right = get_columns_by_name(&probe_side_table, &right_non_join_columns)?;
                    join_table.union(&left)?.union(&right)?
                } else {
                    let join_table = get_columns_by_name(&build_side_table, &common_join_keys)?;
                    let left = get_columns_by_name(&probe_side_table, &left_non_join_columns)?;
                    let right = get_columns_by_name(&build_side_table, &right_non_join_columns)?;
                    join_table.union(&left)?.union(&right)?
                };
                Ok(final_table)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(Arc::new(MicroPartition::new_loaded(
            params.output_schema.clone(),
            Arc::new(final_tables),
            None,
        )))
    }

    fn probe_left_right(
        input: &Arc<MicroPartition>,
        probe_state: &Arc<ProbeState>,
        params: &HashJoinProbeParams,
    ) -> DaftResult<Arc<MicroPartition>> {
        use daft_core::prelude::UInt64Array;
        use daft_recordbatch::{GrowableRecordBatch, get_columns_by_name};

        let build_side_tables = probe_state.get_record_batches().iter().collect::<Vec<_>>();

        let final_tables = input
            .record_batches()
            .iter()
            .map(|input_table| {
                let mut build_side_growable = GrowableRecordBatch::new(
                    &build_side_tables,
                    true,
                    build_side_tables.iter().map(|table| table.len()).sum(),
                )?;
                let mut probe_side_idxs = Vec::with_capacity(input_table.len());

                let join_keys = input_table.eval_expression_list(&params.probe_on)?;
                let idx_iter = probe_state.probe_indices(&join_keys)?;
                for (probe_row_idx, inner_iter) in idx_iter.enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_table_idx, build_row_idx) in inner_iter {
                            build_side_growable.extend(
                                build_table_idx as usize,
                                build_row_idx as usize,
                                1,
                            );
                            probe_side_idxs.push(probe_row_idx as u64);
                        }
                    } else {
                        // if there's no match, we should still emit the probe side and fill the build side with nulls
                        build_side_growable.add_nulls(1);
                        probe_side_idxs.push(probe_row_idx as u64);
                    }
                }

                let build_side_table = build_side_growable.build()?;

                let probe_side_table = {
                    let indices_arr = UInt64Array::from(("", probe_side_idxs));
                    input_table.take(&indices_arr)?
                };

                let common_join_keys: Vec<String> = params.common_join_cols.iter().cloned().collect();
                let left_non_join_columns: Vec<String> = params
                    .left_schema
                    .field_names()
                    .filter(|c| !params.common_join_cols.contains(*c))
                    .map(ToString::to_string)
                    .collect();
                let right_non_join_columns: Vec<String> = params
                    .right_schema
                    .field_names()
                    .filter(|c| !params.common_join_cols.contains(*c))
                    .map(ToString::to_string)
                    .collect();

                let final_table = if params.join_type == JoinType::Left {
                    let join_table = get_columns_by_name(&probe_side_table, &common_join_keys)?;
                    let left = get_columns_by_name(&probe_side_table, &left_non_join_columns)?;
                    let right = get_columns_by_name(&build_side_table, &right_non_join_columns)?;
                    join_table.union(&left)?.union(&right)?
                } else {
                    let join_table = get_columns_by_name(&probe_side_table, &common_join_keys)?;
                    let left = get_columns_by_name(&build_side_table, &left_non_join_columns)?;
                    let right = get_columns_by_name(&probe_side_table, &right_non_join_columns)?;
                    join_table.union(&left)?.union(&right)?
                };
                Ok(final_table)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(Arc::new(MicroPartition::new_loaded(
            params.output_schema.clone(),
            Arc::new(final_tables),
            None,
        )))
    }

    fn probe_outer(
        input: &Arc<MicroPartition>,
        probe_state: &Arc<ProbeState>,
        bitmap_builder: Option<&mut IndexBitmapBuilder>,
        params: &HashJoinProbeParams,
    ) -> DaftResult<Arc<MicroPartition>> {
        use daft_core::prelude::UInt64Array;
        use daft_core::utils::supertype::try_get_supertype;
        use daft_recordbatch::{GrowableRecordBatch, get_columns_by_name};
        use daft_core::prelude::{Field, Schema};

        let bitmap_builder = bitmap_builder.expect("bitmap should be set for outer joins");
        let build_side_tables = probe_state.get_record_batches().iter().collect::<Vec<_>>();
        
        // Compute outer common column schema
        let outer_common_col_fields = params
            .common_join_cols
            .iter()
            .map(|name| {
                let supertype = try_get_supertype(
                    &params.left_schema.get_field(name)?.dtype,
                    &params.right_schema.get_field(name)?.dtype,
                )?;
                Ok(Field::new(name.clone(), supertype))
            })
            .collect::<DaftResult<Vec<_>>>()?;
        let outer_common_col_schema = Arc::new(Schema::new(outer_common_col_fields));

        let final_tables = input
            .record_batches()
            .iter()
            .map(|input_table| {
                let mut build_side_growable = GrowableRecordBatch::new(
                    &build_side_tables,
                    true,
                    build_side_tables.iter().map(|table| table.len()).sum(),
                )?;
                let mut probe_side_idxs = Vec::with_capacity(input_table.len());

                let join_keys = input_table.eval_expression_list(&params.probe_on)?;
                let idx_iter = probe_state.probe_indices(&join_keys)?;

                for (probe_row_idx, inner_iter) in idx_iter.enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_table_idx, build_row_idx) in inner_iter {
                            bitmap_builder
                                .mark_used(build_table_idx as usize, build_row_idx as usize);
                            build_side_growable.extend(
                                build_table_idx as usize,
                                build_row_idx as usize,
                                1,
                            );
                            probe_side_idxs.push(probe_row_idx as u64);
                        }
                    } else {
                        // if there's no match, we should still emit the probe side and fill the build side with nulls
                        build_side_growable.add_nulls(1);
                        probe_side_idxs.push(probe_row_idx as u64);
                    }
                }

                let build_side_table = build_side_growable.build()?;
                let probe_side_table = {
                    let indices_arr = UInt64Array::from(("", probe_side_idxs));
                    input_table.take(&indices_arr)?
                };

                let common_join_keys: Vec<String> = params.common_join_cols.iter().cloned().collect();
                let left_non_join_columns: Vec<String> = params
                    .left_schema
                    .field_names()
                    .filter(|c| !params.common_join_cols.contains(*c))
                    .map(ToString::to_string)
                    .collect();
                let right_non_join_columns: Vec<String> = params
                    .right_schema
                    .field_names()
                    .filter(|c| !params.common_join_cols.contains(*c))
                    .map(ToString::to_string)
                    .collect();

                #[allow(deprecated)]
                let join_table = get_columns_by_name(&probe_side_table, &common_join_keys)?
                    .cast_to_schema(&outer_common_col_schema)?;
                // Get left and right columns based on which side we built on
                let (left, right) = if params.build_on_left {
                    // Built on left, so build_side_table has left columns, probe_side_table has right columns
                    let left = get_columns_by_name(&build_side_table, &left_non_join_columns)?;
                    let right = get_columns_by_name(&probe_side_table, &right_non_join_columns)?;
                    (left, right)
                } else {
                    // Built on right, so build_side_table has right columns, probe_side_table has left columns
                    let left = get_columns_by_name(&probe_side_table, &left_non_join_columns)?;
                    let right = get_columns_by_name(&build_side_table, &right_non_join_columns)?;
                    (left, right)
                };
                let final_table = join_table.union(&left)?.union(&right)?;
                Ok(final_table)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(Arc::new(MicroPartition::new_loaded(
            params.output_schema.clone(),
            Arc::new(final_tables),
            None,
        )))
    }

    fn probe_anti_semi(
        input: &Arc<MicroPartition>,
        probe_state: &Arc<ProbeState>,
        params: &HashJoinProbeParams,
    ) -> DaftResult<Arc<MicroPartition>> {
        use daft_core::prelude::UInt64Array;
        let is_semi = params.join_type == JoinType::Semi;

        let input_tables = input.record_batches();
        let mut input_idxs = vec![vec![]; input_tables.len()];
        for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
            let join_keys = table.eval_expression_list(&params.probe_on)?;
            let iter = probe_state.probe_exists(&join_keys)?;

            for (probe_row_idx, matched) in iter.enumerate() {
                // 1. If this is a semi join, we keep the row if it matches.
                // 2. If this is an anti join, we keep the row if it doesn't match.
                match (is_semi, matched) {
                    (true, true) | (false, false) => {
                        input_idxs[probe_side_table_idx].push(probe_row_idx as u64);
                    }
                    _ => {}
                }
            }
        }
        let probe_side_tables = input_idxs
            .into_iter()
            .zip(input_tables.iter())
            .map(|(idxs, table)| {
                let idxs_arr = UInt64Array::from(("idxs", idxs));
                table.take(&idxs_arr)
            })
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(Arc::new(MicroPartition::new_loaded(
            probe_side_tables[0].schema.clone(),
            Arc::new(probe_side_tables),
            None,
        )))
    }

    fn probe_anti_semi_with_bitmap(
        input: &Arc<MicroPartition>,
        bitmap_builder: &mut IndexBitmapBuilder,
        probe_state: &Arc<ProbeState>,
        params: &HashJoinProbeParams,
    ) -> DaftResult<()> {
        for table in input.record_batches() {
            let join_keys = table.eval_expression_list(&params.probe_on)?;
            let idx_iter = probe_state.probe_indices(&join_keys)?;

            for inner_iter in idx_iter.flatten() {
                for (build_table_idx, build_row_idx) in inner_iter {
                    bitmap_builder.mark_used(build_table_idx as usize, build_row_idx as usize);
                }
            }
        }
        Ok(())
    }

    async fn merge_bitmaps_and_construct_null_table(
        states: Vec<HashJoinProbeState>,
    ) -> DaftResult<RecordBatch> {
        use futures::{StreamExt, stream};

        let mut states_iter = states.into_iter();
        let first_state = states_iter
            .next()
            .expect("at least one state should be present");
        let first_tables = first_state.probe_state.get_record_batches();
        let first_bitmap = first_state
            .bitmap_builder
            .expect("bitmap should be set")
            .build();

        let merged_bitmap = {
            let bitmaps = stream::once(async move { first_bitmap })
                .chain(stream::iter(states_iter).map(|s| {
                    s.bitmap_builder
                        .expect("bitmap should be set")
                        .build()
                }))
                .collect::<Vec<_>>()
                .await;

            bitmaps.into_iter().fold(None, |acc, x| match acc {
                None => Some(x),
                Some(acc) => Some(acc.merge(&x)),
            })
        }
        .expect("at least one bitmap should be present");

        let leftovers = merged_bitmap
            .convert_to_boolean_arrays()
            .zip(first_tables.iter())
            .map(|(bitmap, table)| table.mask_filter(&bitmap.into_series()))
            .collect::<DaftResult<Vec<_>>>()?;
        RecordBatch::concat(&leftovers)
    }

    async fn finalize_outer(
        states: Vec<HashJoinProbeState>,
        params: &HashJoinProbeParams,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        use daft_core::utils::supertype::try_get_supertype;
        use daft_core::prelude::{Field, Schema, Series};
        use daft_recordbatch::get_columns_by_name;

        let build_side_table = Self::merge_bitmaps_and_construct_null_table(states).await?;
        
        // If build_side_table is empty, return empty result with correct schema
        if build_side_table.is_empty() {
            return Ok(Some(Arc::new(MicroPartition::empty(Some(params.output_schema.clone())))));
        }
        
        // Compute outer common column schema
        let outer_common_col_fields = params
            .common_join_cols
            .iter()
            .map(|name| {
                let supertype = try_get_supertype(
                    &params.left_schema.get_field(name)?.dtype,
                    &params.right_schema.get_field(name)?.dtype,
                )?;
                Ok(Field::new(name.clone(), supertype))
            })
            .collect::<DaftResult<Vec<_>>>()?;
        let outer_common_col_schema = Arc::new(Schema::new(outer_common_col_fields));

        let left_non_join_columns: Vec<String> = params
            .left_schema
            .field_names()
            .filter(|c| !params.common_join_cols.contains(*c))
            .map(ToString::to_string)
            .collect();
        let right_non_join_schema = Arc::new(Schema::new(
            params
                .right_schema
                .fields()
                .iter()
                .filter(|f| !params.common_join_cols.contains(&f.name))
                .cloned(),
        ));

        let common_join_cols: Vec<String> = params.common_join_cols.iter().cloned().collect();
        let right_non_join_columns: Vec<String> = params
            .right_schema
            .field_names()
            .filter(|c| !params.common_join_cols.contains(*c))
            .map(ToString::to_string)
            .collect();
        
        // Get columns from build_side_table based on which side we built on
        let (left, right, join_table) = if params.build_on_left {
            // Built on left, so build_side_table has left columns
            // Try to get columns - if they don't exist, return empty
            let left = match get_columns_by_name(&build_side_table, &left_non_join_columns) {
                Ok(cols) => cols,
                Err(_) => {
                    return Ok(Some(Arc::new(MicroPartition::empty(Some(params.output_schema.clone())))));
                }
            };
            let right = {
                let columns = right_non_join_schema
                    .fields()
                    .iter()
                    .map(|field| Series::full_null(&field.name, &field.dtype, left.len()))
                    .collect::<Vec<_>>();
                RecordBatch::new_unchecked(right_non_join_schema.clone(), columns, left.len())
            };
            #[allow(deprecated)]
            let join_table = match get_columns_by_name(&build_side_table, &common_join_cols) {
                Ok(cols) => cols.cast_to_schema(&outer_common_col_schema)?,
                Err(_) => {
                    return Ok(Some(Arc::new(MicroPartition::empty(Some(params.output_schema.clone())))));
                }
            };
            (left, right, join_table)
        } else {
            // Built on right, so build_side_table has right columns
            // The join columns in build_side_table have right side names, but common_join_cols has left side names
            // We need to get the right side join key columns from the build_side_table
            // For now, try to get by common_join_cols (left names), and if that fails, 
            // the build side was likely empty or has different column names
            let right = match get_columns_by_name(&build_side_table, &right_non_join_columns) {
                Ok(cols) => cols,
                Err(_) => {
                    return Ok(Some(Arc::new(MicroPartition::empty(Some(params.output_schema.clone())))));
                }
            };
            let left = {
                let left_non_join_schema = Arc::new(Schema::new(
                    params
                        .left_schema
                        .fields()
                        .iter()
                        .filter(|f| !params.common_join_cols.contains(&f.name))
                        .cloned(),
                ));
                let columns = left_non_join_schema
                    .fields()
                    .iter()
                    .map(|field| Series::full_null(&field.name, &field.dtype, right.len()))
                    .collect::<Vec<_>>();
                RecordBatch::new_unchecked(left_non_join_schema.clone(), columns, right.len())
            };
            // Try to get join columns - if common_join_cols don't exist, the table might be empty or have wrong schema
            #[allow(deprecated)]
            let join_table = match get_columns_by_name(&build_side_table, &common_join_cols) {
                Ok(cols) => cols.cast_to_schema(&outer_common_col_schema)?,
                Err(_) => {
                    // Build side table doesn't have the expected join columns - return empty
                    return Ok(Some(Arc::new(MicroPartition::empty(Some(params.output_schema.clone())))));
                }
            };
            (left, right, join_table)
        };
        let final_table = join_table.union(&left)?.union(&right)?;
        Ok(Some(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        ))))
    }

    async fn finalize_left(
        states: Vec<HashJoinProbeState>,
        params: &HashJoinProbeParams,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        use daft_core::prelude::Series;
        use daft_recordbatch::get_columns_by_name;

        let build_side_table = Self::merge_bitmaps_and_construct_null_table(states).await?;
        
        // If build_side_table is empty, return empty result with correct schema
        if build_side_table.is_empty() {
            return Ok(Some(Arc::new(MicroPartition::empty(Some(params.output_schema.clone())))));
        }
        
        let common_join_cols: Vec<String> = params.common_join_cols.iter().cloned().collect();
        let left_non_join_columns: Vec<String> = params
            .left_schema
            .field_names()
            .filter(|c| !params.common_join_cols.contains(*c))
            .map(ToString::to_string)
            .collect();
        let right_non_join_schema = Arc::new(Schema::new(
            params
                .right_schema
                .fields()
                .iter()
                .filter(|f| !params.common_join_cols.contains(&f.name))
                .cloned(),
        ));

        // For left join, we only finalize when build_on_left is true (needs_bitmap check ensures this)
        // So build_side_table has left columns
        let join_table = get_columns_by_name(&build_side_table, &common_join_cols)?;
        let left = get_columns_by_name(&build_side_table, &left_non_join_columns)?;
        let right = {
            let columns = right_non_join_schema
                .fields()
                .iter()
                .map(|field| Series::full_null(&field.name, &field.dtype, left.len()))
                .collect::<Vec<_>>();
            RecordBatch::new_unchecked(right_non_join_schema.clone(), columns, left.len())
        };
        let final_table = join_table.union(&left)?.union(&right)?;
        Ok(Some(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        ))))
    }

    async fn finalize_right(
        states: Vec<HashJoinProbeState>,
        params: &HashJoinProbeParams,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        use daft_core::prelude::Series;
        use daft_recordbatch::get_columns_by_name;

        let build_side_table = Self::merge_bitmaps_and_construct_null_table(states).await?;
        
        // If build_side_table is empty, return empty result with correct schema
        if build_side_table.is_empty() {
            return Ok(Some(Arc::new(MicroPartition::empty(Some(params.output_schema.clone())))));
        }
        
        let common_join_cols: Vec<String> = params.common_join_cols.iter().cloned().collect();
        let right_non_join_columns: Vec<String> = params
            .right_schema
            .field_names()
            .filter(|c| !params.common_join_cols.contains(*c))
            .map(ToString::to_string)
            .collect();
        let left_non_join_schema = Arc::new(Schema::new(
            params
                .left_schema
                .fields()
                .iter()
                .filter(|f| !params.common_join_cols.contains(&f.name))
                .cloned(),
        ));

        // For right join, we only finalize when build_on_left is false (needs_bitmap check ensures this)
        // So build_side_table has right columns
        let join_table = get_columns_by_name(&build_side_table, &common_join_cols)?;
        let left = {
            let columns = left_non_join_schema
                .fields()
                .iter()
                .map(|field| Series::full_null(&field.name, &field.dtype, build_side_table.len()))
                .collect::<Vec<_>>();
            RecordBatch::new_unchecked(
                left_non_join_schema.clone(),
                columns,
                build_side_table.len(),
            )
        };
        let right = get_columns_by_name(&build_side_table, &right_non_join_columns)?;
        let final_table = join_table.union(&left)?.union(&right)?;
        Ok(Some(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        ))))
    }

    async fn finalize_anti_semi(
        states: Vec<HashJoinProbeState>,
        is_semi: bool,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        use futures::{StreamExt, stream};

        let mut states_iter = states.into_iter();
        let first_state = states_iter
            .next()
            .expect("at least one state should be present");
        let first_tables = first_state.probe_state.get_record_batches();
        let first_bitmap = first_state
            .bitmap_builder
            .expect("bitmap should be set")
            .build();

        let mut merged_bitmap = {
            let bitmaps = stream::once(async move { first_bitmap })
                .chain(stream::iter(states_iter).map(|s| {
                    s.bitmap_builder
                        .expect("bitmap should be set")
                        .build()
                }))
                .collect::<Vec<_>>()
                .await;

            bitmaps.into_iter().fold(None, |acc, x| match acc {
                None => Some(x),
                Some(acc) => Some(acc.merge(&x)),
            })
        }
        .expect("at least one bitmap should be present");

        // The bitmap marks matched rows as 0, so we need to negate it if we are doing semi join, i.e. the matched rows become 1 so that
        // we can we can keep them in the final result.
        if is_semi {
            merged_bitmap = merged_bitmap.negate();
        }

        let leftovers = merged_bitmap
            .convert_to_boolean_arrays()
            .zip(first_tables.iter())
            .map(|(bitmap, table)| table.mask_filter(&bitmap.into_series()))
            .collect::<DaftResult<Vec<_>>>()?;
        let build_side_table = RecordBatch::concat(&leftovers)?;

        Ok(Some(Arc::new(MicroPartition::new_loaded(
            build_side_table.schema.clone(),
            Arc::new(vec![build_side_table]),
            None,
        ))))
    }
}

pub(crate) struct HashJoinProbeState {
    probe_state: Arc<ProbeState>,
    bitmap_builder: Option<IndexBitmapBuilder>,
}

// IndexBitmapBuilder - reuse from outer_hash_join_probe.rs
pub(crate) struct IndexBitmapBuilder {
    mutable_bitmaps: Vec<MutableBitmap>,
}

impl IndexBitmapBuilder {
    pub fn new(tables: &[RecordBatch]) -> Self {
        Self {
            mutable_bitmaps: tables
                .iter()
                .map(|table| MutableBitmap::from_len_set(table.len()))
                .collect(),
        }
    }

    #[inline]
    pub fn mark_used(&mut self, table_idx: usize, row_idx: usize) {
        self.mutable_bitmaps[table_idx].set(row_idx, false);
    }

    pub fn build(self) -> IndexBitmap {
        IndexBitmap {
            bitmaps: self
                .mutable_bitmaps
                .into_iter()
                .map(|bitmap| bitmap.into())
                .collect(),
        }
    }
}

pub(crate) struct IndexBitmap {
    bitmaps: Vec<Bitmap>,
}

impl IndexBitmap {
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            bitmaps: self
                .bitmaps
                .iter()
                .zip(other.bitmaps.iter())
                .map(|(a, b)| a & b)
                .collect(),
        }
    }

    pub fn negate(&self) -> Self {
        Self {
            bitmaps: self.bitmaps.iter().map(|bitmap| !bitmap).collect(),
        }
    }

    pub fn convert_to_boolean_arrays(self) -> impl Iterator<Item = daft_core::prelude::BooleanArray> {
        self.bitmaps
            .into_iter()
            .map(|bitmap| daft_core::prelude::BooleanArray::from(("bitmap", bitmap)))
    }
}

struct HashJoinProbeParams {
    probe_on: Vec<BoundExpr>,
    join_type: JoinType,
    build_on_left: bool,
    common_join_cols: IndexSet<String>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    output_schema: SchemaRef,
}

