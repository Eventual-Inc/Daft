use std::sync::Arc;

use bitmap::{Bitmap, MutableBitmap, and};
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{
    prelude::*,
    series::{IntoSeries, Series},
    utils::supertype::try_get_supertype,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{GrowableRecordBatch, ProbeState, RecordBatch, get_columns_by_name};
use futures::{StreamExt, stream};
use indexmap::IndexSet;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeOutput,
    StreamingSinkFinalizeResult, StreamingSinkOutput,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName, state_bridge::BroadcastStateBridgeRef};

pub(crate) struct IndexBitmapBuilder {
    mutable_bitmaps: Vec<MutableBitmap>,
}

impl IndexBitmapBuilder {
    pub fn new(table: &[RecordBatch]) -> Self {
        Self {
            mutable_bitmaps: table
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
                .map(|(a, b)| and(a, b))
                .collect(),
        }
    }

    pub fn negate(&self) -> Self {
        Self {
            bitmaps: self.bitmaps.iter().map(|bitmap| !bitmap).collect(),
        }
    }

    pub fn convert_to_boolean_arrays(self) -> impl Iterator<Item = BooleanArray> {
        self.bitmaps
            .into_iter()
            .map(|bitmap| BooleanArray::from(("bitmap", bitmap)))
    }
}

pub(crate) enum OuterHashJoinState {
    Building(BroadcastStateBridgeRef<ProbeState>, bool),
    Probing(Arc<ProbeState>, Option<IndexBitmapBuilder>),
}

impl OuterHashJoinState {
    async fn get_or_build_probe_state(&mut self) -> Arc<ProbeState> {
        match self {
            Self::Building(bridge, needs_bitmap) => {
                let probe_state = bridge.get_state().await;
                let builder =
                    needs_bitmap.then(|| IndexBitmapBuilder::new(probe_state.get_record_batches()));
                *self = Self::Probing(probe_state.clone(), builder);
                probe_state
            }
            Self::Probing(probe_state, _) => probe_state.clone(),
        }
    }

    async fn get_or_build_bitmap(&mut self) -> &mut Option<IndexBitmapBuilder> {
        match self {
            Self::Building(bridge, _) => {
                let probe_state = bridge.get_state().await;
                let builder = IndexBitmapBuilder::new(probe_state.get_record_batches());
                *self = Self::Probing(probe_state, Some(builder));
                match self {
                    Self::Probing(_, builder) => builder,
                    _ => unreachable!(),
                }
            }
            Self::Probing(_, builder) => builder,
        }
    }
}

struct OuterHashJoinParams {
    probe_on: Vec<BoundExpr>,
    common_join_cols: Vec<String>,
    left_non_join_columns: Vec<String>,
    right_non_join_columns: Vec<String>,
    outer_common_col_schema: SchemaRef,
    left_non_join_schema: SchemaRef,
    right_non_join_schema: SchemaRef,
    join_type: JoinType,
    build_on_left: bool,
}

pub(crate) struct OuterHashJoinProbeSink {
    params: Arc<OuterHashJoinParams>,
    needs_bitmap: bool,
    output_schema: SchemaRef,
    probe_state_bridge: BroadcastStateBridgeRef<ProbeState>,
}

#[allow(clippy::too_many_arguments)]
impl OuterHashJoinProbeSink {
    pub(crate) fn new(
        probe_on: Vec<BoundExpr>,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        join_type: JoinType,
        build_on_left: bool,
        common_join_cols: IndexSet<String>,
        output_schema: &SchemaRef,
        probe_state_bridge: BroadcastStateBridgeRef<ProbeState>,
    ) -> DaftResult<Self> {
        let needs_bitmap = join_type == JoinType::Outer
            || join_type == JoinType::Right && !build_on_left
            || join_type == JoinType::Left && build_on_left;
        // For outer joins, we need to swap the left and right schemas if we are building on the right.
        let (left_schema, right_schema) = match (join_type, build_on_left) {
            (JoinType::Outer, false) => (right_schema, left_schema),
            _ => (left_schema, right_schema),
        };
        let outer_common_col_fields = common_join_cols
            .iter()
            .map(|name| {
                let supertype = try_get_supertype(
                    &left_schema.get_field(name)?.dtype,
                    &right_schema.get_field(name)?.dtype,
                )?;

                Ok(Field::new(name.clone(), supertype))
            })
            .collect::<DaftResult<Vec<_>>>()?;
        let outer_common_col_schema = Arc::new(Schema::new(outer_common_col_fields));
        let left_non_join_fields = left_schema
            .into_iter()
            .filter(|f| !common_join_cols.contains(&f.name))
            .cloned();
        let left_non_join_schema = Arc::new(Schema::new(left_non_join_fields));
        let left_non_join_columns = left_non_join_schema
            .field_names()
            .map(ToString::to_string)
            .collect();
        let right_non_join_fields = right_schema
            .fields()
            .iter()
            .filter(|f| !common_join_cols.contains(&f.name))
            .cloned();
        let right_non_join_schema = Arc::new(Schema::new(right_non_join_fields));
        let right_non_join_columns = right_non_join_schema
            .field_names()
            .map(ToString::to_string)
            .collect();
        let common_join_cols = common_join_cols.into_iter().collect();
        Ok(Self {
            params: Arc::new(OuterHashJoinParams {
                probe_on,
                common_join_cols,
                left_non_join_columns,
                right_non_join_columns,
                outer_common_col_schema,
                left_non_join_schema,
                right_non_join_schema,
                join_type,
                build_on_left,
            }),
            needs_bitmap,
            output_schema: output_schema.clone(),
            probe_state_bridge,
        })
    }

    fn probe_left_right_with_bitmap(
        input: &Arc<MicroPartition>,
        bitmap_builder: &mut IndexBitmapBuilder,
        probe_state: &ProbeState,
        join_type: JoinType,
        probe_on: &[BoundExpr],
        common_join_cols: &[String],
        left_non_join_columns: &[String],
        right_non_join_columns: &[String],
        output_schema: &SchemaRef,
    ) -> DaftResult<Arc<MicroPartition>> {
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

                let join_keys = input_table.eval_expression_list(probe_on)?;
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

                let final_table = if join_type == JoinType::Left {
                    let join_table = get_columns_by_name(&build_side_table, common_join_cols)?;
                    let left = get_columns_by_name(&build_side_table, left_non_join_columns)?;
                    let right = get_columns_by_name(&probe_side_table, right_non_join_columns)?;
                    join_table.union(&left)?.union(&right)?
                } else {
                    let join_table = get_columns_by_name(&build_side_table, common_join_cols)?;
                    let left = get_columns_by_name(&probe_side_table, left_non_join_columns)?;
                    let right = get_columns_by_name(&build_side_table, right_non_join_columns)?;
                    join_table.union(&left)?.union(&right)?
                };
                Ok(final_table)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(Arc::new(MicroPartition::new_loaded(
            output_schema.clone(),
            Arc::new(final_tables),
            None,
        )))
    }

    fn probe_left_right(
        input: &Arc<MicroPartition>,
        probe_state: &ProbeState,
        join_type: JoinType,
        probe_on: &[BoundExpr],
        common_join_cols: &[String],
        left_non_join_columns: &[String],
        right_non_join_columns: &[String],
        output_schema: &SchemaRef,
    ) -> DaftResult<Arc<MicroPartition>> {
        let build_side_tables = probe_state.get_record_batches().iter().collect::<Vec<_>>();

        let final_tables = input
            .record_batches()
            .iter()
            .map(|input_table| {
                // We can instantiate with capacity for left/right probes since we will always push even if there's no match.
                let mut build_side_growable = GrowableRecordBatch::new(
                    &build_side_tables,
                    true,
                    build_side_tables.iter().map(|table| table.len()).sum(),
                )?;
                let mut probe_side_idxs = Vec::with_capacity(input_table.len());

                let join_keys = input_table.eval_expression_list(probe_on)?;
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

                let final_table = if join_type == JoinType::Left {
                    let join_table = get_columns_by_name(&probe_side_table, common_join_cols)?;
                    let left = get_columns_by_name(&probe_side_table, left_non_join_columns)?;
                    let right = get_columns_by_name(&build_side_table, right_non_join_columns)?;
                    join_table.union(&left)?.union(&right)?
                } else {
                    let join_table = get_columns_by_name(&probe_side_table, common_join_cols)?;
                    let left = get_columns_by_name(&build_side_table, left_non_join_columns)?;
                    let right = get_columns_by_name(&probe_side_table, right_non_join_columns)?;
                    join_table.union(&left)?.union(&right)?
                };
                Ok(final_table)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(Arc::new(MicroPartition::new_loaded(
            output_schema.clone(),
            Arc::new(final_tables),
            None,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    fn probe_outer(
        input: &Arc<MicroPartition>,
        probe_state: &ProbeState,
        bitmap_builder: &mut IndexBitmapBuilder,
        probe_on: &[BoundExpr],
        common_join_cols: &[String],
        outer_common_col_schema: &SchemaRef,
        left_non_join_columns: &[String],
        right_non_join_columns: &[String],
        build_on_left: bool,
        output_schema: &SchemaRef,
    ) -> DaftResult<Arc<MicroPartition>> {
        let build_side_tables = probe_state.get_record_batches().iter().collect::<Vec<_>>();
        let final_tables = input
            .record_batches()
            .iter()
            .map(|input_table| {
                // We can instantiate with capacity for outer probes since we will always push even if there's no match.
                let mut build_side_growable = GrowableRecordBatch::new(
                    &build_side_tables,
                    true,
                    build_side_tables.iter().map(|table| table.len()).sum(),
                )?;
                let mut probe_side_idxs = Vec::with_capacity(input_table.len());

                let join_keys = input_table.eval_expression_list(probe_on)?;
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

                #[allow(deprecated)]
                let join_table = get_columns_by_name(&probe_side_table, common_join_cols)?
                    .cast_to_schema(outer_common_col_schema)?;
                let left = get_columns_by_name(&build_side_table, left_non_join_columns)?;
                let right = get_columns_by_name(&probe_side_table, right_non_join_columns)?;
                // If we built the probe table on the right, flip the order of union.
                let (left, right) = if build_on_left {
                    (left, right)
                } else {
                    (right, left)
                };
                let final_table = join_table.union(&left)?.union(&right)?;
                Ok(final_table)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(Arc::new(MicroPartition::new_loaded(
            output_schema.clone(),
            Arc::new(final_tables),
            None,
        )))
    }

    async fn merge_bitmaps_and_construct_null_table(
        states: Vec<OuterHashJoinState>,
    ) -> DaftResult<RecordBatch> {
        let mut states_iter = states.into_iter();
        let mut first_state = states_iter
            .next()
            .expect("at least one state should be present");
        let first_probe_state = first_state.get_or_build_probe_state().await;
        let first_tables = first_probe_state.get_record_batches();
        let first_bitmap = first_state
            .get_or_build_bitmap()
            .await
            .take()
            .expect("bitmap should be set")
            .build();

        let merged_bitmap = {
            let bitmaps = stream::once(async move { first_bitmap })
                .chain(stream::iter(states_iter).then(|mut s| async move {
                    s.get_or_build_bitmap()
                        .await
                        .take()
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
        states: Vec<OuterHashJoinState>,
        common_join_cols: &[String],
        outer_common_col_schema: &SchemaRef,
        left_non_join_columns: &[String],
        right_non_join_schema: &SchemaRef,
        build_on_left: bool,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let build_side_table = Self::merge_bitmaps_and_construct_null_table(states).await?;
        #[allow(deprecated)]
        let join_table = get_columns_by_name(&build_side_table, common_join_cols)?
            .cast_to_schema(outer_common_col_schema)?;
        let left = get_columns_by_name(&build_side_table, left_non_join_columns)?;
        let right = {
            let columns = right_non_join_schema
                .fields()
                .iter()
                .map(|field| Series::full_null(&field.name, &field.dtype, left.len()))
                .collect::<Vec<_>>();
            RecordBatch::new_unchecked(right_non_join_schema.clone(), columns, left.len())
        };
        // If we built the probe table on the right, flip the order of union.
        let (left, right) = if build_on_left {
            (left, right)
        } else {
            (right, left)
        };
        let final_table = join_table.union(&left)?.union(&right)?;
        Ok(Some(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        ))))
    }

    async fn finalize_left(
        states: Vec<OuterHashJoinState>,
        common_join_cols: &[String],
        left_non_join_columns: &[String],
        right_non_join_schema: &SchemaRef,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let build_side_table = Self::merge_bitmaps_and_construct_null_table(states).await?;
        let join_table = get_columns_by_name(&build_side_table, common_join_cols)?;
        let left = get_columns_by_name(&build_side_table, left_non_join_columns)?;
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
        states: Vec<OuterHashJoinState>,
        common_join_cols: &[String],
        right_non_join_columns: &[String],
        left_non_join_schema: &SchemaRef,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let build_side_table = Self::merge_bitmaps_and_construct_null_table(states).await?;
        let join_table = get_columns_by_name(&build_side_table, common_join_cols)?;
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
        let right = get_columns_by_name(&build_side_table, right_non_join_columns)?;
        let final_table = join_table.union(&left)?.union(&right)?;
        Ok(Some(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        ))))
    }
}

impl StreamingSink for OuterHashJoinProbeSink {
    type State = OuterHashJoinState;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    #[instrument(skip_all, name = "OuterHashJoinProbeSink::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
            return Ok((state, StreamingSinkOutput::NeedMoreInput(Some(empty)))).into();
        }

        let needs_bitmap = self.needs_bitmap;
        let params = self.params.clone();
        let output_schema = self.output_schema.clone();
        spawner
            .spawn(
                async move {
                    let probe_state = state.get_or_build_probe_state().await;

                    let out = match params.join_type {
                        JoinType::Left | JoinType::Right if needs_bitmap => {
                            Self::probe_left_right_with_bitmap(
                                &input,
                                state
                                    .get_or_build_bitmap()
                                    .await
                                    .as_mut()
                                    .expect("bitmap should be set"),
                                &probe_state,
                                params.join_type,
                                &params.probe_on,
                                &params.common_join_cols,
                                &params.left_non_join_columns,
                                &params.right_non_join_columns,
                                &output_schema,
                            )
                        }
                        JoinType::Left | JoinType::Right => Self::probe_left_right(
                            &input,
                            &probe_state,
                            params.join_type,
                            &params.probe_on,
                            &params.common_join_cols,
                            &params.left_non_join_columns,
                            &params.right_non_join_columns,
                            &output_schema,
                        ),
                        JoinType::Outer => {
                            let bitmap_builder = state
                                .get_or_build_bitmap()
                                .await
                                .as_mut()
                                .expect("bitmap should be set");
                            Self::probe_outer(
                                &input,
                                &probe_state,
                                bitmap_builder,
                                &params.probe_on,
                                &params.common_join_cols,
                                &params.outer_common_col_schema,
                                &params.left_non_join_columns,
                                &params.right_non_join_columns,
                                params.build_on_left,
                                &output_schema,
                            )
                        }
                        _ => unreachable!(
                        "Only Left, Right, and Outer joins are supported in OuterHashJoinProbeSink"
                    ),
                    }?;
                    Ok((state, StreamingSinkOutput::NeedMoreInput(Some(out))))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        match self.params.join_type {
            JoinType::Left => "Left Hash Join Probe".into(),
            JoinType::Right => "Right Hash Join Probe".into(),
            JoinType::Outer => "Outer Hash Join Probe".into(),
            _ => unreachable!(
                "Only Left, Right, and Outer joins are supported in OuterHashJoinProbeSink"
            ),
        }
    }

    fn op_type(&self) -> NodeType {
        NodeType::OuterHashJoinProbe
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        match self.params.join_type {
            JoinType::Left => res.push("LeftHashJoinProbe:".to_string()),
            JoinType::Right => res.push("RightHashJoinProbe:".to_string()),
            JoinType::Outer => res.push("OuterHashJoinProbe:".to_string()),
            _ => unreachable!(
                "Only Left, Right, and Outer joins are supported in OuterHashJoinProbeSink"
            ),
        }
        res.push(format!(
            "Probe on: [{}]",
            self.params
                .probe_on
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        res.push(format!("Build on left: {}", self.params.build_on_left));
        res
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(OuterHashJoinState::Building(
            self.probe_state_bridge.clone(),
            self.needs_bitmap,
        ))
    }

    #[instrument(skip_all, name = "OuterHashJoinProbeSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        if self.needs_bitmap {
            let params = self.params.clone();
            spawner
                .spawn(
                    async move {
                        let res = match params.join_type {
                            JoinType::Left => Self::finalize_left(
                                states,
                                &params.common_join_cols,
                                &params.left_non_join_columns,
                                &params.right_non_join_schema,
                            )
                            .await,
                            JoinType::Right => Self::finalize_right(
                                states,
                                &params.common_join_cols,
                                &params.right_non_join_columns,
                                &params.left_non_join_schema,
                            )
                            .await,
                            JoinType::Outer => Self::finalize_outer(
                                    states,
                                    &params.common_join_cols,
                                    &params.outer_common_col_schema,
                                    &params.left_non_join_columns,
                                    &params.right_non_join_schema,
                                    params.build_on_left,
                                )
                                .await,
                            _ => unreachable!(
                                "Only Left, Right, and Outer joins are supported in OuterHashJoinProbeSink"
                            ),
                        }?;
                    Ok(StreamingSinkFinalizeOutput::Finished(res))
                    },
                    Span::current(),
                )
                .into()
        } else {
            Ok(StreamingSinkFinalizeOutput::Finished(None)).into()
        }
    }

    fn max_concurrency(&self) -> usize {
        common_runtime::get_compute_pool_num_threads()
    }
    fn batching_strategy(&self) -> Self::BatchingStrategy {
        crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        )
    }
}
