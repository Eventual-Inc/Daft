use std::sync::Arc;

use bitmap::{and, Bitmap, MutableBitmap};
use common_error::DaftResult;
use daft_core::{
    prelude::*,
    series::{IntoSeries, Series},
    utils::supertype::try_get_supertype,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{get_columns_by_name, GrowableRecordBatch, ProbeState, RecordBatch};
use futures::{stream, StreamExt};
use indexmap::IndexSet;
use itertools::Itertools;
use tracing::{info_span, instrument, Span};

use super::base::{
    StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult, StreamingSinkOutput,
    StreamingSinkState,
};
use crate::{
    dispatcher::{DispatchSpawner, RoundRobinDispatcher, UnorderedDispatcher},
    state_bridge::BroadcastStateBridgeRef,
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

pub(crate) struct IndexBitmapBuilder {
    mutable_bitmaps: Vec<MutableBitmap>,
}

impl IndexBitmapBuilder {
    pub fn new(tables: &[RecordBatch]) -> Self {
        Self {
            mutable_bitmaps: tables
                .iter()
                .map(|t| MutableBitmap::from_len_set(t.len()))
                .collect(),
        }
    }

    #[inline]
    pub fn mark_used(&mut self, table_idx: usize, row_idx: usize) {
        self.mutable_bitmaps[table_idx].set(row_idx, false);
    }

    pub fn build(self) -> IndexBitmap {
        IndexBitmap {
            bitmaps: self.mutable_bitmaps.into_iter().map(|b| b.into()).collect(),
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
            bitmaps: self.bitmaps.iter().map(|b| !b).collect(),
        }
    }

    pub fn convert_to_boolean_arrays(self) -> impl Iterator<Item = BooleanArray> {
        self.bitmaps
            .into_iter()
            .map(|b| BooleanArray::from(("bitmap", b)))
    }
}

enum OuterHashJoinState {
    Building(BroadcastStateBridgeRef<ProbeState>, bool),
    Probing(Arc<ProbeState>, Option<IndexBitmapBuilder>),
}

impl OuterHashJoinState {
    async fn get_or_build_probe_state(&mut self) -> Arc<ProbeState> {
        match self {
            Self::Building(bridge, needs_bitmap) => {
                let probe_state = bridge.get_state().await;
                let builder =
                    needs_bitmap.then(|| IndexBitmapBuilder::new(probe_state.get_tables()));
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
                let builder = IndexBitmapBuilder::new(probe_state.get_tables());
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

impl StreamingSinkState for OuterHashJoinState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
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
    ) -> DaftResult<Arc<MicroPartition>> {
        let probe_table = probe_state.get_probeable();
        let tables = probe_state.get_tables();

        let _growables = info_span!("OuterHashJoinProbeSink::build_growables").entered();
        let mut build_side_growable = GrowableRecordBatch::new(
            &tables.iter().collect::<Vec<_>>(),
            false,
            tables.iter().map(|t| t.len()).sum(),
        )?;

        let input_tables = input.get_tables()?;
        let mut probe_side_growable =
            GrowableRecordBatch::new(&input_tables.iter().collect::<Vec<_>>(), false, input.len())?;

        drop(_growables);
        {
            let _loop = info_span!("OuterHashJoinProbeSink::eval_and_probe").entered();
            for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                let join_keys = table.eval_expression_list(probe_on)?;
                let idx_mapper = probe_table.probe_indices(&join_keys)?;

                for (probe_row_idx, inner_iter) in idx_mapper.make_iter().enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_side_table_idx, build_row_idx) in inner_iter {
                            bitmap_builder
                                .mark_used(build_side_table_idx as usize, build_row_idx as usize);
                            build_side_growable.extend(
                                build_side_table_idx as usize,
                                build_row_idx as usize,
                                1,
                            );
                            probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                        }
                    }
                }
            }
        }
        let build_side_table = build_side_growable.build()?;
        let probe_side_table = probe_side_growable.build()?;

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
        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
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
    ) -> DaftResult<Arc<MicroPartition>> {
        let probe_table = probe_state.get_probeable();
        let tables = probe_state.get_tables();

        let _growables = info_span!("OuterHashJoinProbeSink::build_growables").entered();
        let mut build_side_growable = GrowableRecordBatch::new(
            &tables.iter().collect::<Vec<_>>(),
            true,
            tables.iter().map(|t| t.len()).sum(),
        )?;

        let input_tables = input.get_tables()?;
        let mut probe_side_growable =
            GrowableRecordBatch::new(&input_tables.iter().collect::<Vec<_>>(), false, input.len())?;

        drop(_growables);
        {
            let _loop = info_span!("OuterHashJoinProbeSink::eval_and_probe").entered();
            for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                let join_keys = table.eval_expression_list(probe_on)?;
                let idx_mapper = probe_table.probe_indices(&join_keys)?;

                for (probe_row_idx, inner_iter) in idx_mapper.make_iter().enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_side_table_idx, build_row_idx) in inner_iter {
                            build_side_growable.extend(
                                build_side_table_idx as usize,
                                build_row_idx as usize,
                                1,
                            );
                            probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                        }
                    } else {
                        // if there's no match, we should still emit the probe side and fill the build side with nulls
                        build_side_growable.add_nulls(1);
                        probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                    }
                }
            }
        }
        let build_side_table = build_side_growable.build()?;
        let probe_side_table = probe_side_growable.build()?;

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
        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
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
    ) -> DaftResult<Arc<MicroPartition>> {
        let probe_table = probe_state.get_probeable();
        let tables = probe_state.get_tables();

        let _growables = info_span!("OuterHashJoinProbeSink::build_growables").entered();
        // Need to set use_validity to true here because we add nulls to the build side
        let mut build_side_growable = GrowableRecordBatch::new(
            &tables.iter().collect::<Vec<_>>(),
            true,
            tables.iter().map(|t| t.len()).sum(),
        )?;

        let input_tables = input.get_tables()?;
        let mut probe_side_growable =
            GrowableRecordBatch::new(&input_tables.iter().collect::<Vec<_>>(), false, input.len())?;

        drop(_growables);
        {
            let _loop = info_span!("OuterHashJoinProbeSink::eval_and_probe").entered();
            for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                let join_keys = table.eval_expression_list(probe_on)?;
                let idx_mapper = probe_table.probe_indices(&join_keys)?;

                for (probe_row_idx, inner_iter) in idx_mapper.make_iter().enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_side_table_idx, build_row_idx) in inner_iter {
                            let build_side_table_idx = build_side_table_idx as usize;
                            let build_row_idx = build_row_idx as usize;
                            bitmap_builder.mark_used(build_side_table_idx, build_row_idx);
                            build_side_growable.extend(build_side_table_idx, build_row_idx, 1);
                            probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                        }
                    } else {
                        // if there's no match, we should still emit the probe side and fill the build side with nulls
                        build_side_growable.add_nulls(1);
                        probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                    }
                }
            }
        }
        let build_side_table = build_side_growable.build()?;
        let probe_side_table = probe_side_growable.build()?;

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
        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        )))
    }

    async fn merge_bitmaps_and_construct_null_table(
        mut states: Vec<Box<dyn StreamingSinkState>>,
    ) -> DaftResult<RecordBatch> {
        let mut states_iter = states.iter_mut();
        let first_state = states_iter
            .next()
            .expect("at least one state should be present")
            .as_any_mut()
            .downcast_mut::<OuterHashJoinState>()
            .expect("OuterHashJoinProbeSink state should be OuterHashJoinProbeState");
        let tables = first_state
            .get_or_build_probe_state()
            .await
            .get_tables()
            .clone();
        let first_bitmap = first_state
            .get_or_build_bitmap()
            .await
            .take()
            .expect("bitmap should be set")
            .build();

        let merged_bitmap = {
            let bitmaps = stream::once(async move { first_bitmap })
                .chain(stream::iter(states_iter).then(|s| async move {
                    let state = s
                        .as_any_mut()
                        .downcast_mut::<OuterHashJoinState>()
                        .expect("OuterHashJoinProbeSink state should be OuterHashJoinProbeState");
                    state
                        .get_or_build_bitmap()
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
            .zip(tables.iter())
            .map(|(bitmap, table)| table.mask_filter(&bitmap.into_series()))
            .collect::<DaftResult<Vec<_>>>()?;

        RecordBatch::concat(&leftovers)
    }

    async fn finalize_outer(
        states: Vec<Box<dyn StreamingSinkState>>,
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
        states: Vec<Box<dyn StreamingSinkState>>,
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
        states: Vec<Box<dyn StreamingSinkState>>,
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
    #[instrument(skip_all, name = "OuterHashJoinProbeSink::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult {
        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
            return Ok((state, StreamingSinkOutput::NeedMoreInput(Some(empty)))).into();
        }

        let needs_bitmap = self.needs_bitmap;
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    let outer_join_state = state
                        .as_any_mut()
                        .downcast_mut::<OuterHashJoinState>()
                        .expect("OuterHashJoinProbeSink should have OuterHashJoinProbeState");
                    let probe_state = outer_join_state.get_or_build_probe_state().await;

                    let out = match params.join_type {
                        JoinType::Left | JoinType::Right if needs_bitmap => {
                            Self::probe_left_right_with_bitmap(
                                &input,
                                outer_join_state
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
                        ),
                        JoinType::Outer => {
                            let bitmap_builder = outer_join_state
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

    fn name(&self) -> &'static str {
        "OuterHashJoinProbe"
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

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(OuterHashJoinState::Building(
            self.probe_state_bridge.clone(),
            self.needs_bitmap,
        ))
    }

    #[instrument(skip_all, name = "OuterHashJoinProbeSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn StreamingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult {
        if self.needs_bitmap {
            let params = self.params.clone();
            spawner
                .spawn(
                    async move {
                        match params.join_type {
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
                    }
                    },
                    Span::current(),
                )
                .into()
        } else {
            Ok(None).into()
        }
    }

    fn dispatch_spawner(
        &self,
        runtime_handle: &ExecutionRuntimeContext,
        maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        if maintain_order {
            Arc::new(RoundRobinDispatcher::with_fixed_threshold(
                runtime_handle.default_morsel_size(),
            ))
        } else {
            Arc::new(UnorderedDispatcher::with_fixed_threshold(
                runtime_handle.default_morsel_size(),
            ))
        }
    }
}
