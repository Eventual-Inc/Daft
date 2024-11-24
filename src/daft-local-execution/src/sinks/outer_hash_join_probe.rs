use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_core::{
    prelude::{
        bitmap::{and, Bitmap, MutableBitmap},
        BooleanArray, Schema, SchemaRef,
    },
    series::{IntoSeries, Series},
};
use daft_dsl::ExprRef;
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_table::{GrowableTable, ProbeState, Table};
use futures::{stream, StreamExt};
use indexmap::IndexSet;
use tracing::{info_span, instrument};

use super::{
    hash_join_build::ProbeStateBridgeRef,
    streaming_sink::{
        StreamingSink, StreamingSinkExecuteResult, StreamingSinkFinalizeResult,
        StreamingSinkOutput, StreamingSinkState,
    },
};
use crate::{
    dispatcher::{DispatchSpawner, RoundRobinDispatcher, UnorderedDispatcher},
    ExecutionRuntimeContext,
};

struct IndexBitmapBuilder {
    mutable_bitmaps: Vec<MutableBitmap>,
}

impl IndexBitmapBuilder {
    fn new(tables: &[Table]) -> Self {
        Self {
            mutable_bitmaps: tables
                .iter()
                .map(|t| MutableBitmap::from_len_set(t.len()))
                .collect(),
        }
    }

    #[inline]
    fn mark_used(&mut self, table_idx: usize, row_idx: usize) {
        self.mutable_bitmaps[table_idx].set(row_idx, false);
    }

    fn build(self) -> IndexBitmap {
        IndexBitmap {
            bitmaps: self.mutable_bitmaps.into_iter().map(|b| b.into()).collect(),
        }
    }
}

struct IndexBitmap {
    bitmaps: Vec<Bitmap>,
}

impl IndexBitmap {
    fn merge(&self, other: &Self) -> Self {
        Self {
            bitmaps: self
                .bitmaps
                .iter()
                .zip(other.bitmaps.iter())
                .map(|(a, b)| and(a, b))
                .collect(),
        }
    }

    fn convert_to_boolean_arrays(self) -> impl Iterator<Item = BooleanArray> {
        self.bitmaps
            .into_iter()
            .map(|b| BooleanArray::from(("bitmap", b)))
    }
}

enum OuterHashJoinState {
    Building(ProbeStateBridgeRef, bool),
    Probing(Arc<ProbeState>, Option<IndexBitmapBuilder>),
}

impl OuterHashJoinState {
    async fn get_or_build_probe_state(&mut self) -> Arc<ProbeState> {
        match self {
            Self::Building(bridge, needs_bitmap) => {
                let probe_state = bridge.get_probe_state().await;
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
                let probe_state = bridge.get_probe_state().await;
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
    probe_on: Vec<ExprRef>,
    common_join_keys: Vec<String>,
    left_non_join_columns: Vec<String>,
    right_non_join_columns: Vec<String>,
    right_non_join_schema: SchemaRef,
    join_type: JoinType,
}

pub(crate) struct OuterHashJoinProbeSink {
    params: Arc<OuterHashJoinParams>,
    output_schema: SchemaRef,
    probe_state_bridge: ProbeStateBridgeRef,
}

impl OuterHashJoinProbeSink {
    pub(crate) fn new(
        probe_on: Vec<ExprRef>,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        join_type: JoinType,
        common_join_keys: IndexSet<String>,
        output_schema: &SchemaRef,
        probe_state_bridge: ProbeStateBridgeRef,
    ) -> Self {
        let left_non_join_columns = left_schema
            .fields
            .keys()
            .filter(|c| !common_join_keys.contains(*c))
            .cloned()
            .collect();
        let right_non_join_fields = right_schema
            .fields
            .values()
            .filter(|f| !common_join_keys.contains(&f.name))
            .cloned()
            .collect();
        let right_non_join_schema =
            Arc::new(Schema::new(right_non_join_fields).expect("right schema should be valid"));
        let right_non_join_columns = right_non_join_schema.fields.keys().cloned().collect();
        let common_join_keys = common_join_keys.into_iter().collect();
        Self {
            params: Arc::new(OuterHashJoinParams {
                probe_on,
                common_join_keys,
                left_non_join_columns,
                right_non_join_columns,
                right_non_join_schema,
                join_type,
            }),
            output_schema: output_schema.clone(),
            probe_state_bridge,
        }
    }

    fn probe_left_right(
        input: &Arc<MicroPartition>,
        probe_state: &ProbeState,
        join_type: JoinType,
        probe_on: &[ExprRef],
        common_join_keys: &[String],
        left_non_join_columns: &[String],
        right_non_join_columns: &[String],
    ) -> DaftResult<Arc<MicroPartition>> {
        let probe_table = probe_state.get_probeable().clone();
        let tables = probe_state.get_tables().clone();

        let _growables = info_span!("OuterHashJoinProbeSink::build_growables").entered();
        let mut build_side_growable = GrowableTable::new(
            &tables.iter().collect::<Vec<_>>(),
            true,
            tables.iter().map(|t| t.len()).sum(),
        )?;

        let input_tables = input.get_tables()?;
        let mut probe_side_growable =
            GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, input.len())?;

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
            let join_table = probe_side_table.get_columns(common_join_keys)?;
            let left = probe_side_table.get_columns(left_non_join_columns)?;
            let right = build_side_table.get_columns(right_non_join_columns)?;
            join_table.union(&left)?.union(&right)?
        } else {
            let join_table = probe_side_table.get_columns(common_join_keys)?;
            let left = build_side_table.get_columns(left_non_join_columns)?;
            let right = probe_side_table.get_columns(right_non_join_columns)?;
            join_table.union(&left)?.union(&right)?
        };
        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        )))
    }

    fn probe_outer(
        input: &Arc<MicroPartition>,
        probe_state: &ProbeState,
        bitmap_builder: &mut IndexBitmapBuilder,
        probe_on: &[ExprRef],
        common_join_keys: &[String],
        left_non_join_columns: &[String],
        right_non_join_columns: &[String],
    ) -> DaftResult<Arc<MicroPartition>> {
        let probe_table = probe_state.get_probeable().clone();
        let tables = probe_state.get_tables().clone();

        let _growables = info_span!("OuterHashJoinProbeSink::build_growables").entered();
        // Need to set use_validity to true here because we add nulls to the build side
        let mut build_side_growable = GrowableTable::new(
            &tables.iter().collect::<Vec<_>>(),
            true,
            tables.iter().map(|t| t.len()).sum(),
        )?;

        let input_tables = input.get_tables()?;
        let mut probe_side_growable =
            GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, input.len())?;

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

        let join_table = probe_side_table.get_columns(common_join_keys)?;
        let left = build_side_table.get_columns(left_non_join_columns)?;
        let right = probe_side_table.get_columns(right_non_join_columns)?;
        let final_table = join_table.union(&left)?.union(&right)?;
        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        )))
    }

    async fn finalize_outer(
        mut states: Vec<Box<dyn StreamingSinkState>>,
        common_join_keys: &[String],
        left_non_join_columns: &[String],
        right_non_join_schema: &SchemaRef,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
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

        let build_side_table = Table::concat(&leftovers)?;

        let join_table = build_side_table.get_columns(common_join_keys)?;
        let left = build_side_table.get_columns(left_non_join_columns)?;
        let right = {
            let columns = right_non_join_schema
                .fields
                .values()
                .map(|field| Series::full_null(&field.name, &field.dtype, left.len()))
                .collect::<Vec<_>>();
            Table::new_unchecked(right_non_join_schema.clone(), columns, left.len())
        };
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
        runtime_ref: &RuntimeRef,
    ) -> StreamingSinkExecuteResult {
        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
            return Ok((state, StreamingSinkOutput::NeedMoreInput(Some(empty)))).into();
        }

        let params = self.params.clone();
        runtime_ref
            .spawn(async move {
                let outer_join_state = state
                    .as_any_mut()
                    .downcast_mut::<OuterHashJoinState>()
                    .expect("OuterHashJoinProbeSink should have OuterHashJoinProbeState");
                let probe_state = outer_join_state.get_or_build_probe_state().await;
                let out = match params.join_type {
                    JoinType::Left | JoinType::Right => Self::probe_left_right(
                        &input,
                        &probe_state,
                        params.join_type,
                        &params.probe_on,
                        &params.common_join_keys,
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
                            &params.common_join_keys,
                            &params.left_non_join_columns,
                            &params.right_non_join_columns,
                        )
                    }
                    _ => unreachable!(
                        "Only Left, Right, and Outer joins are supported in OuterHashJoinProbeSink"
                    ),
                }?;
                Ok((state, StreamingSinkOutput::NeedMoreInput(Some(out))))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "OuterHashJoinProbeSink"
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(OuterHashJoinState::Building(
            self.probe_state_bridge.clone(),
            self.params.join_type == JoinType::Outer,
        ))
    }

    fn finalize(
        &self,
        states: Vec<Box<dyn StreamingSinkState>>,
        runtime_ref: &RuntimeRef,
    ) -> StreamingSinkFinalizeResult {
        if self.params.join_type == JoinType::Outer {
            let params = self.params.clone();
            runtime_ref
                .spawn(async move {
                    Self::finalize_outer(
                        states,
                        &params.common_join_keys,
                        &params.left_non_join_columns,
                        &params.right_non_join_schema,
                    )
                    .await
                })
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
            Arc::new(RoundRobinDispatcher::new(Some(
                runtime_handle.default_morsel_size(),
            )))
        } else {
            Arc::new(UnorderedDispatcher::new(Some(
                runtime_handle.default_morsel_size(),
            )))
        }
    }
}
