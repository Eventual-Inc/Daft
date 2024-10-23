use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{
    prelude::{
        bitmap::{and, Bitmap, MutableBitmap},
        BooleanArray, Schema, SchemaRef,
    },
    series::{IntoSeries, Series},
};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use daft_table::{GrowableTable, ProbeState, Table};
use indexmap::IndexSet;
use tracing::{info_span, instrument};

use super::streaming_sink::{
    DynStreamingSinkState, StreamingSink, StreamingSinkOutput, StreamingSinkState,
};
use crate::pipeline::PipelineResultType;

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

enum OuterHashJoinProbeState {
    Building,
    ReadyToProbe(Arc<ProbeState>, Option<IndexBitmapBuilder>),
}

impl OuterHashJoinProbeState {
    fn initialize_probe_state(&mut self, probe_state: Arc<ProbeState>, needs_bitmap: bool) {
        let tables = probe_state.get_tables();
        if matches!(self, Self::Building) {
            *self = Self::ReadyToProbe(
                probe_state.clone(),
                if needs_bitmap {
                    Some(IndexBitmapBuilder::new(tables))
                } else {
                    None
                },
            );
        } else {
            panic!("OuterHashJoinProbeState should only be in Building state when setting table")
        }
    }

    fn get_probe_state(&self) -> &ProbeState {
        if let Self::ReadyToProbe(probe_state, _) = self {
            probe_state
        } else {
            panic!("get_probeable_and_table can only be used during the ReadyToProbe Phase")
        }
    }

    fn get_bitmap_builder(&mut self) -> &mut Option<IndexBitmapBuilder> {
        if let Self::ReadyToProbe(_, bitmap_builder) = self {
            bitmap_builder
        } else {
            panic!("get_bitmap can only be used during the ReadyToProbe Phase")
        }
    }
}

impl DynStreamingSinkState for OuterHashJoinProbeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub(crate) struct OuterHashJoinProbeSink {
    probe_on: Vec<ExprRef>,
    common_join_keys: Vec<String>,
    left_non_join_columns: Vec<String>,
    right_non_join_columns: Vec<String>,
    right_non_join_schema: SchemaRef,
    join_type: JoinType,
    output_schema: SchemaRef,
}

impl OuterHashJoinProbeSink {
    pub(crate) fn new(
        probe_on: Vec<ExprRef>,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        join_type: JoinType,
        common_join_keys: IndexSet<String>,
        output_schema: &SchemaRef,
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
            probe_on,
            common_join_keys,
            left_non_join_columns,
            right_non_join_columns,
            right_non_join_schema,
            join_type,
            output_schema: output_schema.clone(),
        }
    }

    fn probe_left_right(
        &self,
        input: &Arc<MicroPartition>,
        state: &OuterHashJoinProbeState,
    ) -> DaftResult<Arc<MicroPartition>> {
        let (probe_table, tables) = {
            let probe_state = state.get_probe_state();
            (probe_state.get_probeable(), probe_state.get_tables())
        };

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
                let join_keys = table.eval_expression_list(&self.probe_on)?;
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

        let final_table = if self.join_type == JoinType::Left {
            let join_table = probe_side_table.get_columns(&self.common_join_keys)?;
            let left = probe_side_table.get_columns(&self.left_non_join_columns)?;
            let right = build_side_table.get_columns(&self.right_non_join_columns)?;
            join_table.union(&left)?.union(&right)?
        } else {
            let join_table = probe_side_table.get_columns(&self.common_join_keys)?;
            let left = build_side_table.get_columns(&self.left_non_join_columns)?;
            let right = probe_side_table.get_columns(&self.right_non_join_columns)?;
            join_table.union(&left)?.union(&right)?
        };
        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        )))
    }

    fn probe_outer(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut OuterHashJoinProbeState,
    ) -> DaftResult<Arc<MicroPartition>> {
        let (probe_table, tables) = {
            let probe_state = state.get_probe_state();
            (
                probe_state.get_probeable().clone(),
                probe_state.get_tables().clone(),
            )
        };
        let bitmap_builder = state.get_bitmap_builder();
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

        let left_idx_used = bitmap_builder
            .as_mut()
            .expect("bitmap should be set in outer join");

        drop(_growables);
        {
            let _loop = info_span!("OuterHashJoinProbeSink::eval_and_probe").entered();
            for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                let join_keys = table.eval_expression_list(&self.probe_on)?;
                let idx_mapper = probe_table.probe_indices(&join_keys)?;

                for (probe_row_idx, inner_iter) in idx_mapper.make_iter().enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_side_table_idx, build_row_idx) in inner_iter {
                            let build_side_table_idx = build_side_table_idx as usize;
                            let build_row_idx = build_row_idx as usize;
                            left_idx_used.mark_used(build_side_table_idx, build_row_idx);
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

        let join_table = probe_side_table.get_columns(&self.common_join_keys)?;
        let left = build_side_table.get_columns(&self.left_non_join_columns)?;
        let right = probe_side_table.get_columns(&self.right_non_join_columns)?;
        let final_table = join_table.union(&left)?.union(&right)?;
        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        )))
    }

    fn finalize_outer(
        &self,
        mut states: Vec<Box<dyn DynStreamingSinkState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let states = states
            .iter_mut()
            .map(|s| {
                s.as_any_mut()
                    .downcast_mut::<OuterHashJoinProbeState>()
                    .expect("OuterHashJoinProbeSink state should be OuterHashJoinProbeState")
            })
            .collect::<Vec<_>>();
        let tables = states
            .first()
            .expect("at least one state should be present")
            .get_probe_state()
            .get_tables()
            .clone();

        let merged_bitmap = {
            let bitmaps = states.into_iter().map(|s| {
                if let OuterHashJoinProbeState::ReadyToProbe(_, bitmap) = s {
                    bitmap
                        .take()
                        .expect("bitmap should be present in outer join")
                        .build()
                } else {
                    panic!("OuterHashJoinProbeState should be in ReadyToProbe state")
                }
            });
            bitmaps.fold(None, |acc, x| match acc {
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

        let join_table = build_side_table.get_columns(&self.common_join_keys)?;
        let left = build_side_table.get_columns(&self.left_non_join_columns)?;
        let right = {
            let columns = self
                .right_non_join_schema
                .fields
                .values()
                .map(|field| Series::full_null(&field.name, &field.dtype, left.len()))
                .collect::<Vec<_>>();
            Table::new_unchecked(self.right_non_join_schema.clone(), columns, left.len())
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
        idx: usize,
        input: &PipelineResultType,
        state_handle: &StreamingSinkState,
    ) -> DaftResult<StreamingSinkOutput> {
        match idx {
            0 => {
                state_handle.with_state_mut::<OuterHashJoinProbeState, _, _>(|state| {
                    state.initialize_probe_state(
                        input.as_probe_state().clone(),
                        self.join_type == JoinType::Outer,
                    );
                });
                Ok(StreamingSinkOutput::NeedMoreInput(None))
            }
            _ => state_handle.with_state_mut::<OuterHashJoinProbeState, _, _>(|state| {
                let input = input.as_data();
                if input.is_empty() {
                    let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
                    return Ok(StreamingSinkOutput::NeedMoreInput(Some(empty)));
                }
                let out = match self.join_type {
                    JoinType::Left | JoinType::Right => self.probe_left_right(input, state),
                    JoinType::Outer => self.probe_outer(input, state),
                    _ => unreachable!(
                        "Only Left, Right, and Outer joins are supported in OuterHashJoinProbeSink"
                    ),
                }?;
                Ok(StreamingSinkOutput::NeedMoreInput(Some(out)))
            }),
        }
    }

    fn name(&self) -> &'static str {
        "OuterHashJoinProbeSink"
    }

    fn make_state(&self) -> Box<dyn DynStreamingSinkState> {
        Box::new(OuterHashJoinProbeState::Building)
    }

    fn finalize(
        &self,
        states: Vec<Box<dyn DynStreamingSinkState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.join_type == JoinType::Outer {
            self.finalize_outer(states)
        } else {
            Ok(None)
        }
    }
}
