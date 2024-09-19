use std::sync::Arc;

use arrow2::bitmap::MutableBitmap;
use common_error::DaftResult;
use daft_core::{
    prelude::{Schema, SchemaRef},
    series::Series,
};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use daft_table::{GrowableTable, Probeable, Table};
use indexmap::IndexSet;
use tracing::{info_span, instrument};

use super::streaming_sink::{StreamingSink, StreamingSinkOutput, StreamingSinkState};
use crate::pipeline::PipelineResultType;

struct IndexBitmapTracker {
    bitmaps: Vec<MutableBitmap>,
}

impl IndexBitmapTracker {
    fn new(tables: &Arc<Vec<Table>>) -> Self {
        let bitmaps = tables
            .iter()
            .map(|table| MutableBitmap::from_len_zeroed(table.len()))
            .collect();
        Self { bitmaps }
    }

    fn set_true(&mut self, table_idx: usize, row_idx: usize) {
        self.bitmaps[table_idx].set(row_idx, true);
    }

    fn or(&self, other: &Self) -> Self {
        let bitmaps = self
            .bitmaps
            .iter()
            .zip(other.bitmaps.iter())
            .map(|(a, b)| a.or(b))
            .collect();
        Self { bitmaps }
    }

    fn get_unused_indices(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.bitmaps
            .iter()
            .enumerate()
            .flat_map(|(table_idx, bitmap)| {
                bitmap
                    .iter()
                    .enumerate()
                    .filter_map(move |(row_idx, is_set)| {
                        if !is_set {
                            Some((table_idx, row_idx))
                        } else {
                            None
                        }
                    })
            })
    }
}

enum OuterHashJoinProbeState {
    Building,
    ReadyToProbe(
        Arc<dyn Probeable>,
        Arc<Vec<Table>>,
        Option<IndexBitmapTracker>,
    ),
}

impl OuterHashJoinProbeState {
    fn initialize_probe_state(
        &mut self,
        table: &Arc<dyn Probeable>,
        tables: &Arc<Vec<Table>>,
        needs_bitmap: bool,
    ) {
        if let OuterHashJoinProbeState::Building = self {
            *self = OuterHashJoinProbeState::ReadyToProbe(
                table.clone(),
                tables.clone(),
                if needs_bitmap {
                    Some(IndexBitmapTracker::new(tables))
                } else {
                    None
                },
            );
        } else {
            panic!("OuterHashJoinProbeState should only be in Building state when setting table")
        }
    }

    fn get_probeable_and_tables(&self) -> (Arc<dyn Probeable>, Arc<Vec<Table>>) {
        if let OuterHashJoinProbeState::ReadyToProbe(probe_table, tables, _) = self {
            (probe_table.clone(), tables.clone())
        } else {
            panic!("get_probeable_and_table can only be used during the ReadyToProbe Phase")
        }
    }

    fn get_bitmap(&mut self) -> &mut Option<IndexBitmapTracker> {
        if let OuterHashJoinProbeState::ReadyToProbe(_, _, bitmap) = self {
            bitmap
        } else {
            panic!("get_bitmap can only be used during the ReadyToProbe Phase")
        }
    }
}

impl StreamingSinkState for OuterHashJoinProbeState {
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
}

impl OuterHashJoinProbeSink {
    pub(crate) fn new(
        probe_on: Vec<ExprRef>,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        join_type: JoinType,
        common_join_keys: IndexSet<String>,
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
        }
    }

    fn probe_left_right(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut OuterHashJoinProbeState,
    ) -> DaftResult<Arc<MicroPartition>> {
        let (probe_table, tables) = state.get_probeable_and_tables();

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
        let (probe_table, tables) = state.get_probeable_and_tables();
        let bitmap = state.get_bitmap();
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

        let left_idx_used = bitmap.as_mut().expect("bitmap should be set in outer join");

        drop(_growables);
        {
            let _loop = info_span!("OuterHashJoinProbeSink::eval_and_probe").entered();
            for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                let join_keys = table.eval_expression_list(&self.probe_on)?;
                let idx_mapper = probe_table.probe_indices(&join_keys)?;

                for (probe_row_idx, inner_iter) in idx_mapper.make_iter().enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_side_table_idx, build_row_idx) in inner_iter {
                            left_idx_used
                                .set_true(build_side_table_idx as usize, build_row_idx as usize);
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
        mut states: Vec<Box<dyn StreamingSinkState>>,
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
            .get_probeable_and_tables()
            .1;

        let merged_bitmap = {
            let bitmaps = states
                .into_iter()
                .map(|s| {
                    if let OuterHashJoinProbeState::ReadyToProbe(_, _, bitmap) = s {
                        bitmap
                            .take()
                            .expect("bitmap should be present in outer join")
                    } else {
                        panic!("OuterHashJoinProbeState should be in ReadyToProbe state")
                    }
                })
                .collect::<Vec<_>>();
            bitmaps.into_iter().fold(None, |acc, x| match acc {
                None => Some(x),
                Some(acc) => Some(acc.or(&x)),
            })
        }
        .expect("at least one bitmap should be present");

        let mut build_side_growable =
            GrowableTable::new(&tables.iter().collect::<Vec<_>>(), true, 20)?;

        for (table_idx, row_idx) in merged_bitmap.get_unused_indices() {
            build_side_growable.extend(table_idx, row_idx, 1);
        }

        let build_side_table = build_side_growable.build()?;

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
        state: &mut dyn StreamingSinkState,
    ) -> DaftResult<StreamingSinkOutput> {
        match idx {
            0 => {
                let state = state
                    .as_any_mut()
                    .downcast_mut::<OuterHashJoinProbeState>()
                    .expect("OuterHashJoinProbeSink state should be OuterHashJoinProbeState");
                let (probe_table, tables) = input.as_probe_table();
                state.initialize_probe_state(
                    probe_table,
                    tables,
                    self.join_type == JoinType::Outer,
                );
                Ok(StreamingSinkOutput::NeedMoreInput(None))
            }
            _ => {
                let state = state
                    .as_any_mut()
                    .downcast_mut::<OuterHashJoinProbeState>()
                    .expect("OuterHashJoinProbeSink state should be OuterHashJoinProbeState");
                let input = input.as_data();
                let out = match self.join_type {
                    JoinType::Left | JoinType::Right => self.probe_left_right(input, state),
                    JoinType::Outer => self.probe_outer(input, state),
                    _ => unreachable!(
                        "Only Left, Right, and Outer joins are supported in OuterHashJoinProbeSink"
                    ),
                }?;
                Ok(StreamingSinkOutput::NeedMoreInput(Some(out)))
            }
        }
    }

    fn name(&self) -> &'static str {
        "OuterHashJoinProbeSink"
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(OuterHashJoinProbeState::Building)
    }

    fn finalize(
        &self,
        states: Vec<Box<dyn StreamingSinkState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.join_type == JoinType::Outer {
            self.finalize_outer(states)
        } else {
            Ok(None)
        }
    }
}
