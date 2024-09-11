use std::sync::Arc;

use arrow2::bitmap::MutableBitmap;
use common_error::DaftResult;
use daft_core::{
    prelude::{Field, Schema, SchemaRef},
    series::Series,
};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use daft_table::{GrowableTable, Probeable, Table};
use indexmap::IndexSet;
use tracing::{info_span, instrument};

use crate::pipeline::PipelineResultType;

use super::{IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState};

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

enum HashJoinProbeState {
    Building,
    ReadyToProbe(
        Arc<dyn Probeable>,
        Arc<Vec<Table>>,
        Option<IndexBitmapTracker>,
    ),
}

impl HashJoinProbeState {
    fn initialize_probe_state(
        &mut self,
        table: &Arc<dyn Probeable>,
        tables: &Arc<Vec<Table>>,
        needs_bitmap: bool,
    ) {
        if let HashJoinProbeState::Building = self {
            *self = HashJoinProbeState::ReadyToProbe(
                table.clone(),
                tables.clone(),
                if needs_bitmap {
                    Some(IndexBitmapTracker::new(tables))
                } else {
                    None
                },
            );
        } else {
            panic!("HashJoinProbeState should only be in Building state when setting table")
        }
    }

    fn probe_inner(
        &self,
        input: &Arc<MicroPartition>,
        probe_on: &[ExprRef],
        common_join_key_names: &[&str],
        left_side_non_join_column_names: &[&str],
        right_side_non_join_column_names: &[&str],
        build_on_left: bool,
    ) -> DaftResult<Arc<MicroPartition>> {
        if let HashJoinProbeState::ReadyToProbe(probe_table, tables, ..) = self {
            let _growables = info_span!("HashJoinOperator::build_growables").entered();

            let mut build_side_growable =
                GrowableTable::new(&tables.iter().collect::<Vec<_>>(), false, 20)?;

            let input_tables = input.get_tables()?;

            let mut probe_side_growable =
                GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, 20)?;

            drop(_growables);
            {
                let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
                for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                    // we should emit one table at a time when this is streaming
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
                                // we can perform run length compression for this to make this more efficient
                                probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                            }
                        }
                    }
                }
            }
            let build_side_table = build_side_growable.build()?;
            let probe_side_table = probe_side_growable.build()?;

            let (left_table, right_table) = if build_on_left {
                (build_side_table, probe_side_table)
            } else {
                (probe_side_table, build_side_table)
            };

            let join_keys_table = left_table.get_columns(common_join_key_names)?;
            let left_non_join_columns = left_table.get_columns(left_side_non_join_column_names)?;
            let right_non_join_columns =
                right_table.get_columns(right_side_non_join_column_names)?;
            let final_table = join_keys_table
                .union(&left_non_join_columns)?
                .union(&right_non_join_columns)?;

            Ok(Arc::new(MicroPartition::new_loaded(
                final_table.schema.clone(),
                Arc::new(vec![final_table]),
                None,
            )))
        } else {
            panic!("probe can only be used during the ReadyToProbe Phase")
        }
    }

    // For left join, build_side is the right side and probe_side is the left side
    // For right join, build_side is the left side and probe_side is the right side
    fn probe_left_right(
        &self,
        input: &Arc<MicroPartition>,
        probe_on: &[ExprRef],
        common_join_key_names: &[&str],
        left_side_non_join_column_names: &[&str],
        right_side_non_join_column_names: &[&str],
        is_left: bool,
    ) -> DaftResult<Arc<MicroPartition>> {
        if let HashJoinProbeState::ReadyToProbe(probe_table, tables, ..) = self {
            let _growables = info_span!("HashJoinOperator::build_growables").entered();

            // Need to set use_validity to true here because we add nulls to the build side
            let mut build_side_growable =
                GrowableTable::new(&tables.iter().collect::<Vec<_>>(), true, 20)?;

            let input_tables = input.get_tables()?;

            let mut probe_side_growable =
                GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, 20)?;

            drop(_growables);
            {
                let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
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

            let final_table = if is_left {
                let join_table = probe_side_table.get_columns(common_join_key_names)?;
                let left = probe_side_table.get_columns(left_side_non_join_column_names)?;
                let right = build_side_table.get_columns(right_side_non_join_column_names)?;
                join_table.union(&left)?.union(&right)?
            } else {
                let join_table = probe_side_table.get_columns(common_join_key_names)?;
                let left = build_side_table.get_columns(left_side_non_join_column_names)?;
                let right = probe_side_table.get_columns(right_side_non_join_column_names)?;
                join_table.union(&left)?.union(&right)?
            };
            Ok(Arc::new(MicroPartition::new_loaded(
                final_table.schema.clone(),
                Arc::new(vec![final_table]),
                None,
            )))
        } else {
            panic!("probe can only be used during the ReadyToProbe Phase")
        }
    }

    fn probe_outer(
        &mut self,
        input: &Arc<MicroPartition>,
        probe_on: &[ExprRef],
        common_join_key_names: &[&str],
        left_side_non_join_column_names: &[&str],
        right_side_non_join_column_names: &[&str],
    ) -> DaftResult<Arc<MicroPartition>> {
        if let HashJoinProbeState::ReadyToProbe(probe_table, tables, bitmap) = self {
            let _growables = info_span!("HashJoinOperator::build_growables").entered();

            // Need to set use_validity to true here because we add nulls to the build side
            let mut build_side_growable =
                GrowableTable::new(&tables.iter().collect::<Vec<_>>(), true, 20)?;

            let input_tables = input.get_tables()?;

            let mut probe_side_growable =
                GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, 20)?;

            let left_idx_used = bitmap.as_mut().expect("bitmap should be set in outer join");

            drop(_growables);
            {
                let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
                for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                    let join_keys = table.eval_expression_list(probe_on)?;
                    let idx_mapper = probe_table.probe_indices(&join_keys)?;

                    for (probe_row_idx, inner_iter) in idx_mapper.make_iter().enumerate() {
                        if let Some(inner_iter) = inner_iter {
                            for (build_side_table_idx, build_row_idx) in inner_iter {
                                left_idx_used.set_true(
                                    build_side_table_idx as usize,
                                    build_row_idx as usize,
                                );
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

            let join_table = probe_side_table.get_columns(common_join_key_names)?;
            let left = build_side_table.get_columns(left_side_non_join_column_names)?;
            let right = probe_side_table.get_columns(right_side_non_join_column_names)?;
            let final_table = join_table.union(&left)?.union(&right)?;
            Ok(Arc::new(MicroPartition::new_loaded(
                final_table.schema.clone(),
                Arc::new(vec![final_table]),
                None,
            )))
        } else {
            panic!("probe can only be used during the ReadyToProbe Phase")
        }
    }

    fn probe_anti_semi(
        &self,
        input: &Arc<MicroPartition>,
        probe_on: &[ExprRef],
        is_semi: bool,
    ) -> DaftResult<Arc<MicroPartition>> {
        if let HashJoinProbeState::ReadyToProbe(probe_set, ..) = self {
            let _growables = info_span!("HashJoinOperator::build_growables").entered();

            let input_tables = input.get_tables()?;

            let mut probe_side_growable =
                GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, 20)?;

            drop(_growables);
            {
                let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
                for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                    let join_keys = table.eval_expression_list(probe_on)?;
                    let iter = probe_set.probe_exists(&join_keys)?;

                    for (probe_row_idx, matched) in iter.enumerate() {
                        match (is_semi, matched) {
                            (true, true) | (false, false) => {
                                probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                            }
                            _ => {}
                        }
                    }
                }
            }
            let probe_side_table = probe_side_growable.build()?;
            Ok(Arc::new(MicroPartition::new_loaded(
                probe_side_table.schema.clone(),
                Arc::new(vec![probe_side_table]),
                None,
            )))
        } else {
            panic!("probe can only be used during the ReadyToProbe Phase")
        }
    }
}

impl IntermediateOperatorState for HashJoinProbeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub(crate) struct HashJoinProbeOperator {
    probe_on: Vec<ExprRef>,
    common_join_key_names: Vec<String>,
    left_non_join_columns: Vec<Field>,
    right_non_join_columns: Vec<Field>,
    join_type: JoinType,
    build_on_left: bool,
}

impl HashJoinProbeOperator {
    pub(crate) fn new(
        probe_on: Vec<ExprRef>,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        join_type: JoinType,
        build_on_left: bool,
        common_join_keys: IndexSet<String>,
    ) -> Self {
        let (common_join_key_names, left_non_join_columns, right_non_join_columns) = match join_type
        {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Outer => {
                let left_non_join_columns = left_schema
                    .fields
                    .values()
                    .filter(|field| !common_join_keys.contains(field.name.as_str()))
                    .cloned()
                    .collect();
                let right_non_join_columns = right_schema
                    .fields
                    .values()
                    .filter(|field| !common_join_keys.contains(field.name.as_str()))
                    .cloned()
                    .collect();
                (
                    common_join_keys.into_iter().collect(),
                    left_non_join_columns,
                    right_non_join_columns,
                )
            }
            JoinType::Anti | JoinType::Semi => (vec![], vec![], vec![]),
        };
        Self {
            probe_on,
            common_join_key_names,
            left_non_join_columns,
            right_non_join_columns,
            join_type,
            build_on_left,
        }
    }

    fn finalize_outer(
        &self,
        mut states: Vec<Box<dyn IntermediateOperatorState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let states = states
            .iter_mut()
            .map(|s| {
                s.as_any_mut()
                    .downcast_mut::<HashJoinProbeState>()
                    .expect("HashJoinProbeOperator state should be HashJoinProbeState")
            })
            .collect::<Vec<_>>();
        let tables = if let HashJoinProbeState::ReadyToProbe(_, tables, ..) = states
            .first()
            .expect("at least one state should be present")
        {
            tables.clone()
        } else {
            panic!("HashJoinProbeState should be in ReadyToProbe state")
        };

        let merged_bitmap = {
            let bitmaps = states
                .into_iter()
                .map(|s| {
                    if let HashJoinProbeState::ReadyToProbe(_, _, bitmap) = s {
                        bitmap
                            .take()
                            .expect("bitmap should be present in outer join")
                    } else {
                        panic!("HashJoinProbeState should be in ReadyToProbe state")
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

        let join_table = build_side_table.get_columns(&self.common_join_key_names)?;
        let left = build_side_table.get_columns(
            &self
                .left_non_join_columns
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>(),
        )?;
        let right = {
            let schema = Schema::new(self.right_non_join_columns.to_vec())?;
            let columns = self
                .right_non_join_columns
                .iter()
                .map(|field| Series::full_null(&field.name, &field.dtype, left.len()))
                .collect::<Vec<_>>();
            Table::new_unchecked(schema, columns, left.len())
        };
        let final_table = join_table.union(&left)?.union(&right)?;
        Ok(Some(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        ))))
    }
}

impl IntermediateOperator for HashJoinProbeOperator {
    #[instrument(skip_all, name = "HashJoinOperator::execute")]
    fn execute(
        &self,
        idx: usize,
        input: &PipelineResultType,
        state: &mut dyn IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult> {
        match idx {
            0 => {
                let state = state
                    .as_any_mut()
                    .downcast_mut::<HashJoinProbeState>()
                    .expect("HashJoinProbeOperator state should be HashJoinProbeState");
                let (probe_table, tables) = input.as_probe_table();
                state.initialize_probe_state(
                    probe_table,
                    tables,
                    self.join_type == JoinType::Outer,
                );
                Ok(IntermediateOperatorResult::NeedMoreInput(None))
            }
            _ => {
                let state = state
                    .as_any_mut()
                    .downcast_mut::<HashJoinProbeState>()
                    .expect("HashJoinProbeOperator state should be HashJoinProbeState");
                let input = input.as_data();
                let common_join_key_names = self
                    .common_join_key_names
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>();
                let left_non_join_column_names = self
                    .left_non_join_columns
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect::<Vec<_>>();
                let right_non_join_column_names = self
                    .right_non_join_columns
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect::<Vec<_>>();
                let out = match self.join_type {
                    JoinType::Inner => state.probe_inner(
                        input,
                        &self.probe_on,
                        &common_join_key_names,
                        &left_non_join_column_names,
                        &right_non_join_column_names,
                        self.build_on_left,
                    ),
                    JoinType::Left | JoinType::Right => state.probe_left_right(
                        input,
                        &self.probe_on,
                        &common_join_key_names,
                        &left_non_join_column_names,
                        &right_non_join_column_names,
                        self.join_type == JoinType::Left,
                    ),
                    JoinType::Semi | JoinType::Anti => state.probe_anti_semi(
                        input,
                        &self.probe_on,
                        self.join_type == JoinType::Semi,
                    ),
                    JoinType::Outer => state.probe_outer(
                        input,
                        &self.probe_on,
                        &common_join_key_names,
                        &left_non_join_column_names,
                        &right_non_join_column_names,
                    ),
                }?;
                Ok(IntermediateOperatorResult::NeedMoreInput(Some(out)))
            }
        }
    }

    fn name(&self) -> &'static str {
        "HashJoinProbeOperator"
    }

    fn make_state(&self) -> Box<dyn IntermediateOperatorState> {
        Box::new(HashJoinProbeState::Building)
    }

    fn finalize(
        &self,
        states: Vec<Box<dyn IntermediateOperatorState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        if self.join_type == JoinType::Outer {
            self.finalize_outer(states)
        } else {
            Ok(None)
        }
    }
}
