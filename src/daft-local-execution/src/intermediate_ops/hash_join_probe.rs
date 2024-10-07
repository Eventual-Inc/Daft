use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use daft_table::{GrowableTable, Probeable, Table};
use indexmap::IndexSet;
use tracing::{info_span, instrument};

use super::intermediate_op::{
    IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState,
};
use crate::pipeline::PipelineResultType;

enum HashJoinProbeState {
    Building,
    ReadyToProbe(Arc<dyn Probeable>, Arc<Vec<Table>>),
}

impl HashJoinProbeState {
    fn set_table(&mut self, table: &Arc<dyn Probeable>, tables: &Arc<Vec<Table>>) {
        if matches!(self, Self::Building) {
            *self = Self::ReadyToProbe(table.clone(), tables.clone());
        } else {
            panic!("HashJoinProbeState should only be in Building state when setting table")
        }
    }

    fn get_probeable_and_table(&self) -> (&Arc<dyn Probeable>, &Arc<Vec<Table>>) {
        if let Self::ReadyToProbe(probe_table, tables) = self {
            (probe_table, tables)
        } else {
            panic!("get_probeable_and_table can only be used during the ReadyToProbe Phase")
        }
    }
}

impl IntermediateOperatorState for HashJoinProbeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct HashJoinProbeOperator {
    probe_on: Vec<ExprRef>,
    common_join_keys: Vec<String>,
    left_non_join_columns: Vec<String>,
    right_non_join_columns: Vec<String>,
    join_type: JoinType,
    build_on_left: bool,
}

impl HashJoinProbeOperator {
    pub fn new(
        probe_on: Vec<ExprRef>,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        join_type: JoinType,
        build_on_left: bool,
        common_join_keys: IndexSet<String>,
    ) -> Self {
        let (common_join_keys, left_non_join_columns, right_non_join_columns) = match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right => {
                let left_non_join_columns = left_schema
                    .fields
                    .keys()
                    .filter(|c| !common_join_keys.contains(*c))
                    .cloned()
                    .collect();
                let right_non_join_columns = right_schema
                    .fields
                    .keys()
                    .filter(|c| !common_join_keys.contains(*c))
                    .cloned()
                    .collect();
                (
                    common_join_keys.into_iter().collect(),
                    left_non_join_columns,
                    right_non_join_columns,
                )
            }
            _ => {
                panic!("Semi, Anti, and join are not supported in HashJoinProbeOperator")
            }
        };
        Self {
            probe_on,
            common_join_keys,
            left_non_join_columns,
            right_non_join_columns,
            join_type,
            build_on_left,
        }
    }

    fn probe_inner(
        &self,
        input: &Arc<MicroPartition>,
        state: &HashJoinProbeState,
    ) -> DaftResult<Arc<MicroPartition>> {
        let (probe_table, tables) = state.get_probeable_and_table();

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
                            // we can perform run length compression for this to make this more efficient
                            probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                        }
                    }
                }
            }
        }
        let build_side_table = build_side_growable.build()?;
        let probe_side_table = probe_side_growable.build()?;

        let (left_table, right_table) = if self.build_on_left {
            (build_side_table, probe_side_table)
        } else {
            (probe_side_table, build_side_table)
        };

        let join_keys_table = left_table.get_columns(&self.common_join_keys)?;
        let left_non_join_columns = left_table.get_columns(&self.left_non_join_columns)?;
        let right_non_join_columns = right_table.get_columns(&self.right_non_join_columns)?;
        let final_table = join_keys_table
            .union(&left_non_join_columns)?
            .union(&right_non_join_columns)?;

        Ok(Arc::new(MicroPartition::new_loaded(
            final_table.schema.clone(),
            Arc::new(vec![final_table]),
            None,
        )))
    }

    fn probe_left_right(
        &self,
        input: &Arc<MicroPartition>,
        state: &HashJoinProbeState,
    ) -> DaftResult<Arc<MicroPartition>> {
        let (probe_table, tables) = state.get_probeable_and_table();

        let _growables = info_span!("HashJoinOperator::build_growables").entered();

        let mut build_side_growable = GrowableTable::new(
            &tables.iter().collect::<Vec<_>>(),
            true,
            tables.iter().map(daft_table::Table::len).sum(),
        )?;

        let input_tables = input.get_tables()?;

        let mut probe_side_growable =
            GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, input.len())?;

        drop(_growables);
        {
            let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
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
}

impl IntermediateOperator for HashJoinProbeOperator {
    #[instrument(skip_all, name = "HashJoinOperator::execute")]
    fn execute(
        &self,
        idx: usize,
        input: &PipelineResultType,
        state: Option<&mut Box<dyn IntermediateOperatorState>>,
    ) -> DaftResult<IntermediateOperatorResult> {
        let state = state
            .expect("HashJoinProbeOperator should have state")
            .as_any_mut()
            .downcast_mut::<HashJoinProbeState>()
            .expect("HashJoinProbeOperator state should be HashJoinProbeState");

        if idx == 0 {
            let (probe_table, tables) = input.as_probe_table();
            state.set_table(probe_table, tables);
            Ok(IntermediateOperatorResult::NeedMoreInput(None))
        } else {
            let input = input.as_data();
            let out = match self.join_type {
                JoinType::Inner => self.probe_inner(input, state),
                JoinType::Left | JoinType::Right => self.probe_left_right(input, state),
                _ => {
                    unimplemented!(
                        "Only Inner, Left, and Right joins are supported in HashJoinProbeOperator"
                    )
                }
            }?;
            Ok(IntermediateOperatorResult::NeedMoreInput(Some(out)))
        }
    }

    fn name(&self) -> &'static str {
        "HashJoinProbeOperator"
    }

    fn make_state(&self) -> Option<Box<dyn IntermediateOperatorState>> {
        Some(Box::new(HashJoinProbeState::Building))
    }
}
