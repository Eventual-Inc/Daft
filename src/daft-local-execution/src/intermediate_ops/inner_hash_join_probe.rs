use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_table::{GrowableTable, Probeable, Table};
use indexmap::IndexSet;
use tracing::{info_span, instrument};

use super::intermediate_op::{
    IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState,
};
use crate::pipeline::PipelineResultType;

enum InnerHashJoinProbeState {
    Building,
    ReadyToProbe(Arc<dyn Probeable>, Arc<Vec<Table>>),
}

impl InnerHashJoinProbeState {
    fn set_table(&mut self, table: &Arc<dyn Probeable>, tables: &Arc<Vec<Table>>) {
        if let InnerHashJoinProbeState::Building = self {
            *self = InnerHashJoinProbeState::ReadyToProbe(table.clone(), tables.clone());
        } else {
            panic!("InnerHashJoinProbeState should only be in Building state when setting table")
        }
    }

    fn get_probeable_and_table(&self) -> (&Arc<dyn Probeable>, &Arc<Vec<Table>>) {
        if let InnerHashJoinProbeState::ReadyToProbe(probe_table, tables) = self {
            (probe_table, tables)
        } else {
            panic!("get_probeable_and_table can only be used during the ReadyToProbe Phase")
        }
    }
}

impl IntermediateOperatorState for InnerHashJoinProbeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct InnerHashJoinProbeOperator {
    probe_on: Vec<ExprRef>,
    common_join_keys: Vec<String>,
    left_non_join_columns: Vec<String>,
    right_non_join_columns: Vec<String>,
    build_on_left: bool,
}

impl InnerHashJoinProbeOperator {
    pub fn new(
        probe_on: Vec<ExprRef>,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        build_on_left: bool,
        common_join_keys: IndexSet<String>,
    ) -> Self {
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
        let common_join_keys = common_join_keys.into_iter().collect();
        Self {
            probe_on,
            common_join_keys,
            left_non_join_columns,
            right_non_join_columns,
            build_on_left,
        }
    }

    fn probe_inner(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut InnerHashJoinProbeState,
    ) -> DaftResult<Arc<MicroPartition>> {
        let (probe_table, tables) = state.get_probeable_and_table();

        let _growables = info_span!("InnerHashJoinOperator::build_growables").entered();

        let mut build_side_growable =
            GrowableTable::new(&tables.iter().collect::<Vec<_>>(), false, 20)?;

        let input_tables = input.get_tables()?;

        let mut probe_side_growable =
            GrowableTable::new(&input_tables.iter().collect::<Vec<_>>(), false, 20)?;

        drop(_growables);
        {
            let _loop = info_span!("InnerHashJoinOperator::eval_and_probe").entered();
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
}

impl IntermediateOperator for InnerHashJoinProbeOperator {
    #[instrument(skip_all, name = "InnerHashJoinOperator::execute")]
    fn execute(
        &self,
        idx: usize,
        input: &PipelineResultType,
        state: Option<&mut Box<dyn IntermediateOperatorState>>,
    ) -> DaftResult<IntermediateOperatorResult> {
        let state = state
            .expect("InnerHashJoinProbeOperator should have state")
            .as_any_mut()
            .downcast_mut::<InnerHashJoinProbeState>()
            .expect("InnerHashJoinProbeOperator state should be InnerHashJoinProbeState");
        match idx {
            0 => {
                let (probe_table, tables) = input.as_probe_table();
                state.set_table(probe_table, tables);
                Ok(IntermediateOperatorResult::NeedMoreInput(None))
            }
            _ => {
                let input = input.as_data();
                let out = self.probe_inner(input, state)?;
                Ok(IntermediateOperatorResult::NeedMoreInput(Some(out)))
            }
        }
    }

    fn name(&self) -> &'static str {
        "InnerHashJoinProbeOperator"
    }

    fn make_state(&self) -> Option<Box<dyn IntermediateOperatorState>> {
        Some(Box::new(InnerHashJoinProbeState::Building))
    }
}
