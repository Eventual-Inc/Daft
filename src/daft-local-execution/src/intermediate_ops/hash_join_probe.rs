use std::sync::Arc;

use common_error::DaftResult;
use daft_core::join::JoinType;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_table::{GrowableTable, ProbeTable, Table};
use tracing::{info_span, instrument};

use crate::pipeline::PipelineResultType;

use super::intermediate_op::{
    IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState,
};

enum HashJoinProbeState {
    Building,
    ReadyToProbe(Arc<ProbeTable>, Arc<Vec<Table>>),
}

impl HashJoinProbeState {
    fn set_table(&mut self, table: &Arc<ProbeTable>, tables: &Arc<Vec<Table>>) {
        if let HashJoinProbeState::Building = self {
            *self = HashJoinProbeState::ReadyToProbe(table.clone(), tables.clone());
        } else {
            panic!("HashJoinProbeState should only be in Building state when setting table")
        }
    }

    fn probe(
        &self,
        input: &Arc<MicroPartition>,
        right_on: &[ExprRef],
        pruned_right_side_columns: &[String],
    ) -> DaftResult<Arc<MicroPartition>> {
        if let HashJoinProbeState::ReadyToProbe(probe_table, tables) = self {
            let _growables = info_span!("HashJoinOperator::build_growables").entered();

            // Left should only be created once per probe table
            let mut left_growable =
                GrowableTable::new(&tables.iter().collect::<Vec<_>>(), false, 20)?;
            // right should only be created morsel

            let right_input_tables = input.get_tables()?;

            let mut right_growable =
                GrowableTable::new(&right_input_tables.iter().collect::<Vec<_>>(), false, 20)?;

            drop(_growables);
            {
                let _loop = info_span!("HashJoinOperator::eval_and_probe").entered();
                for (r_table_idx, table) in right_input_tables.iter().enumerate() {
                    // we should emit one table at a time when this is streaming
                    let join_keys = table.eval_expression_list(right_on)?;
                    let iter = probe_table.probe(&join_keys)?;

                    for (l_table_idx, l_row_idx, right_idx) in iter {
                        left_growable.extend(l_table_idx as usize, l_row_idx as usize, 1);
                        // we can perform run length compression for this to make this more efficient
                        right_growable.extend(r_table_idx, right_idx as usize, 1);
                    }
                }
            }
            let left_table = left_growable.build()?;
            let right_table = right_growable.build()?;

            let pruned_right_table = right_table.get_columns(pruned_right_side_columns)?;

            let final_table = left_table.union(&pruned_right_table)?;
            Ok(Arc::new(MicroPartition::new_loaded(
                final_table.schema.clone(),
                Arc::new(vec![final_table]),
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

pub struct HashJoinProbeOperator {
    right_on: Vec<ExprRef>,
    pruned_right_side_columns: Vec<String>,
    _join_type: JoinType,
}

impl HashJoinProbeOperator {
    pub fn new(
        right_on: Vec<ExprRef>,
        pruned_right_side_columns: Vec<String>,
        join_type: JoinType,
    ) -> Self {
        Self {
            right_on,
            pruned_right_side_columns,
            _join_type: join_type,
        }
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
        match idx {
            0 => {
                let state = state
                    .expect("HashJoinProbeOperator should have state")
                    .as_any_mut()
                    .downcast_mut::<HashJoinProbeState>()
                    .expect("HashJoinProbeOperator state should be HashJoinProbeState");
                let (probe_table, tables) = input.as_probe_table();
                state.set_table(probe_table, tables);
                Ok(IntermediateOperatorResult::NeedMoreInput(None))
            }
            _ => {
                let state = state
                    .expect("HashJoinProbeOperator should have state")
                    .as_any_mut()
                    .downcast_mut::<HashJoinProbeState>()
                    .expect("HashJoinProbeOperator state should be HashJoinProbeState");
                let input = input.as_data();
                let out = state.probe(input, &self.right_on, &self.pruned_right_side_columns)?;
                Ok(IntermediateOperatorResult::NeedMoreInput(Some(out)))
            }
        }
    }

    fn name(&self) -> &'static str {
        "HashJoinProbeOperator"
    }

    fn make_state(&self) -> Option<Box<dyn IntermediateOperatorState>> {
        Some(Box::new(HashJoinProbeState::Building))
    }
}
