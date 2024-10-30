use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_table::{GrowableTable, ProbeState};
use indexmap::IndexSet;
use tracing::{info_span, instrument};

use super::intermediate_op::{
    DynIntermediateOpState, IntermediateOperator, IntermediateOperatorResult,
    IntermediateOperatorState,
};
use crate::pipeline::PipelineResultType;

enum InnerHashJoinProbeState {
    Building,
    ReadyToProbe(Arc<ProbeState>),
}

impl InnerHashJoinProbeState {
    fn set_probe_state(&mut self, probe_state: Arc<ProbeState>) {
        if matches!(self, Self::Building) {
            *self = Self::ReadyToProbe(probe_state);
        } else {
            panic!("InnerHashJoinProbeState should only be in Building state when setting table")
        }
    }

    fn get_probe_state(&self) -> &Arc<ProbeState> {
        if let Self::ReadyToProbe(probe_state) = self {
            probe_state
        } else {
            panic!("get_probeable_and_table can only be used during the ReadyToProbe Phase")
        }
    }
}

impl DynIntermediateOpState for InnerHashJoinProbeState {
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
    output_schema: SchemaRef,
}

impl InnerHashJoinProbeOperator {
    const DEFAULT_GROWABLE_SIZE: usize = 20;

    pub fn new(
        probe_on: Vec<ExprRef>,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        build_on_left: bool,
        common_join_keys: IndexSet<String>,
        output_schema: &SchemaRef,
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
            output_schema: output_schema.clone(),
        }
    }

    fn probe_inner(
        &self,
        input: &Arc<MicroPartition>,
        state: &InnerHashJoinProbeState,
    ) -> DaftResult<Arc<MicroPartition>> {
        let (probe_table, tables) = {
            let probe_state = state.get_probe_state();
            (probe_state.get_probeable(), probe_state.get_tables())
        };

        let _growables = info_span!("InnerHashJoinOperator::build_growables").entered();

        let mut build_side_growable = GrowableTable::new(
            &tables.iter().collect::<Vec<_>>(),
            false,
            Self::DEFAULT_GROWABLE_SIZE,
        )?;

        let input_tables = input.get_tables()?;

        let mut probe_side_growable = GrowableTable::new(
            &input_tables.iter().collect::<Vec<_>>(),
            false,
            Self::DEFAULT_GROWABLE_SIZE,
        )?;

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
        state: &IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult> {
        state.with_state_mut::<InnerHashJoinProbeState, _, _>(|state| match idx {
            0 => {
                let probe_state = input.as_probe_state();
                state.set_probe_state(probe_state.clone());
                Ok(IntermediateOperatorResult::NeedMoreInput(None))
            }
            _ => {
                let input = input.as_data();
                if input.is_empty() {
                    let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
                    return Ok(IntermediateOperatorResult::NeedMoreInput(Some(empty)));
                }
                let out = self.probe_inner(input, state)?;
                Ok(IntermediateOperatorResult::NeedMoreInput(Some(out)))
            }
        })
    }

    fn name(&self) -> &'static str {
        "InnerHashJoinProbeOperator"
    }

    fn make_state(&self) -> DaftResult<Box<dyn DynIntermediateOpState>> {
        Ok(Box::new(InnerHashJoinProbeState::Building))
    }
}
