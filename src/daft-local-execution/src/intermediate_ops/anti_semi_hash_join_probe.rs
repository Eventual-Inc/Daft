use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use daft_plan::JoinType;
use daft_table::{GrowableTable, Probeable};
use tracing::{info_span, instrument};

use super::intermediate_op::{
    DynIntermediateOpState, IntermediateOperator, IntermediateOperatorResult,
    IntermediateOperatorState,
};
use crate::pipeline::PipelineResultType;

enum AntiSemiProbeState {
    Building,
    ReadyToProbe(Arc<dyn Probeable>),
}

impl AntiSemiProbeState {
    fn set_table(&mut self, table: &Arc<dyn Probeable>) {
        if matches!(self, Self::Building) {
            *self = Self::ReadyToProbe(table.clone());
        } else {
            panic!("AntiSemiProbeState should only be in Building state when setting table")
        }
    }

    fn get_probeable(&self) -> &Arc<dyn Probeable> {
        if let Self::ReadyToProbe(probeable) = self {
            probeable
        } else {
            panic!("AntiSemiProbeState should only be in ReadyToProbe state when getting probeable")
        }
    }
}

impl DynIntermediateOpState for AntiSemiProbeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct AntiSemiProbeOperator {
    probe_on: Vec<ExprRef>,
    is_semi: bool,
    output_schema: SchemaRef,
}

impl AntiSemiProbeOperator {
    const DEFAULT_GROWABLE_SIZE: usize = 20;

    pub fn new(probe_on: Vec<ExprRef>, join_type: &JoinType, output_schema: &SchemaRef) -> Self {
        Self {
            probe_on,
            is_semi: *join_type == JoinType::Semi,
            output_schema: output_schema.clone(),
        }
    }

    fn probe_anti_semi(
        &self,
        input: &Arc<MicroPartition>,
        state: &AntiSemiProbeState,
    ) -> DaftResult<Arc<MicroPartition>> {
        let probe_set = state.get_probeable();

        let _growables = info_span!("AntiSemiOperator::build_growables").entered();

        let input_tables = input.get_tables()?;

        let mut probe_side_growable = GrowableTable::new(
            &input_tables.iter().collect::<Vec<_>>(),
            false,
            Self::DEFAULT_GROWABLE_SIZE,
        )?;

        drop(_growables);
        {
            let _loop = info_span!("AntiSemiOperator::eval_and_probe").entered();
            for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                let join_keys = table.eval_expression_list(&self.probe_on)?;
                let iter = probe_set.probe_exists(&join_keys)?;

                for (probe_row_idx, matched) in iter.enumerate() {
                    match (self.is_semi, matched) {
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
    }
}

impl IntermediateOperator for AntiSemiProbeOperator {
    #[instrument(skip_all, name = "AntiSemiOperator::execute")]
    fn execute(
        &self,
        idx: usize,
        input: &PipelineResultType,
        state: &IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult> {
        state.with_state_mut::<AntiSemiProbeState, _, _>(|state| {
            if idx == 0 {
                let probe_state = input.as_probe_state();
                state.set_table(probe_state.get_probeable());
                Ok(IntermediateOperatorResult::NeedMoreInput(None))
            } else {
                let input = input.as_data();
                if input.is_empty() {
                    let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
                    return Ok(IntermediateOperatorResult::NeedMoreInput(Some(empty)));
                }
                let out = self.probe_anti_semi(input, state)?;
                Ok(IntermediateOperatorResult::NeedMoreInput(Some(out)))
            }
        })
    }

    fn name(&self) -> &'static str {
        "AntiSemiProbeOperator"
    }

    fn make_state(&self) -> DaftResult<Box<dyn DynIntermediateOpState>> {
        Ok(Box::new(AntiSemiProbeState::Building))
    }
}
