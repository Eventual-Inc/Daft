use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_table::{GrowableTable, Probeable};
use tracing::{info_span, instrument};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::sinks::hash_join_build::ProbeStateBridgeRef;

enum AntiSemiProbeState {
    Building(ProbeStateBridgeRef),
    Probing(Arc<dyn Probeable>),
}

impl AntiSemiProbeState {
    async fn get_or_await_probeable(&mut self) -> Arc<dyn Probeable> {
        match self {
            Self::Building(bridge) => {
                let probe_state = bridge.get_probe_state().await;
                let probeable = probe_state.get_probeable();
                *self = Self::Probing(probeable.clone());
                probeable.clone()
            }
            Self::Probing(probeable) => probeable.clone(),
        }
    }
}

impl IntermediateOpState for AntiSemiProbeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct AntiSemiJoinParams {
    probe_on: Vec<ExprRef>,
    is_semi: bool,
}

pub(crate) struct AntiSemiProbeOperator {
    params: Arc<AntiSemiJoinParams>,
    output_schema: SchemaRef,
    probe_state_bridge: ProbeStateBridgeRef,
}

impl AntiSemiProbeOperator {
    const DEFAULT_GROWABLE_SIZE: usize = 20;

    pub fn new(
        probe_on: Vec<ExprRef>,
        join_type: &JoinType,
        output_schema: &SchemaRef,
        probe_state_bridge: ProbeStateBridgeRef,
    ) -> Self {
        Self {
            params: Arc::new(AntiSemiJoinParams {
                probe_on,
                is_semi: *join_type == JoinType::Semi,
            }),
            output_schema: output_schema.clone(),
            probe_state_bridge,
        }
    }

    fn probe_anti_semi(
        probe_on: &[ExprRef],
        probe_set: &Arc<dyn Probeable>,
        input: &Arc<MicroPartition>,
        is_semi: bool,
    ) -> DaftResult<Arc<MicroPartition>> {
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
    }
}

impl IntermediateOperator for AntiSemiProbeOperator {
    #[instrument(skip_all, name = "AntiSemiOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn IntermediateOpState>,
        runtime: &RuntimeRef,
    ) -> IntermediateOpExecuteResult {
        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
            return Ok((
                state,
                IntermediateOperatorResult::NeedMoreInput(Some(empty)),
            ))
            .into();
        }

        let params = self.params.clone();
        runtime
            .spawn(async move {
                let probe_state = state
                    .as_any_mut()
                    .downcast_mut::<AntiSemiProbeState>()
                    .expect("AntiSemiProbeState should be used with AntiSemiProbeOperator");
                let probeable = probe_state.get_or_await_probeable().await;
                let res =
                    Self::probe_anti_semi(&params.probe_on, &probeable, &input, params.is_semi);
                Ok((state, IntermediateOperatorResult::NeedMoreInput(Some(res?))))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "AntiSemiProbeOperator"
    }

    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(AntiSemiProbeState::Building(
            self.probe_state_bridge.clone(),
        )))
    }
}
