use std::{borrow::Cow, sync::Arc};

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{get_columns_by_name, GrowableRecordBatch, ProbeState};
use indexmap::IndexSet;
use itertools::Itertools;
use tracing::{info_span, instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::{pipeline::NodeName, state_bridge::BroadcastStateBridgeRef, ExecutionTaskSpawner};

enum InnerHashJoinProbeState {
    Building(BroadcastStateBridgeRef<ProbeState>),
    Probing(Arc<ProbeState>),
}

impl InnerHashJoinProbeState {
    async fn get_or_await_probe_state(&mut self) -> Arc<ProbeState> {
        match self {
            Self::Building(bridge) => {
                let probe_state = bridge.get_state().await;
                *self = Self::Probing(probe_state.clone());
                probe_state
            }
            Self::Probing(probeable) => probeable.clone(),
        }
    }
}

impl IntermediateOpState for InnerHashJoinProbeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct InnerHashJoinParams {
    probe_on: Vec<BoundExpr>,
    common_join_keys: Vec<String>,
    left_non_join_columns: Vec<String>,
    right_non_join_columns: Vec<String>,
    build_on_left: bool,
}

pub struct InnerHashJoinProbeOperator {
    params: Arc<InnerHashJoinParams>,
    output_schema: SchemaRef,
    probe_state_bridge: BroadcastStateBridgeRef<ProbeState>,
}

impl InnerHashJoinProbeOperator {
    const DEFAULT_GROWABLE_SIZE: usize = 20;

    pub fn new(
        probe_on: Vec<BoundExpr>,
        left_schema: &SchemaRef,
        right_schema: &SchemaRef,
        build_on_left: bool,
        common_join_keys: IndexSet<String>,
        output_schema: &SchemaRef,
        probe_state_bridge: BroadcastStateBridgeRef<ProbeState>,
    ) -> Self {
        let left_non_join_columns = left_schema
            .field_names()
            .filter(|c| !common_join_keys.contains(*c))
            .map(ToString::to_string)
            .collect();
        let right_non_join_columns = right_schema
            .field_names()
            .filter(|c| !common_join_keys.contains(*c))
            .map(ToString::to_string)
            .collect();
        let common_join_keys = common_join_keys.into_iter().collect();
        Self {
            params: Arc::new(InnerHashJoinParams {
                probe_on,
                common_join_keys,
                left_non_join_columns,
                right_non_join_columns,
                build_on_left,
            }),
            output_schema: output_schema.clone(),
            probe_state_bridge,
        }
    }

    fn probe_inner(
        input: &Arc<MicroPartition>,
        probe_state: &Arc<ProbeState>,
        probe_on: &[BoundExpr],
        common_join_keys: &[String],
        left_non_join_columns: &[String],
        right_non_join_columns: &[String],
        build_on_left: bool,
    ) -> DaftResult<Arc<MicroPartition>> {
        let probe_table = probe_state.get_probeable();
        let tables = probe_state.get_tables();

        let _growables = info_span!("InnerHashJoinOperator::build_growables").entered();

        let mut build_side_growable = GrowableRecordBatch::new(
            &tables.iter().collect::<Vec<_>>(),
            false,
            Self::DEFAULT_GROWABLE_SIZE,
        )?;

        let input_tables = input.get_tables()?;

        let mut probe_side_growable = GrowableRecordBatch::new(
            &input_tables.iter().collect::<Vec<_>>(),
            false,
            Self::DEFAULT_GROWABLE_SIZE,
        )?;

        drop(_growables);
        {
            let _loop = info_span!("InnerHashJoinOperator::eval_and_probe").entered();
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

        let join_keys_table = get_columns_by_name(&left_table, common_join_keys)?;
        let left_non_join_columns = get_columns_by_name(&left_table, left_non_join_columns)?;
        let right_non_join_columns = get_columns_by_name(&right_table, right_non_join_columns)?;
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
        input: Arc<MicroPartition>,
        mut state: Box<dyn IntermediateOpState>,
        task_spawner: &ExecutionTaskSpawner,
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
        task_spawner
            .spawn(
                async move {
                    let inner_join_state = state
                    .as_any_mut()
                    .downcast_mut::<InnerHashJoinProbeState>()
                    .expect(
                        "InnerHashJoinProbeState should be used with InnerHashJoinProbeOperator",
                    );
                    let probe_state = inner_join_state.get_or_await_probe_state().await;
                    let res = Self::probe_inner(
                        &input,
                        &probe_state,
                        &params.probe_on,
                        &params.common_join_keys,
                        &params.left_non_join_columns,
                        &params.right_non_join_columns,
                        params.build_on_left,
                    );
                    Ok((state, IntermediateOperatorResult::NeedMoreInput(Some(res?))))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        Cow::Borrowed("InnerHashJoinProbe")
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("InnerHashJoinProbe:".to_string());
        res.push(format!(
            "Probe on: [{}]",
            self.params
                .probe_on
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        res.push(format!("Build on left: {}", self.params.build_on_left));
        res
    }

    fn make_state(&self) -> DaftResult<Box<dyn IntermediateOpState>> {
        Ok(Box::new(InnerHashJoinProbeState::Building(
            self.probe_state_bridge.clone(),
        )))
    }
}
