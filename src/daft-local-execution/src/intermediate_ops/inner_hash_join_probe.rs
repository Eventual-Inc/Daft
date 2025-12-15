use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::{SchemaRef, UInt64Array};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{GrowableRecordBatch, ProbeState, get_columns_by_name};
use indexmap::IndexSet;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOperator, IntermediateOperatorResult,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName, state_bridge::BroadcastStateBridgeRef};

pub(crate) enum InnerHashJoinProbeState {
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

    #[allow(clippy::too_many_arguments)]
    fn probe_inner(
        input: &Arc<MicroPartition>,
        probe_state: &Arc<ProbeState>,
        probe_on: &[BoundExpr],
        common_join_keys: &[String],
        left_non_join_columns: &[String],
        right_non_join_columns: &[String],
        build_on_left: bool,
        output_schema: &SchemaRef,
    ) -> DaftResult<Arc<MicroPartition>> {
        let build_side_tables = probe_state.get_record_batches().iter().collect::<Vec<_>>();

        let input_tables = input.record_batches();
        let result_tables = input_tables
            .iter()
            .map(|input_table| {
                let mut build_side_growable = GrowableRecordBatch::new(
                    &build_side_tables,
                    false,
                    Self::DEFAULT_GROWABLE_SIZE,
                )?;
                let mut probe_side_idxs = Vec::new();

                let join_keys = input_table.eval_expression_list(probe_on)?;
                let idx_iter = probe_state.probe_indices(&join_keys)?;
                for (probe_row_idx, inner_iter) in idx_iter.enumerate() {
                    if let Some(inner_iter) = inner_iter {
                        for (build_rb_idx, build_row_idx) in inner_iter {
                            build_side_growable.extend(
                                build_rb_idx as usize,
                                build_row_idx as usize,
                                1,
                            );
                            probe_side_idxs.push(probe_row_idx as u64);
                        }
                    }
                }

                let build_side_table = build_side_growable.build()?;
                let probe_side_table = {
                    let indices_arr = UInt64Array::from(("", probe_side_idxs));
                    input_table.take(&indices_arr)?
                };

                let (left_table, right_table) = if build_on_left {
                    (build_side_table, probe_side_table)
                } else {
                    (probe_side_table, build_side_table)
                };

                let join_keys_table = get_columns_by_name(&left_table, common_join_keys)?;
                let left_non_join_columns =
                    get_columns_by_name(&left_table, left_non_join_columns)?;
                let right_non_join_columns =
                    get_columns_by_name(&right_table, right_non_join_columns)?;
                let final_table = join_keys_table
                    .union(&left_non_join_columns)?
                    .union(&right_non_join_columns)?;
                Ok(final_table)
            })
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(Arc::new(MicroPartition::new_loaded(
            output_schema.clone(),
            Arc::new(result_tables),
            None,
        )))
    }
}

impl IntermediateOperator for InnerHashJoinProbeOperator {
    type State = InnerHashJoinProbeState;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    #[instrument(skip_all, name = "InnerHashJoinOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult<Self> {
        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
            return Ok((
                state,
                IntermediateOperatorResult::NeedMoreInput(Some(empty)),
            ))
            .into();
        }

        let params = self.params.clone();
        let output_schema = self.output_schema.clone();
        task_spawner
            .spawn(
                async move {
                    let probe_state = state.get_or_await_probe_state().await;
                    let res = Self::probe_inner(
                        &input,
                        &probe_state,
                        &params.probe_on,
                        &params.common_join_keys,
                        &params.left_non_join_columns,
                        &params.right_non_join_columns,
                        params.build_on_left,
                        &output_schema,
                    );
                    Ok((state, IntermediateOperatorResult::NeedMoreInput(Some(res?))))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Inner Hash Join Probe".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::InnerHashJoinProbe
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("Inner Hash Join Probe:".to_string());
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

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(InnerHashJoinProbeState::Building(
            self.probe_state_bridge.clone(),
        ))
    }
    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        ))
    }
}
