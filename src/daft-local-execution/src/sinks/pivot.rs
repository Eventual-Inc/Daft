use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_dsl::expr::bound_expr::{BoundAggExpr, BoundExpr};
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

pub(crate) enum PivotState {
    Accumulating(Vec<Arc<MicroPartition>>),
    Done,
}

impl PivotState {
    fn push(&mut self, part: Arc<MicroPartition>) {
        if let Self::Accumulating(parts) = self {
            parts.push(part);
        } else {
            panic!("PivotSink should be in Accumulating state");
        }
    }

    fn finalize(&mut self) -> Vec<Arc<MicroPartition>> {
        let res = if let Self::Accumulating(parts) = self {
            std::mem::take(parts)
        } else {
            panic!("PivotSink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

struct PivotParams {
    group_by: Vec<BoundExpr>,
    pivot_column: BoundExpr,
    value_column: BoundExpr,
    aggregation: BoundAggExpr,
    names: Vec<String>,
    pre_agg: bool,
}

pub struct PivotSink {
    pivot_params: Arc<PivotParams>,
}

impl PivotSink {
    pub fn new(
        group_by: Vec<BoundExpr>,
        pivot_column: BoundExpr,
        value_column: BoundExpr,
        aggregation: BoundAggExpr,
        names: Vec<String>,
        pre_agg: bool,
    ) -> Self {
        Self {
            pivot_params: Arc::new(PivotParams {
                group_by,
                pivot_column,
                value_column,
                aggregation,
                names,
                pre_agg,
            }),
        }
    }
}

impl BlockingSink for PivotSink {
    type State = PivotState;

    #[instrument(skip_all, name = "PivotSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        state.push(input);
        Ok(BlockingSinkStatus::NeedMoreInput(state)).into()
    }

    #[instrument(skip_all, name = "PivotSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let pivot_params = self.pivot_params.clone();
        spawner
            .spawn(
                async move {
                    let all_parts = states.into_iter().flat_map(|mut state| state.finalize());
                    let concated = MicroPartition::concat(all_parts)?;

                    let agged = if pivot_params.pre_agg {
                        let group_by_with_pivot = pivot_params
                            .group_by
                            .iter()
                            .chain(std::iter::once(&pivot_params.pivot_column))
                            .cloned()
                            .collect::<Vec<_>>();
                        concated.agg(
                            std::slice::from_ref(&pivot_params.aggregation),
                            &group_by_with_pivot,
                        )?
                    } else {
                        concated
                    };

                    let pivoted = Arc::new(agged.pivot(
                        &pivot_params.group_by,
                        pivot_params.pivot_column.clone(),
                        pivot_params.value_column.clone(),
                        pivot_params.names.clone(),
                    )?);
                    Ok(BlockingSinkFinalizeOutput::Finished(vec![pivoted]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Pivot".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Pivot
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push("Pivot:".to_string());
        display.push(format!(
            "Group by = {}",
            self.pivot_params
                .group_by
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        display.push(format!("Pivot column: {}", self.pivot_params.pivot_column));
        display.push(format!("Value column: {}", self.pivot_params.value_column));
        display.push(format!(
            "Pivoted columns: {}",
            self.pivot_params.names.iter().join(", ")
        ));
        display
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(PivotState::Accumulating(vec![]))
    }
}
