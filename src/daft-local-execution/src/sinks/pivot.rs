use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::expr::bound_expr::{BoundAggExpr, BoundExpr};
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkState, BlockingSinkStatus,
};
use crate::ExecutionTaskSpawner;

enum PivotState {
    Accumulating(Vec<Arc<MicroPartition>>),
    Done,
}

impl PivotState {
    fn push(&mut self, part: Arc<MicroPartition>) {
        if let Self::Accumulating(ref mut parts) = self {
            parts.push(part);
        } else {
            panic!("PivotSink should be in Accumulating state");
        }
    }

    fn finalize(&mut self) -> Vec<Arc<MicroPartition>> {
        let res = if let Self::Accumulating(ref mut parts) = self {
            std::mem::take(parts)
        } else {
            panic!("PivotSink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for PivotState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct PivotParams {
    group_by: Vec<BoundExpr>,
    pivot_column: BoundExpr,
    value_column: BoundExpr,
    aggregation: BoundAggExpr,
    names: Vec<String>,
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
    ) -> Self {
        Self {
            pivot_params: Arc::new(PivotParams {
                group_by,
                pivot_column,
                value_column,
                aggregation,
                names,
            }),
        }
    }
}

impl BlockingSink for PivotSink {
    #[instrument(skip_all, name = "PivotSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        state
            .as_any_mut()
            .downcast_mut::<PivotState>()
            .expect("PivotSink should have PivotState")
            .push(input);
        Ok(BlockingSinkStatus::NeedMoreInput(state)).into()
    }

    #[instrument(skip_all, name = "PivotSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let pivot_params = self.pivot_params.clone();
        spawner
            .spawn(
                async move {
                    let all_parts = states.into_iter().flat_map(|mut state| {
                        state
                            .as_any_mut()
                            .downcast_mut::<PivotState>()
                            .expect("PivotSink should have PivotState")
                            .finalize()
                    });
                    let concated = MicroPartition::concat(all_parts)?;
                    let group_by_with_pivot = pivot_params
                        .group_by
                        .iter()
                        .chain(std::iter::once(&pivot_params.pivot_column))
                        .cloned()
                        .collect::<Vec<_>>();
                    let agged =
                        concated.agg(&[pivot_params.aggregation.clone()], &group_by_with_pivot)?;
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

    fn name(&self) -> &'static str {
        "Pivot"
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

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(PivotState::Accumulating(vec![])))
    }
}
