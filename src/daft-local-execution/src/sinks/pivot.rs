use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::{Schema, SchemaRef};
use daft_dsl::{col, AggExpr, Expr, ExprRef};
use daft_micropartition::MicroPartition;
use daft_plan::populate_aggregation_stages;
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkState, BlockingSinkStatus};
use crate::NUM_CPUS;

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

pub struct PivotSink {
    sink_aggs: Vec<ExprRef>,
    finalize_aggs: Vec<ExprRef>,
    sink_group_by: Vec<ExprRef>,
    finalize_group_by: Vec<ExprRef>,
    final_projections: Vec<ExprRef>,
    pivot_group_by: Vec<ExprRef>,
    pivot_col: ExprRef,
    values_col: ExprRef,
    names: Vec<String>,
}

impl PivotSink {
    pub fn new(
        group_by: &[ExprRef],
        pivot_col: ExprRef,
        values_col: ExprRef,
        aggregation: AggExpr,
        names: Vec<String>,
        input_schema: &SchemaRef,
    ) -> DaftResult<Self> {
        let group_by_with_pivot = group_by
            .iter()
            .chain(std::iter::once(&pivot_col))
            .cloned()
            .collect::<Vec<_>>();
        let aggregate_fields = group_by_with_pivot
            .iter()
            .map(|expr| expr.to_field(input_schema))
            .chain(std::iter::once(aggregation.to_field(input_schema)))
            .collect::<DaftResult<Vec<_>>>()?;
        let aggregate_schema = Schema::new(aggregate_fields)?;
        let (sink_aggs, finalize_aggs, final_projections) = populate_aggregation_stages(
            &[aggregation],
            &aggregate_schema.into(),
            &group_by_with_pivot,
        );
        let sink_aggs = sink_aggs
            .values()
            .cloned()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect::<Vec<_>>();
        let finalize_aggs = finalize_aggs
            .values()
            .cloned()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect::<Vec<_>>();
        let finalize_group_by = if sink_aggs.is_empty() {
            group_by_with_pivot.clone()
        } else {
            group_by_with_pivot.iter().map(|e| col(e.name())).collect()
        };
        Ok(Self {
            sink_aggs,
            finalize_aggs,
            final_projections,
            sink_group_by: group_by_with_pivot,
            finalize_group_by,
            pivot_group_by: group_by.to_vec(),
            pivot_col,
            values_col,
            names,
        })
    }
}

impl BlockingSink for PivotSink {
    #[instrument(skip_all, name = "PivotSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
    ) -> DaftResult<BlockingSinkStatus> {
        let agg_state = state
            .as_any_mut()
            .downcast_mut::<PivotState>()
            .expect("PivotSink should have PivotState");
        if self.sink_aggs.is_empty() {
            agg_state.push(input.clone());
        } else {
            let agged = input.agg(&self.sink_aggs, &self.sink_group_by)?;
            agg_state.push(agged.into());
        }
        Ok(BlockingSinkStatus::NeedMoreInput(state))
    }

    #[instrument(skip_all, name = "PivotSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let all_parts = states.into_iter().flat_map(|mut state| {
            state
                .as_any_mut()
                .downcast_mut::<PivotState>()
                .expect("PivotSink should have PivotState")
                .finalize()
        });
        let concated = MicroPartition::concat(all_parts)?;
        let agged = concated.agg(&self.finalize_aggs, &self.finalize_group_by)?;
        let projected = agged.eval_expression_list(&self.final_projections)?;
        let pivoted = projected.pivot(
            &self.pivot_group_by,
            self.pivot_col.clone(),
            self.values_col.clone(),
            self.names.clone(),
        )?;
        Ok(Some(pivoted.into()))
    }

    fn name(&self) -> &'static str {
        "PivotSink"
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(PivotState::Accumulating(vec![])))
    }

    fn make_dispatcher(
        &self,
        runtime_handle: &crate::ExecutionRuntimeHandle,
    ) -> Arc<dyn crate::dispatcher::Dispatcher> {
        Arc::new(crate::dispatcher::UnorderedDispatcher::new(Some(
            runtime_handle.default_morsel_size(),
        )))
    }
}
