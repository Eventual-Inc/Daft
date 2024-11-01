use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::{col, AggExpr, Expr, ExprRef};
use daft_micropartition::MicroPartition;
use daft_plan::populate_aggregation_stages;
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkState, BlockingSinkStatus};
use crate::NUM_CPUS;

enum AggregateState {
    Accumulating(Vec<Arc<MicroPartition>>),
    Done,
}

impl AggregateState {
    fn push(&mut self, part: Arc<MicroPartition>) {
        if let Self::Accumulating(ref mut parts) = self {
            parts.push(part);
        } else {
            panic!("AggregateSink should be in Accumulating state");
        }
    }

    fn finalize(&mut self) -> Vec<Arc<MicroPartition>> {
        let res = if let Self::Accumulating(ref mut parts) = self {
            std::mem::take(parts)
        } else {
            panic!("AggregateSink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for AggregateState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct AggregateSink {
    sink_aggs: Vec<ExprRef>,
    finalize_aggs: Vec<ExprRef>,
    final_projections: Vec<ExprRef>,
    sink_group_by: Vec<ExprRef>,
    finalize_group_by: Vec<ExprRef>,
}

impl AggregateSink {
    pub fn new(agg_exprs: &[AggExpr], group_by: &[ExprRef], schema: &SchemaRef) -> Self {
        let (sink_aggs, finalize_aggs, final_projections) =
            populate_aggregation_stages(agg_exprs, schema, group_by);
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
            group_by.to_vec()
        } else {
            group_by.iter().map(|e| col(e.name())).collect()
        };
        Self {
            sink_aggs,
            finalize_aggs,
            final_projections,
            sink_group_by: group_by.to_vec(),
            finalize_group_by,
        }
    }
}

impl BlockingSink for AggregateSink {
    #[instrument(skip_all, name = "AggregateSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
    ) -> DaftResult<BlockingSinkStatus> {
        let agg_state = state
            .as_any_mut()
            .downcast_mut::<AggregateState>()
            .expect("AggregateSink should have AggregateState");
        if self.sink_aggs.is_empty() {
            agg_state.push(input.clone());
        } else {
            let agged = input.agg(&self.sink_aggs, &self.sink_group_by)?;
            agg_state.push(agged.into());
        }
        Ok(BlockingSinkStatus::NeedMoreInput(state))
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let all_parts = states.into_iter().flat_map(|mut state| {
            state
                .as_any_mut()
                .downcast_mut::<AggregateState>()
                .expect("AggregateSink should have AggregateState")
                .finalize()
        });
        let concated = MicroPartition::concat(all_parts)?;
        let agged = concated.agg(&self.finalize_aggs, &self.finalize_group_by)?;
        let projected = Arc::new(agged.eval_expression_list(&self.final_projections)?);
        Ok(Some(projected))
    }

    fn name(&self) -> &'static str {
        "AggregateSink"
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(AggregateState::Accumulating(vec![])))
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
