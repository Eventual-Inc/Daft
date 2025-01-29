use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::{Expr, ExprRef};
use daft_micropartition::MicroPartition;
use daft_physical_plan::extract_agg_expr;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, NUM_CPUS};

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

struct AggParams {
    sink_agg_exprs: Vec<ExprRef>,
    finalize_agg_exprs: Vec<ExprRef>,
    final_projections: Vec<ExprRef>,
}

pub struct AggregateSink {
    agg_sink_params: Arc<AggParams>,
}

impl AggregateSink {
    pub fn new(aggregations: &[ExprRef], schema: &SchemaRef) -> DaftResult<Self> {
        let aggregations = aggregations
            .iter()
            .map(extract_agg_expr)
            .collect::<DaftResult<Vec<_>>>()?;
        let (sink_aggs, finalize_aggs, final_projections) =
            daft_physical_plan::populate_aggregation_stages(&aggregations, schema, &[]);
        let sink_agg_exprs = sink_aggs
            .values()
            .cloned()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect();
        let finalize_agg_exprs = finalize_aggs
            .values()
            .cloned()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect();

        Ok(Self {
            agg_sink_params: Arc::new(AggParams {
                sink_agg_exprs,
                finalize_agg_exprs,
                final_projections,
            }),
        })
    }
}

impl BlockingSink for AggregateSink {
    #[instrument(skip_all, name = "AggregateSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        let params = self.agg_sink_params.clone();
        spawner
            .spawn(
                async move {
                    let agg_state = state
                        .as_any_mut()
                        .downcast_mut::<AggregateState>()
                        .expect("AggregateSink should have AggregateState");
                    let agged = Arc::new(input.agg(&params.sink_agg_exprs, &[])?);
                    agg_state.push(agged);
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.agg_sink_params.clone();
        spawner
            .spawn(
                async move {
                    let all_parts = states.into_iter().flat_map(|mut state| {
                        state
                            .as_any_mut()
                            .downcast_mut::<AggregateState>()
                            .expect("AggregateSink should have AggregateState")
                            .finalize()
                    });
                    let concated = MicroPartition::concat(all_parts)?;
                    let agged = concated.agg(&params.finalize_agg_exprs, &[])?;
                    let projected = agged.eval_expression_list(&params.final_projections)?;
                    Ok(Some(Arc::new(projected)))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "Aggregate"
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!(
            "Aggregate: {}",
            self.agg_sink_params
                .sink_agg_exprs
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        )]
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(AggregateState::Accumulating(vec![])))
    }
}
