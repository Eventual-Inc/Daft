use std::{
    cmp::{max, min},
    sync::Arc,
};

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_core::prelude::SchemaRef;
use daft_dsl::{col, AggExpr, Expr, ExprRef};
use daft_micropartition::MicroPartition;
use snafu::ResultExt;
use tracing::instrument;

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::{JoinSnafu, NUM_CPUS};

enum GroupedAggregateState {
    Accumulating(Vec<Arc<MicroPartition>>),
    Done,
}

impl GroupedAggregateState {
    fn push(&mut self, part: Arc<MicroPartition>) {
        if let Self::Accumulating(ref mut parts) = self {
            parts.push(part);
        } else {
            panic!("GroupedAggregateSink should be in Accumulating state");
        }
    }

    fn finalize(&mut self) -> Vec<Arc<MicroPartition>> {
        let res = if let Self::Accumulating(ref mut parts) = self {
            std::mem::take(parts)
        } else {
            panic!("GroupedAggregateSink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for GroupedAggregateState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct AggParams {
    sink_agg_exprs: Vec<ExprRef>,
    sink_group_by: Vec<ExprRef>,
    finalize_agg_exprs: Vec<ExprRef>,
    finalize_group_by: Vec<ExprRef>,
    final_projections: Vec<ExprRef>,
}

pub struct GroupedAggregateSink {
    agg_sink_params: Arc<AggParams>,
}

impl GroupedAggregateSink {
    pub fn new(aggregations: &[AggExpr], group_by: &[ExprRef], schema: &SchemaRef) -> Self {
        let (sink_aggs, finalize_aggs, final_projections) =
            daft_physical_plan::populate_aggregation_stages(aggregations, schema, group_by);
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
        let finalize_group_by = if sink_aggs.is_empty() {
            group_by.to_vec()
        } else {
            group_by.iter().map(|e| col(e.name())).collect()
        };

        Self {
            agg_sink_params: Arc::new(AggParams {
                sink_agg_exprs,
                sink_group_by: group_by.to_vec(),
                finalize_agg_exprs,
                finalize_group_by,
                final_projections,
            }),
        }
    }
}

impl BlockingSink for GroupedAggregateSink {
    #[instrument(skip_all, name = "GroupedAggregateSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        runtime: &RuntimeRef,
    ) -> BlockingSinkSinkResult {
        if self.agg_sink_params.sink_agg_exprs.is_empty() {
            let agg_state = state
                .as_any_mut()
                .downcast_mut::<GroupedAggregateState>()
                .expect("GroupedAggregateSink should have GroupedAggregateState");
            agg_state.push(input.clone());
            return Ok(BlockingSinkStatus::NeedMoreInput(state)).into();
        }

        let params = self.agg_sink_params.clone();
        let input = input.clone();
        runtime
            .spawn(async move {
                let agg_state = state
                    .as_any_mut()
                    .downcast_mut::<GroupedAggregateState>()
                    .expect("GroupedAggregateSink should have GroupedAggregateState");
                let agged = Arc::new(input.agg(&params.sink_agg_exprs, &params.sink_group_by)?);
                agg_state.push(agged);
                Ok(BlockingSinkStatus::NeedMoreInput(state))
            })
            .into()
    }

    #[instrument(skip_all, name = "GroupedAggregateSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        runtime: &RuntimeRef,
    ) -> BlockingSinkFinalizeResult {
        let params = self.agg_sink_params.clone();
        runtime
            .spawn(async move {
                let all_parts = states.into_iter().flat_map(|mut state| {
                    state
                        .as_any_mut()
                        .downcast_mut::<GroupedAggregateState>()
                        .expect("AggregateSink should have AggregateState")
                        .finalize()
                });
                let concated = MicroPartition::concat(all_parts)?;
                let num_parallel_finalizations = min(max(1, concated.len() / 100_000), *NUM_CPUS);

                if num_parallel_finalizations > 1 {
                    let mut futures = Vec::with_capacity(num_parallel_finalizations);
                    let parts = concated
                        .partition_by_hash(&params.finalize_group_by, num_parallel_finalizations)?;
                    for part in parts {
                        let params = params.clone();
                        futures.push(tokio::spawn(async move {
                            let agged =
                                part.agg(&params.finalize_agg_exprs, &params.finalize_group_by)?;
                            let projected =
                                agged.eval_expression_list(&params.final_projections)?;
                            DaftResult::Ok(Arc::new(projected))
                        }));
                    }
                    let mut res = Vec::with_capacity(num_parallel_finalizations);
                    for fut in futures {
                        res.push(fut.await.context(JoinSnafu)??);
                    }
                    Ok(Some(Arc::new(MicroPartition::concat(res)?)))
                } else {
                    let agged =
                        concated.agg(&params.finalize_agg_exprs, &params.finalize_group_by)?;
                    let projected = agged.eval_expression_list(&params.final_projections)?;
                    Ok(Some(Arc::new(projected)))
                }
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "GroupedAggregateSink"
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(GroupedAggregateState::Accumulating(vec![])))
    }
}
