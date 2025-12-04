use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use common_runtime::get_compute_pool_num_threads;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::{BoundAggExpr, BoundExpr};
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

pub(crate) enum AggregateState {
    Accumulating(Vec<Arc<MicroPartition>>),
    Done,
}

impl AggregateState {
    fn push(&mut self, part: Arc<MicroPartition>) {
        if let Self::Accumulating(parts) = self {
            parts.push(part);
        } else {
            panic!("AggregateSink should be in Accumulating state");
        }
    }

    fn finalize(&mut self) -> Vec<Arc<MicroPartition>> {
        let res = if let Self::Accumulating(parts) = self {
            std::mem::take(parts)
        } else {
            panic!("AggregateSink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

struct AggParams {
    sink_agg_exprs: Vec<BoundAggExpr>,
    finalize_agg_exprs: Vec<BoundAggExpr>,
    final_projections: Vec<BoundExpr>,
}

pub struct AggregateSink {
    aggregate_name: &'static str,
    agg_sink_params: Arc<AggParams>,
}

impl AggregateSink {
    pub fn new(aggregations: &[BoundAggExpr], input_schema: &SchemaRef) -> DaftResult<Self> {
        let aggregate_name = if aggregations.len() == 1 {
            aggregations[0].as_ref().agg_name()
        } else {
            "Aggregate"
        };

        let (sink_agg_exprs, finalize_agg_exprs, final_projections) =
            daft_local_plan::agg::populate_aggregation_stages_bound(
                aggregations,
                input_schema,
                &[],
            )?;

        Ok(Self {
            aggregate_name,
            agg_sink_params: Arc::new(AggParams {
                sink_agg_exprs,
                finalize_agg_exprs,
                final_projections,
            }),
        })
    }
}

impl BlockingSink for AggregateSink {
    type State = AggregateState;

    #[instrument(skip_all, name = "AggregateSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let params = self.agg_sink_params.clone();
        spawner
            .spawn(
                async move {
                    let agged = Arc::new(input.agg(&params.sink_agg_exprs, &[])?);
                    state.push(agged);
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let params = self.agg_sink_params.clone();
        spawner
            .spawn(
                async move {
                    let all_parts = states.into_iter().flat_map(|mut state| state.finalize());
                    let concated = MicroPartition::concat(all_parts)?;
                    let agged = concated.agg(&params.finalize_agg_exprs, &[])?;
                    let projected = agged.eval_expression_list(&params.final_projections)?;
                    Ok(BlockingSinkFinalizeOutput::Finished(vec![Arc::new(
                        projected,
                    )]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        self.aggregate_name.into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Aggregate
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
        get_compute_pool_num_threads()
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(AggregateState::Accumulating(vec![]))
    }
}
