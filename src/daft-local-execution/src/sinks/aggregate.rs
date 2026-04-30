use std::sync::Arc;

use daft_common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::{BoundAggExpr, BoundExpr};
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
};

pub(crate) enum AggregateState {
    Accumulating(Vec<MicroPartition>),
    Done,
}

impl AggregateState {
    fn push(&mut self, part: MicroPartition) {
        if let Self::Accumulating(parts) = self {
            parts.push(part);
        } else {
            panic!("AggregateSink should be in Accumulating state");
        }
    }

    fn finalize(&mut self) -> Vec<MicroPartition> {
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
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let params = self.agg_sink_params.clone();
        spawner
            .spawn(
                async move {
                    let agged = input.agg(&params.sink_agg_exprs, &[])?;
                    state.push(agged);
                    Ok(state)
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
    ) -> BlockingSinkFinalizeResult {
        let params = self.agg_sink_params.clone();
        spawner
            .spawn(
                async move {
                    let all_parts: Vec<MicroPartition> = states
                        .into_iter()
                        .flat_map(|mut state| state.finalize())
                        .collect();
                    let concated = MicroPartition::concat(all_parts)?;
                    let agged = concated.agg(&params.finalize_agg_exprs, &[])?;
                    let projected = agged.eval_expression_list(&params.final_projections)?;
                    Ok(BlockingSinkOutput::Partitions(vec![projected]))
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

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(AggregateState::Accumulating(vec![]))
    }
}
