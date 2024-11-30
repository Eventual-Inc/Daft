use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
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

struct AggParams {
    agg_exprs: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
}

pub struct AggregateSink {
    agg_sink_params: Arc<AggParams>,
}

impl AggregateSink {
    pub fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>) -> Self {
        Self {
            agg_sink_params: Arc::new(AggParams {
                agg_exprs,
                group_by,
            }),
        }
    }
}

impl BlockingSink for AggregateSink {
    #[instrument(skip_all, name = "AggregateSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        _runtime: &RuntimeRef,
    ) -> BlockingSinkSinkResult {
        state
            .as_any_mut()
            .downcast_mut::<AggregateState>()
            .expect("AggregateSink should have AggregateState")
            .push(input);
        Ok(BlockingSinkStatus::NeedMoreInput(state)).into()
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
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
                        .downcast_mut::<AggregateState>()
                        .expect("AggregateSink should have AggregateState")
                        .finalize()
                });
                let concated = MicroPartition::concat(all_parts)?;
                let agged = Arc::new(concated.agg(&params.agg_exprs, &params.group_by)?);
                Ok(Some(agged))
            })
            .into()
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
}
