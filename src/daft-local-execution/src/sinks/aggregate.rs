use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkState, BlockingSinkStatus};
use crate::{pipeline::PipelineResultType, NUM_CPUS};

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
    agg_exprs: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
}

impl AggregateSink {
    pub fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>) -> Self {
        Self {
            agg_exprs,
            group_by,
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
        state
            .as_any_mut()
            .downcast_mut::<AggregateState>()
            .expect("AggregateSink should have AggregateState")
            .push(input.clone());
        Ok(BlockingSinkStatus::NeedMoreInput(state))
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
    ) -> DaftResult<Option<PipelineResultType>> {
        let all_parts = states.into_iter().flat_map(|mut state| {
            state
                .as_any_mut()
                .downcast_mut::<AggregateState>()
                .expect("AggregateSink should have AggregateState")
                .finalize()
        });
        let concated = MicroPartition::concat(all_parts)?;
        let agged = Arc::new(concated.agg(&self.agg_exprs, &self.group_by)?);
        Ok(Some(agged.into()))
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
