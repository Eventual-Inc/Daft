use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use crate::pipeline::PipelineResultType;

use super::blocking_sink::{BlockingSink, BlockingSinkState, BlockingSinkStatus};

enum AggregateState {
    Accumulating(Vec<Arc<MicroPartition>>),
    Done(Vec<Arc<MicroPartition>>),
}

impl BlockingSinkState for AggregateState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn finalize(&mut self) {
        if let AggregateState::Accumulating(parts) = self {
            *self = AggregateState::Done(std::mem::take(parts));
        }
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

    pub fn arced(self) -> Arc<dyn BlockingSink> {
        Arc::new(self)
    }
}

impl BlockingSink for AggregateSink {
    #[instrument(skip_all, name = "AggregateSink::sink")]
    fn sink(
        &self,
        input: &Arc<MicroPartition>,
        state: &mut dyn BlockingSinkState,
    ) -> DaftResult<BlockingSinkStatus> {
        let aggregate_state = state.as_any_mut().downcast_mut::<AggregateState>().unwrap();
        if let AggregateState::Accumulating(parts) = aggregate_state {
            parts.push(input.clone());
            Ok(BlockingSinkStatus::NeedMoreInput)
        } else {
            panic!("AggregateSink should be in Accumulating state");
        }
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
    fn finalize(
        &self,
        states: &[&dyn BlockingSinkState],
    ) -> DaftResult<Option<PipelineResultType>> {
        let parts = states
            .iter()
            .flat_map(|state| {
                let aggregate_state = state.as_any().downcast_ref::<AggregateState>().unwrap();
                if let AggregateState::Done(parts) = aggregate_state {
                    parts
                } else {
                    panic!("AggregateState should be in Done state");
                }
            })
            .collect::<Vec<_>>();

        let concated =
            MicroPartition::concat(&parts.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
        let agged = Arc::new(concated.agg(&self.agg_exprs, &self.group_by)?);
        Ok(Some(agged.into()))
    }

    fn name(&self) -> &'static str {
        "AggregateSink"
    }

    fn make_state(&self) -> DaftResult<Box<dyn super::blocking_sink::BlockingSinkState>> {
        Ok(Box::new(AggregateState::Accumulating(vec![])))
    }
}
