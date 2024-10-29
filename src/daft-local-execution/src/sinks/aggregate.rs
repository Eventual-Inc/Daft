use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{
    BlockingSink, BlockingSinkState, BlockingSinkStatus, DynBlockingSinkState,
};
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

    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let res = if let Self::Accumulating(ref mut parts) = self {
            std::mem::take(parts)
        } else {
            panic!("AggregateSink should be in Accumulating state");
        };
        *self = Self::Done;
        Ok(res)
    }
}

impl DynBlockingSinkState for AggregateState {
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
        state_handle: &BlockingSinkState,
    ) -> DaftResult<BlockingSinkStatus> {
        state_handle.with_state_mut::<AggregateState, _, _>(|state| {
            state.push(input.clone());
            Ok(BlockingSinkStatus::NeedMoreInput)
        })
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn DynBlockingSinkState>>,
    ) -> DaftResult<Option<PipelineResultType>> {
        let mut all_parts = vec![];
        for mut state in states {
            let state = state
                .as_any_mut()
                .downcast_mut::<AggregateState>()
                .expect("State type mismatch");
            all_parts.extend(state.finalize()?);
        }
        assert!(
            !all_parts.is_empty(),
            "We can not finalize AggregateSink with no data"
        );
        let concated = MicroPartition::concat(
            &all_parts
                .iter()
                .map(std::convert::AsRef::as_ref)
                .collect::<Vec<_>>(),
        )?;
        let agged = Arc::new(concated.agg(&self.group_by, &self.agg_exprs)?);
        Ok(Some(agged.into()))
    }

    fn name(&self) -> &'static str {
        "AggregateSink"
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    fn make_state(&self) -> DaftResult<Box<dyn DynBlockingSinkState>> {
        Ok(Box::new(AggregateState::Accumulating(vec![])))
    }
}
