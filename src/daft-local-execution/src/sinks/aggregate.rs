use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::{
    blocking_sink::{BlockingSink, BlockingSinkStatus},
    sink::{Sink, SinkResultType},
};

enum AggregateState {
    Accumulating(Vec<Arc<MicroPartition>>),
    Done(Arc<MicroPartition>),
}

pub struct AggregateSink {
    agg_exprs: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
    state: AggregateState,
}

impl AggregateSink {
    pub fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>) -> Self {
        Self {
            agg_exprs,
            group_by,
            state: AggregateState::Accumulating(vec![]),
        }
    }
}

impl BlockingSink for AggregateSink {
    #[instrument(skip_all, name = "AggregateSink::sink")]
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus> {
        if let AggregateState::Accumulating(parts) = &mut self.state {
            parts.push(input.clone());
            Ok(BlockingSinkStatus::NeedMoreInput)
        } else {
            panic!("sink must be in Accumulating phase")
        }
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
    fn finalize(&mut self) -> DaftResult<()> {
        if let AggregateState::Accumulating(parts) = &mut self.state {
            let concated =
                MicroPartition::concat(&parts.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
            let agged = concated.agg(&self.agg_exprs, &self.group_by)?;
            self.state = AggregateState::Done(Arc::new(agged));
            Ok(())
        } else {
            panic!("finalize must be in Accumulating phase")
        }
    }
    fn name(&self) -> &'static str {
        "AggregateSink"
    }
}
