use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkStatus};
use crate::pipeline::PipelineResultType;

enum AggregateState {
    Accumulating(Vec<Arc<MicroPartition>>),
    #[allow(dead_code)]
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

    pub fn boxed(self) -> Box<dyn BlockingSink> {
        Box::new(self)
    }
}

impl BlockingSink for AggregateSink {
    #[instrument(skip_all, name = "AggregateSink::sink")]
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus> {
        if let AggregateState::Accumulating(parts) = &mut self.state {
            parts.push(input.clone());
            Ok(BlockingSinkStatus::NeedMoreInput)
        } else {
            panic!("AggregateSink should be in Accumulating state");
        }
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
    fn finalize(&mut self) -> DaftResult<Option<PipelineResultType>> {
        if let AggregateState::Accumulating(parts) = &mut self.state {
            assert!(
                !parts.is_empty(),
                "We can not finalize AggregateSink with no data"
            );
            let concated = MicroPartition::concat(
                &parts
                    .iter()
                    .map(std::convert::AsRef::as_ref)
                    .collect::<Vec<_>>(),
            )?;
            let agged = Arc::new(concated.agg(&self.agg_exprs, &self.group_by)?);
            self.state = AggregateState::Done(agged.clone());
            Ok(Some(agged.into()))
        } else {
            panic!("AggregateSink should be in Accumulating state");
        }
    }
    fn name(&self) -> &'static str {
        "AggregateSink"
    }
}
