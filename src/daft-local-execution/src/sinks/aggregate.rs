use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkStatus};

pub struct AggregateSink {
    agg_exprs: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
    state: Vec<Arc<MicroPartition>>,
}

impl AggregateSink {
    pub fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>) -> Self {
        Self {
            agg_exprs,
            group_by,
            state: vec![],
        }
    }

    pub fn boxed(self) -> Box<dyn BlockingSink> {
        Box::new(self)
    }
}

impl BlockingSink for AggregateSink {
    #[instrument(skip_all, name = "AggregateSink::sink")]
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<BlockingSinkStatus> {
        self.state.push(input.clone());
        Ok(BlockingSinkStatus::NeedMoreInput)
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
    fn finalize(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        assert!(
            !self.state.is_empty(),
            "We can not finalize AggregateSink with no data"
        );
        let concated =
            MicroPartition::concat(&self.state.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
        let agged = concated.agg(&self.agg_exprs, &self.group_by)?;
        Ok(Some(Arc::new(agged)))
    }
    fn name(&self) -> &'static str {
        "AggregateSink"
    }
}
