use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::sink::{SingleInputSink, SinkResultType};

#[derive(Clone)]
pub struct AggregateSink {
    agg_exprs: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
    parts: Vec<Arc<MicroPartition>>,
}

impl AggregateSink {
    pub fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>) -> Self {
        Self {
            agg_exprs,
            group_by,
            parts: Vec::new(),
        }
    }
}

impl SingleInputSink for AggregateSink {
    #[instrument(skip_all, name = "AggregateSink::sink")]
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        self.parts.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        true
    }

    #[instrument(skip_all, name = "AggregateSink::finalize")]
    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        let concated =
            MicroPartition::concat(&self.parts.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
        let agged = concated.agg(&self.agg_exprs, &self.group_by)?;
        Ok(vec![Arc::new(agged)])
    }
}
