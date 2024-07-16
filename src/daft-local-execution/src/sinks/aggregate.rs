use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

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
    fn sink(&mut self, input: &Arc<MicroPartition>) -> DaftResult<SinkResultType> {
        log::debug!("AggregateSink::sink");

        self.parts.push(input.clone());
        Ok(SinkResultType::NeedMoreInput)
    }

    fn in_order(&self) -> bool {
        true
    }

    fn finalize(&mut self) -> DaftResult<Vec<Arc<MicroPartition>>> {
        log::debug!("AggregateSink::finalize");

        let concated =
            MicroPartition::concat(&self.parts.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
        let agged = concated.agg(&self.agg_exprs, &self.group_by)?;
        Ok(vec![Arc::new(agged)])
    }
}
