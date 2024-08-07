use std::sync::Arc;

use common_error::DaftResult;
use daft_core::schema::SchemaRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::blocking_sink::{BlockingSink, BlockingSinkStatus};

enum AggregateState {
    Accumulating(Vec<Arc<MicroPartition>>),
    #[allow(dead_code)]
    Done(Arc<MicroPartition>),
}

pub struct AggregateSink {
    agg_exprs: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
    state: AggregateState,
    schema: SchemaRef,
}

impl AggregateSink {
    pub fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>, schema: SchemaRef) -> Self {
        Self {
            agg_exprs,
            group_by,
            state: AggregateState::Accumulating(vec![]),
            schema,
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
    fn finalize(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        if let AggregateState::Accumulating(parts) = &mut self.state {
            if parts.is_empty() {
                let empty = MicroPartition::empty(Some(self.schema.clone()));
                self.state = AggregateState::Done(Arc::new(empty));
                return Ok(());
            }
            let concated =
                MicroPartition::concat(&parts.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
            let agged = Arc::new(concated.agg(&self.agg_exprs, &self.group_by)?);
            self.state = AggregateState::Done(agged.clone());
            Ok(Some(agged))
        } else {
            panic!("AggregateSink should be in Accumulating state");
        }
    }
    fn name(&self) -> &'static str {
        "AggregateSink"
    }
}
