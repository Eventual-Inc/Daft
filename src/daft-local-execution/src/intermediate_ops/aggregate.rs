use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::{
    intermediate_op::{IntermediateOpSpec, IntermediateOperator, OperatorOutput},
    state::OperatorTaskState,
};

pub struct AggregateOperator {
    agg_exprs: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
}

impl AggregateSpec {
    pub fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>) -> Self {
        Self {
            agg_exprs,
            group_by,
        }
    }
}

impl IntermediateOpSpec for AggregateSpec {
    fn to_operator(&self) -> Box<dyn IntermediateOperator> {
        Box::new(AggregateOperator {
            spec: self.clone(),
            state: OperatorTaskState::new(),
        })
    }
}

pub struct AggregateOperator {
    spec: AggregateSpec,
    state: OperatorTaskState,
}

impl IntermediateOperator for AggregateOperator {
    #[instrument(skip_all, name = "AggregateOperator::execute")]
    fn execute(&mut self, input: &Arc<MicroPartition>) -> DaftResult<OperatorOutput> {
        let agged = input.agg(&self.spec.agg_exprs, &self.spec.group_by)?;
        self.state.add(Arc::new(agged));

        match self.state.try_clear() {
            Some(part) => Ok(OperatorOutput::Ready(part?)),
            None => Ok(OperatorOutput::NeedMoreInput),
        }
    }

    fn finalize(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        match self.state.clear() {
            Some(part) => part.map(Some),
            None => Ok(None),
        }
    }

    fn name(&self) -> &'static str {
        "AggregateOperator"
    }
}
