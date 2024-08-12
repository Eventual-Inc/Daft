use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::{
    intermediate_op::{IntermediateOpSpec, IntermediateOperator, OperatorOutput},
    state::OperatorTaskState,
};

#[derive(Clone)]
pub struct FilterSpec {
    predicate: ExprRef,
}

impl FilterSpec {
    pub fn new(predicate: ExprRef) -> Self {
        Self { predicate }
    }
}

impl IntermediateOpSpec for FilterSpec {
    fn to_operator(&self) -> Box<dyn IntermediateOperator> {
        Box::new(FilterOperator {
            spec: self.clone(),
            state: OperatorTaskState::new(),
        })
    }
}

pub struct FilterOperator {
    spec: FilterSpec,
    state: OperatorTaskState,
}

impl IntermediateOperator for FilterOperator {
    #[instrument(skip_all, name = "FilterOperator::execute")]
    fn execute(&mut self, input: &Arc<MicroPartition>) -> DaftResult<OperatorOutput> {
        let filtered = input.filter(&[self.spec.predicate.clone()])?;
        self.state.add(Arc::new(filtered));

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
        "FilterOperator"
    }
}
