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
pub struct ProjectSpec {
    projection: Vec<ExprRef>,
}

impl ProjectSpec {
    pub fn new(projection: Vec<ExprRef>) -> Self {
        Self { projection }
    }
}

impl IntermediateOpSpec for ProjectSpec {
    fn to_operator(&self) -> Box<dyn IntermediateOperator> {
        Box::new(ProjectOperator {
            spec: self.clone(),
            state: OperatorTaskState::new(),
        })
    }
}

pub struct ProjectOperator {
    spec: ProjectSpec,
    state: OperatorTaskState,
}

impl IntermediateOperator for ProjectOperator {
    #[instrument(skip_all, name = "ProjectOperator::execute")]
    fn execute(&mut self, input: &Arc<MicroPartition>) -> DaftResult<OperatorOutput> {
        let projected = input.eval_expression_list(&self.spec.projection)?;
        self.state.add(Arc::new(projected));

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
        "ProjectOperator"
    }
}
