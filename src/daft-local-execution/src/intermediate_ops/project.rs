use std::sync::Arc;

use common_runtime::RuntimeRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};

pub struct ProjectOperator {
    projection: Arc<Vec<ExprRef>>,
}

impl ProjectOperator {
    pub fn new(projection: Vec<ExprRef>) -> Self {
        Self {
            projection: Arc::new(projection),
        }
    }
}

impl IntermediateOperator for ProjectOperator {
    #[instrument(skip_all, name = "ProjectOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        runtime: &RuntimeRef,
    ) -> IntermediateOpExecuteResult {
        let projection = self.projection.clone();
        runtime
            .spawn(async move {
                let out = input.eval_expression_list(&projection)?;
                Ok((
                    state,
                    IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "ProjectOperator"
    }
}
