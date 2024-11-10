use std::sync::Arc;

use common_error::DaftResult;
use common_runtime::RuntimeRef;
use daft_dsl::ExprRef;
use daft_functions::list::explode;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpState, IntermediateOperator, IntermediateOperatorResult,
};
use crate::OperatorOutput;

pub struct ExplodeOperator {
    to_explode: Arc<Vec<ExprRef>>,
}

impl ExplodeOperator {
    pub fn new(to_explode: Vec<ExprRef>) -> Self {
        Self {
            to_explode: Arc::new(to_explode.into_iter().map(explode).collect()),
        }
    }
}

impl IntermediateOperator for ExplodeOperator {
    #[instrument(skip_all, name = "ExplodeOperator::execute")]
    fn execute(
        &self,
        input: &Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        runtime: &RuntimeRef,
    ) -> OperatorOutput<DaftResult<(Box<dyn IntermediateOpState>, IntermediateOperatorResult)>>
    {
        let input = input.clone();
        let to_explode = self.to_explode.clone();
        runtime
            .spawn(async move {
                let out = input.explode(&to_explode)?;
                Ok((
                    state,
                    IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "ExplodeOperator"
    }
}
