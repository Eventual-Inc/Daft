use std::sync::Arc;

use common_runtime::RuntimeRef;
use daft_dsl::ExprRef;
use daft_functions::list::explode;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpState, IntermediateOperator, IntermediateOperatorResult,
    IntermediateOperatorResultType,
};

pub(crate) struct ExplodeOperator {
    to_explode: Vec<ExprRef>,
}

impl ExplodeOperator {
    pub(crate) fn new(to_explode: Vec<ExprRef>) -> Self {
        Self {
            to_explode: to_explode.into_iter().map(explode).collect(),
        }
    }
}

impl IntermediateOperator for ExplodeOperator {
    #[instrument(skip_all, name = "ExplodeOperator::execute")]
    fn execute(
        &self,
        input: &Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        runtime_ref: &RuntimeRef,
    ) -> IntermediateOperatorResult {
        let input = input.clone();
        let to_explode = self.to_explode.clone();
        runtime_ref
            .spawn(async move {
                let out = input.explode(&to_explode)?;
                Ok((
                    state,
                    IntermediateOperatorResultType::NeedMoreInput(Some(Arc::new(out))),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "ExplodeOperator"
    }
}
