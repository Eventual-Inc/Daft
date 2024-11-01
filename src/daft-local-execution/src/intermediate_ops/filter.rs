use std::sync::Arc;

use common_runtime::RuntimeRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpState, IntermediateOperator, IntermediateOperatorResult,
    IntermediateOperatorResultType,
};

pub struct FilterOperator {
    predicate: ExprRef,
}

impl FilterOperator {
    pub fn new(predicate: ExprRef) -> Self {
        Self { predicate }
    }
}

impl IntermediateOperator for FilterOperator {
    #[instrument(skip_all, name = "FilterOperator::execute")]
    fn execute(
        &self,
        input: &Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        runtime_ref: &RuntimeRef,
    ) -> IntermediateOperatorResult {
        let input = input.clone();
        let predicate = self.predicate.clone();
        runtime_ref
            .spawn(async move {
                let filtered = input.filter(&[predicate.clone()])?;
                Ok((
                    state,
                    IntermediateOperatorResultType::NeedMoreInput(Some(Arc::new(filtered))),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "FilterOperator"
    }
}
