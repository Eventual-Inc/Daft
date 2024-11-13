use std::sync::Arc;

use common_runtime::RuntimeRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
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
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        runtime: &RuntimeRef,
    ) -> IntermediateOpExecuteResult {
        let predicate = self.predicate.clone();
        runtime
            .spawn(async move {
                let out = input.filter(&[predicate])?;
                Ok((
                    state,
                    IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "FilterOperator"
    }
}
