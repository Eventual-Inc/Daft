use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_functions::list::explode;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState,
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
        _idx: usize,
        input: &Arc<MicroPartition>,
        _state: &IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult> {
        let out = input.explode(&self.to_explode)?;
        Ok(IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(
            out,
        ))))
    }

    fn name(&self) -> &'static str {
        "ExplodeOperator"
    }
}
