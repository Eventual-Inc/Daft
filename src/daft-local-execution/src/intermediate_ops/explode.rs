use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_functions::list::explode;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState,
};
use crate::pipeline::PipelineResultType;

pub struct ExplodeOperator {
    to_explode: Vec<ExprRef>,
}

impl ExplodeOperator {
    pub fn new(to_explode: Vec<ExprRef>) -> Self {
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
        input: &PipelineResultType,
        _state: &IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult> {
        let out = input.as_data().explode(&self.to_explode)?;
        Ok(IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(
            out,
        ))))
    }

    fn name(&self) -> &'static str {
        "ExplodeOperator"
    }
}
