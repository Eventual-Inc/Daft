use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use tracing::instrument;

use crate::pipeline::PipelineOutput;

use super::{
    intermediate_op::{IntermediateOperator, IntermediateOperatorOutput},
    state::OperatorState,
};

pub struct ProjectOperator {
    projection: Vec<ExprRef>,
}

impl ProjectOperator {
    pub fn new(projection: Vec<ExprRef>) -> Self {
        Self { projection }
    }
}

impl IntermediateOperator for ProjectOperator {
    #[instrument(skip_all, name = "ProjectOperator::execute")]
    fn execute(
        &self,
        _index: usize,
        input: &PipelineOutput,
        state: &mut Box<dyn OperatorState>,
    ) -> DaftResult<IntermediateOperatorOutput> {
        let mp = input.as_micro_partition()?;
        state.add(mp);
        if let Some(part) = state.try_clear() {
            let agged = part?.eval_expression_list(&self.projection)?;
            Ok(IntermediateOperatorOutput::NeedMoreInput(
                Arc::new(agged).into(),
            ))
        } else {
            Ok(IntermediateOperatorOutput::NeedMoreInput(None))
        }
    }

    fn finalize(&self, state: &mut Box<dyn OperatorState>) -> DaftResult<Option<PipelineOutput>> {
        if let Some(part) = state.clear() {
            let agged = part?.eval_expression_list(&self.projection)?;
            Ok(Some(Arc::new(agged).into()))
        } else {
            Ok(None)
        }
    }

    fn name(&self) -> &'static str {
        "ProjectOperator"
    }
}
