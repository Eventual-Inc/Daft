use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use tracing::instrument;

use crate::channel::PipelineOutput;

use super::{
    intermediate_op::{IntermediateOperator, IntermediateOperatorOutput},
    state::OperatorState,
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
        _index: usize,
        input: &PipelineOutput,
        state: &mut Box<dyn OperatorState>,
    ) -> DaftResult<IntermediateOperatorOutput> {
        let mp = input.as_micro_partition()?;
        state.add(mp);
        if let Some(part) = state.try_clear() {
            let agged = part?.filter(&[self.predicate.clone()])?;
            Ok(IntermediateOperatorOutput::NeedMoreInput(
                Arc::new(agged).into(),
            ))
        } else {
            Ok(IntermediateOperatorOutput::NeedMoreInput(None))
        }
    }

    fn finalize(&self, state: &mut Box<dyn OperatorState>) -> DaftResult<Option<PipelineOutput>> {
        if let Some(part) = state.clear() {
            let agged = part?.filter(&[self.predicate.clone()])?;
            Ok(Some(Arc::new(agged).into()))
        } else {
            Ok(None)
        }
    }

    fn name(&self) -> &'static str {
        "FilterOperator"
    }
}
