use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use tracing::instrument;

use crate::pipeline::PipelineResultType;

use super::{IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState};

pub(crate) struct AggregateOperator {
    agg_exprs: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
}

impl AggregateOperator {
    pub(crate) fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>) -> Self {
        Self {
            agg_exprs,
            group_by,
        }
    }
}

impl IntermediateOperator for AggregateOperator {
    #[instrument(skip_all, name = "AggregateOperator::execute")]
    fn execute(
        &self,
        _idx: usize,
        input: &PipelineResultType,
        _state: &mut dyn IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult> {
        let out = input.as_data().agg(&self.agg_exprs, &self.group_by)?;
        Ok(IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(
            out,
        ))))
    }

    fn name(&self) -> &'static str {
        "AggregateOperator"
    }
}
