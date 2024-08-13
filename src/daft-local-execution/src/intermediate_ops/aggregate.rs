use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use tracing::instrument;

use crate::pipeline::PipelineOutput;

use super::{
    intermediate_op::{IntermediateOperator, IntermediateOperatorOutput},
    state::OperatorState,
};

pub struct AggregateOperator {
    agg_exprs: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
}

impl AggregateOperator {
    pub fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>) -> Self {
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
        _index: usize,
        input: &PipelineOutput,
        state: &mut Box<dyn OperatorState>,
    ) -> DaftResult<IntermediateOperatorOutput> {
        let mp = input.as_micro_partition()?;
        state.add(mp);
        if let Some(part) = state.try_clear() {
            let agged = part?.agg(&self.agg_exprs, &self.group_by)?;
            Ok(IntermediateOperatorOutput::NeedMoreInput(
                Arc::new(agged).into(),
            ))
        } else {
            Ok(IntermediateOperatorOutput::NeedMoreInput(None))
        }
    }

    fn finalize(&self, state: &mut Box<dyn OperatorState>) -> DaftResult<Option<PipelineOutput>> {
        if let Some(part) = state.clear() {
            let agged = part?.agg(&self.agg_exprs, &self.group_by)?;
            Ok(Some(Arc::new(agged).into()))
        } else {
            Ok(None)
        }
    }

    fn name(&self) -> &'static str {
        "AggregateOperator"
    }
}
