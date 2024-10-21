use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState,
};
use crate::pipeline::PipelineResultType;

pub struct PivotOperator {
    group_by: Vec<ExprRef>,
    pivot_col: ExprRef,
    values_col: ExprRef,
    names: Vec<String>,
}

impl PivotOperator {
    pub fn new(
        group_by: Vec<ExprRef>,
        pivot_col: ExprRef,
        values_col: ExprRef,
        names: Vec<String>,
    ) -> Self {
        Self {
            group_by,
            pivot_col,
            values_col,
            names,
        }
    }
}

impl IntermediateOperator for PivotOperator {
    #[instrument(skip_all, name = "PivotOperator::execute")]
    fn execute(
        &self,
        _idx: usize,
        input: &PipelineResultType,
        _state: Option<&mut Box<dyn IntermediateOperatorState>>,
    ) -> DaftResult<IntermediateOperatorResult> {
        let out = input.as_data().pivot(
            &self.group_by,
            self.pivot_col.clone(),
            self.values_col.clone(),
            self.names.clone(),
        )?;
        Ok(IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(
            out,
        ))))
    }

    fn name(&self) -> &'static str {
        "PivotOperator"
    }
}
