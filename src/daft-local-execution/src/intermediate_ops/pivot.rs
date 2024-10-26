use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState,
};

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
        input: &Arc<MicroPartition>,
        _state: &IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult> {
        let out = input.pivot(
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

    // Don't buffer the input to pivot because it depends on the full output of agg.
    fn morsel_size(&self) -> Option<usize> {
        None
    }
}
