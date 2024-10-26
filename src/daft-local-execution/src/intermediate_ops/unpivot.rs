use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOperator, IntermediateOperatorResult, IntermediateOperatorState,
};

pub struct UnpivotOperator {
    ids: Vec<ExprRef>,
    values: Vec<ExprRef>,
    variable_name: String,
    value_name: String,
}

impl UnpivotOperator {
    pub fn new(
        ids: Vec<ExprRef>,
        values: Vec<ExprRef>,
        variable_name: String,
        value_name: String,
    ) -> Self {
        Self {
            ids,
            values,
            variable_name,
            value_name,
        }
    }
}

impl IntermediateOperator for UnpivotOperator {
    #[instrument(skip_all, name = "UnpivotOperator::execute")]
    fn execute(
        &self,
        _idx: usize,
        input: &Arc<MicroPartition>,
        _state: &IntermediateOperatorState,
    ) -> DaftResult<IntermediateOperatorResult> {
        let out = input.unpivot(
            &self.ids,
            &self.values,
            &self.variable_name,
            &self.value_name,
        )?;
        Ok(IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(
            out,
        ))))
    }

    fn name(&self) -> &'static str {
        "UnpivotOperator"
    }
}
