use std::sync::Arc;

use common_runtime::RuntimeRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpState, IntermediateOperator, IntermediateOperatorResult,
    IntermediateOperatorResultType,
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
        input: &Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        runtime_ref: &RuntimeRef,
    ) -> IntermediateOperatorResult {
        let input = input.clone();
        let ids = self.ids.clone();
        let values = self.values.clone();
        let variable_name = self.variable_name.clone();
        let value_name = self.value_name.clone();

        runtime_ref
            .spawn(async move {
                let out = input.unpivot(&ids, &values, &variable_name, &value_name)?;
                Ok((
                    state,
                    IntermediateOperatorResultType::NeedMoreInput(Some(Arc::new(out))),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "UnpivotOperator"
    }
}
