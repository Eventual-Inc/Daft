use std::sync::Arc;

use common_runtime::RuntimeRef;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};

struct UnpivotParams {
    ids: Vec<ExprRef>,
    values: Vec<ExprRef>,
    variable_name: String,
    value_name: String,
}
pub struct UnpivotOperator {
    params: Arc<UnpivotParams>,
}

impl UnpivotOperator {
    pub fn new(
        ids: Vec<ExprRef>,
        values: Vec<ExprRef>,
        variable_name: String,
        value_name: String,
    ) -> Self {
        Self {
            params: Arc::new(UnpivotParams {
                ids,
                values,
                variable_name,
                value_name,
            }),
        }
    }
}

impl IntermediateOperator for UnpivotOperator {
    #[instrument(skip_all, name = "UnpivotOperator::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn IntermediateOpState>,
        runtime: &RuntimeRef,
    ) -> IntermediateOpExecuteResult {
        let params = self.params.clone();
        runtime
            .spawn(async move {
                let out = input.unpivot(
                    &params.ids,
                    &params.values,
                    &params.variable_name,
                    &params.value_name,
                )?;
                Ok((
                    state,
                    IntermediateOperatorResult::NeedMoreInput(Some(Arc::new(out))),
                ))
            })
            .into()
    }

    fn name(&self) -> &'static str {
        "UnpivotOperator"
    }
}
