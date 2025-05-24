use std::sync::Arc;

use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::intermediate_op::{
    IntermediateOpExecuteResult, IntermediateOpState, IntermediateOperator,
    IntermediateOperatorResult,
};
use crate::ExecutionTaskSpawner;

struct UnpivotParams {
    ids: Vec<BoundExpr>,
    values: Vec<BoundExpr>,
    variable_name: String,
    value_name: String,
}
pub struct UnpivotOperator {
    params: Arc<UnpivotParams>,
}

impl UnpivotOperator {
    pub fn new(
        ids: Vec<BoundExpr>,
        values: Vec<BoundExpr>,
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
        task_spawner: &ExecutionTaskSpawner,
    ) -> IntermediateOpExecuteResult {
        let params = self.params.clone();
        task_spawner
            .spawn(
                async move {
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
                },
                Span::current(),
            )
            .into()
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push(format!(
            "Unpivot: {}",
            self.params.values.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!(
            "Ids = {}",
            self.params.ids.iter().map(|e| e.to_string()).join(", ")
        ));
        res.push(format!("Variable name = {}", self.params.variable_name));
        res.push(format!("Value name = {}", self.params.value_name));
        res
    }

    fn name(&self) -> &'static str {
        "Unpivot"
    }
}
