use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

use super::intermediate_op::IntermediateOperator;

#[derive(Clone)]
pub struct ProjectOperator {
    projection: Vec<ExprRef>,
}

impl ProjectOperator {
    pub fn new(projection: Vec<ExprRef>) -> Self {
        Self { projection }
    }
}

impl IntermediateOperator for ProjectOperator {
    fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        log::debug!("ProjectOperator::execute");
        let out = input.eval_expression_list(&self.projection)?;
        Ok(Arc::new(out))
    }

    fn name(&self) -> &'static str {
        "ProjectOperator"
    }
}
