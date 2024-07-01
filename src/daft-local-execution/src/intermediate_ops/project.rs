use std::sync::Arc;

use common_error::{DaftError, DaftResult};
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
    fn execute(&self, input: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        if input.len() != 1 {
            return Err(DaftError::ValueError(
                "ProjectOperator can only have one input".to_string(),
            ));
        }
        let input = input.first().unwrap();
        let out = input.eval_expression_list(&self.projection)?;
        Ok(vec![Arc::new(out)])
    }
}
