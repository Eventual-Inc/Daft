use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

use super::intermediate_op::IntermediateOperator;

#[derive(Clone)]
pub struct FilterOperator {
    predicate: ExprRef,
}

impl FilterOperator {
    pub fn new(predicate: ExprRef) -> Self {
        Self { predicate }
    }
}

impl IntermediateOperator for FilterOperator {
    fn execute(&self, input: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        if input.len() != 1 {
            return Err(DaftError::ValueError(
                "FilterOperator can only have one input".to_string(),
            ));
        }
        let input = input.first().unwrap();
        let out = input.filter(&[self.predicate.clone()])?;
        Ok(vec![Arc::new(out)])
    }
}
