use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

use super::intermediate_op::IntermediateOperator;

#[derive(Clone)]
pub struct FanoutHashOperator {
    num_partitions: usize,
    partition_expr: Vec<ExprRef>,
}

impl FanoutHashOperator {
    pub fn new(num_partitions: usize, partition_expr: Vec<ExprRef>) -> Self {
        Self {
            num_partitions,
            partition_expr,
        }
    }
}

impl IntermediateOperator for FanoutHashOperator {
    fn execute(&self, input: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        if input.len() != 1 {
            return Err(DaftError::ValueError(
                "FanoutHashOperator can only have one input".to_string(),
            ));
        }
        let input = input.first().unwrap();
        let out = input.partition_by_hash(&self.partition_expr, self.num_partitions)?;
        Ok(out.into_iter().map(Arc::new).collect())
    }
}
