use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

use crate::simple::common::IntermediateOperator;

pub struct ProjectOperator {
    projection: Vec<ExprRef>,
}

impl ProjectOperator {
    pub fn new(projection: Vec<ExprRef>) -> ProjectOperator {
        ProjectOperator { projection }
    }
}

impl IntermediateOperator for ProjectOperator {
    fn execute(&mut self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        println!("ProjectOperator::execute");
        let out = input.eval_expression_list(&self.projection)?;
        Ok(Arc::new(out))
    }
}
