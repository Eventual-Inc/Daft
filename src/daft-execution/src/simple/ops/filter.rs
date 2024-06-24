use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

use crate::simple::common::IntermediateOperator;

pub struct FilterOperator {
    predicate: ExprRef,
}

impl FilterOperator {
    pub fn new(predicate: ExprRef) -> FilterOperator {
        FilterOperator { predicate }
    }
}

impl IntermediateOperator for FilterOperator {
    fn execute(&mut self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        println!("FilterOperator::execute");
        let out = input.filter(&[self.predicate.clone()])?;
        Ok(Arc::new(out))
    }
}
