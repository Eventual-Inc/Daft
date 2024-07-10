use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;

use super::intermediate_op::IntermediateOperator;

#[derive(Clone)]
pub struct AggregateOperator {
    agg_exprs: Vec<ExprRef>,
    group_by: Vec<ExprRef>,
}

impl AggregateOperator {
    pub fn new(agg_exprs: Vec<ExprRef>, group_by: Vec<ExprRef>) -> Self {
        Self {
            agg_exprs,
            group_by,
        }
    }
}

impl IntermediateOperator for AggregateOperator {
    fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        log::debug!("AggregateOperator::execute");
        let out = input.agg(&self.agg_exprs, &self.group_by)?;
        Ok(Arc::new(out))
    }

    fn name(&self) -> String {
        "AggregateOperator".to_string()
    }
}
