use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::ExprRef;
use daft_micropartition::MicroPartition;
use tracing::instrument;

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
    #[instrument(skip_all, name = "AggregateOperator::execute")]
    fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        let out = input.agg(&self.agg_exprs, &self.group_by)?;
        Ok(Arc::new(out))
    }

    fn name(&self) -> &'static str {
        "AggregateOperator"
    }
}
