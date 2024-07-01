use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_dsl::{AggExpr, Expr, ExprRef};
use daft_micropartition::MicroPartition;

use super::intermediate_op::IntermediateOperator;

#[derive(Clone)]
pub struct AggregateOperator {
    aggregations: Vec<ExprRef>,
    groupby: Vec<ExprRef>,
}

impl AggregateOperator {
    pub fn new(aggregations: Vec<AggExpr>, groupby: Vec<ExprRef>) -> Self {
        let exprs_from_agg_exprs = aggregations
            .iter()
            .map(|agg| Arc::new(Expr::Agg(agg.clone())))
            .collect::<Vec<_>>();
        Self {
            aggregations: exprs_from_agg_exprs,
            groupby,
        }
    }
}

impl IntermediateOperator for AggregateOperator {
    fn execute(&self, input: &[Arc<MicroPartition>]) -> DaftResult<Vec<Arc<MicroPartition>>> {
        if input.len() != 1 {
            return Err(DaftError::ValueError(
                "AggregateOperator can only have one input".to_string(),
            ));
        }
        let input = input.first().unwrap();

        let out = input.agg(&self.aggregations, &self.groupby)?;
        Ok(vec![Arc::new(out)])
    }
}
