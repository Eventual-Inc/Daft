use std::sync::Arc;

use common_error::DaftResult;
use daft_dsl::{AggExpr, Expr, ExprRef};
use daft_micropartition::MicroPartition;

#[derive(Clone, Debug)]
pub enum IntermediateOperatorType {
    Filter {
        predicate: ExprRef,
    },
    Project {
        projection: Vec<ExprRef>,
    },
    Aggregate {
        aggregations: Vec<AggExpr>,
        groupby: Vec<ExprRef>,
    },
}

impl IntermediateOperatorType {
    pub fn execute(&self, input: &Arc<MicroPartition>) -> DaftResult<Arc<MicroPartition>> {
        match self {
            IntermediateOperatorType::Filter { predicate } => {
                let out = input.filter(&[predicate.clone()])?;
                Ok(Arc::new(out))
            }
            IntermediateOperatorType::Project { projection } => {
                let out = input.eval_expression_list(projection)?;
                Ok(Arc::new(out))
            }
            IntermediateOperatorType::Aggregate {
                aggregations,
                groupby,
            } => {
                let exprs_from_agg_exprs = aggregations
                    .iter()
                    .map(|agg| Arc::new(Expr::Agg(agg.clone())))
                    .collect::<Vec<_>>();
                let out = input.agg(&exprs_from_agg_exprs, groupby)?;
                Ok(Arc::new(out))
            }
        }
    }
}
