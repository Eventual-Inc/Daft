use common_error::DaftResult;
use daft_dsl::{
    common_treenode::{Transformed, TreeNode},
    functions::FunctionExpr,
    Expr,
};

use crate::PartitionField;

fn unalias(expr: Expr) -> DaftResult<Expr> {
    expr.transform(&|e| {
        if let Expr::Alias(e, _) = e {
            Ok(Transformed::Yes(e.as_ref().clone()))
        } else {
            Ok(Transformed::No(e))
        }
    })
}

fn rewrite_predicate_for_partitioning(
    predicate: Expr,
    pfields: &[PartitionField],
) -> DaftResult<Expr> {
    predicate.transform(&|expr| {
        if let Expr::Function {
            func: FunctionExpr::Temporal(..),
            inputs: _,
        } = expr
        {
            Ok(Transformed::No(expr))
        } else {
            Ok(Transformed::No(expr))
        }
    })
}
