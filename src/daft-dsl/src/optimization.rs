use std::collections::HashMap;

use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};

use crate::{expr::ResolvedColumn, Column, Expr, ExprRef};

pub fn get_required_columns(e: &ExprRef) -> Vec<String> {
    let mut cols = vec![];
    e.apply(&mut |expr: &ExprRef| {
        if let Expr::Column(Column::Resolved(ResolvedColumn::Basic(ref name))) = &**expr {
            cols.push(name.to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("Error occurred when visiting for required columns");
    cols
}

pub fn requires_computation(e: &Expr) -> bool {
    // Returns whether or not this expression runs any computation on the underlying data
    match e {
        Expr::Alias(child, _) => requires_computation(child),
        Expr::Column(..) | Expr::Literal(_) => false,
        Expr::Agg(..)
        | Expr::BinaryOp { .. }
        | Expr::Cast(..)
        | Expr::Function { .. }
        | Expr::ScalarFunction { .. }
        | Expr::Not(..)
        | Expr::IsNull(..)
        | Expr::NotNull(..)
        | Expr::FillNull(..)
        | Expr::IsIn { .. }
        | Expr::List { .. }
        | Expr::Between { .. }
        | Expr::IfElse { .. }
        | Expr::Subquery { .. }
        | Expr::InSubquery { .. }
        | Expr::Exists(..)
        | Expr::Over(..)
        | Expr::WindowFunction(..) => true,
    }
}

pub fn replace_columns_with_expressions(
    expr: ExprRef,
    replace_map: &HashMap<String, ExprRef>,
) -> ExprRef {
    let transformed = expr
        .transform(&|e: ExprRef| {
            if let Expr::Column(Column::Resolved(ResolvedColumn::Basic(ref name))) = e.as_ref()
                && let Some(tgt) = replace_map.get(name.as_ref())
            {
                Ok(Transformed::yes(tgt.clone()))
            } else {
                Ok(Transformed::no(e))
            }
        })
        .expect("Error occurred when rewriting column expressions");
    transformed.data
}
