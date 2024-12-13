use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRecursion};
use daft_core::prelude::*;
use daft_dsl::{Expr, ExprRef, Operator};

/// Convert a boolean expression to disjunctive normal form (https://en.wikipedia.org/wiki/Disjunctive_normal_form)
pub fn to_dnf(expr: ExprRef, schema: &SchemaRef) -> DaftResult<Transformed<ExprRef>> {
    // to_dnf should only be called for boolean expressions
    debug_assert_eq!(expr.get_type(schema)?, DataType::Boolean);

    todo!()
}

pub fn split_conjunction(expr: &ExprRef) -> Vec<ExprRef> {
    let mut splits = vec![];

    expr.apply(|e| match e.as_ref() {
        Expr::BinaryOp {
            op: Operator::And, ..
        }
        | Expr::Alias(..) => Ok(TreeNodeRecursion::Continue),
        _ => {
            splits.push(e.clone());
            Ok(TreeNodeRecursion::Jump)
        }
    })
    .unwrap();

    splits
}

pub fn combine_conjunction<T: IntoIterator<Item = ExprRef>>(exprs: T) -> Option<ExprRef> {
    exprs.into_iter().reduce(|acc, e| acc.and(e))
}
