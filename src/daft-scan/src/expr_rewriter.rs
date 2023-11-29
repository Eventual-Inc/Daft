use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_dsl::{
    col,
    common_treenode::{Transformed, TreeNode},
    functions::{partitioning, FunctionExpr},
    null_lit, Expr, Operator,
};

use crate::{PartitionField, PartitionTransform};

fn unalias(expr: Expr) -> DaftResult<Expr> {
    expr.transform(&|e| {
        if let Expr::Alias(e, _) = e {
            Ok(Transformed::Yes(e.as_ref().clone()))
        } else {
            Ok(Transformed::No(e))
        }
    })
}

fn apply_partitioning_expr(expr: Expr, tfm: PartitionTransform) -> Option<Expr> {
    use PartitionTransform::*;
    match tfm {
        Identity => Some(expr),
        Year => Some(partitioning::years(expr)),
        Month => Some(partitioning::months(expr)),
        Day => Some(partitioning::days(expr)),
        Hour => Some(partitioning::hours(expr)),
        Void => Some(null_lit()),
        _ => None,
    }
}

pub fn rewrite_predicate_for_partitioning(
    predicate: Expr,
    pfields: &[PartitionField],
) -> DaftResult<Expr> {
    if pfields.is_empty() {
        todo!("no predicate")
    }
    let predicate = unalias(predicate)?;

    let source_to_pfield = {
        let mut map = HashMap::with_capacity(pfields.len());
        for pf in pfields.iter() {
            if let Some(ref source_field) = pf.source_field {
                map.insert(source_field.name.as_str(), pf);
            }
        }
        map
    };

    predicate.transform(&|expr| {
        use Operator::*;
        match expr {
            Expr::BinaryOp {
                op,
                ref left, ref right } if matches!(op, Eq | NotEq | Lt | LtEq | Gt | GtEq)=> {

                if let Expr::Column(col_name) = left.as_ref() {
                    if let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                        if let Some(tfm) = pfield.transform && let Some(new_expr) = apply_partitioning_expr(right.as_ref().clone(), tfm) {
                            return Ok(Transformed::Yes(Expr::BinaryOp { op, left: col(pfield.field.name.as_str()).into(), right: new_expr.into() }));
                        }
                    }
                    Ok(Transformed::No(expr))
                } else if let Expr::Column(col_name) = right.as_ref() {
                    Ok(Transformed::No(expr))
                } else {
                    Ok(Transformed::No(expr))
                }
            },
            _ => Ok(Transformed::No(expr))
        }
    })
}
