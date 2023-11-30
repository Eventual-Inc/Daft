use std::{collections::{HashMap, HashSet}, sync::Arc};

use common_error::DaftResult;
use daft_dsl::{
    col,
    common_treenode::{Transformed, TreeNode, VisitRecursion},
    functions::{partitioning, FunctionExpr},
    null_lit, Expr, Operator, optimization::{split_conjuction, conjuct},
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
) -> DaftResult<Vec<Expr>> {
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

    let with_part_cols = predicate.transform(&|expr| {
        use Operator::*;
        match expr {
            Expr::BinaryOp {
                op,
                ref left, ref right } if matches!(op, Eq | NotEq | Lt | LtEq | Gt | GtEq)=> {
                if let Expr::Column(col_name) = left.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && let Some(new_expr) = apply_partitioning_expr(right.as_ref().clone(), tfm) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op, left: col(pfield.field.name.as_str()).into(), right: new_expr.into() }));
                    }
                    Ok(Transformed::No(expr))
                } else if let Expr::Column(col_name) = right.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && let Some(new_expr) = apply_partitioning_expr(left.as_ref().clone(), tfm) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op, left: new_expr.into(), right: col(pfield.field.name.as_str()).into() }));
                    }
                    Ok(Transformed::No(expr))
                } else {
                    Ok(Transformed::No(expr))
                }
            },
            Expr::IsNull(ref expr) if let Expr::Column(col_name) = expr.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) => {
                Ok(Transformed::Yes(Expr::IsNull(col(pfield.field.name.as_str()).into())))
            },
            _ => Ok(Transformed::No(expr))
        }
    })?;

    let p_keys = HashSet::<&str>::from_iter(pfields.iter().map(|p| p.field.name.as_ref()));

    let split = split_conjuction(&with_part_cols);
    let filtered = split.into_iter().filter(|p| {
        let mut keep = true;
        p.apply(&mut |e| {
            if let Expr::Column(col_name) = e && !p_keys.contains(col_name.as_ref()) {
                keep = false;

            }
            Ok(VisitRecursion::Continue)
        }).unwrap();
        keep
    }).cloned().collect::<Vec<_>>();

    Ok(filtered)
}
