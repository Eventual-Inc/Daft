use std::collections::{HashMap, HashSet};

use common_error::DaftResult;
use daft_dsl::{
    col,
    common_treenode::{Transformed, TreeNode, VisitRecursion},
    functions::partitioning,
    null_lit,
    optimization::{conjuct, split_conjuction},
    Expr, Operator,
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

fn apply_partitioning_expr(expr: Expr, pfield: &PartitionField) -> Option<Expr> {
    use PartitionTransform::*;
    match pfield.transform {
        Some(Identity) => Some(
            pfield
                .source_field
                .as_ref()
                .map(|s| expr.cast(&s.dtype))
                .unwrap_or(expr),
        ),
        Some(Year) => Some(partitioning::years(expr)),
        Some(Month) => Some(partitioning::months(expr)),
        Some(Day) => Some(partitioning::days(expr)),
        Some(Hour) => Some(partitioning::hours(expr)),
        Some(Void) => Some(null_lit()),
        Some(IcebergBucket(n)) => Some(partitioning::iceberg_bucket(
            expr.cast(&pfield.source_field.as_ref().unwrap().dtype),
            n as i32,
        )),
        Some(IcebergTruncate(w)) => Some(partitioning::iceberg_truncate(
            expr.cast(&pfield.source_field.as_ref().unwrap().dtype),
            w as i64,
        )),
        _ => None,
    }
}

pub fn rewrite_predicate_for_partitioning(
    predicate: Expr,
    pfields: &[PartitionField],
) -> DaftResult<Option<Expr>> {
    if pfields.is_empty() {
        return Ok(None);
    }

    let predicate = unalias(predicate)?;

    let source_to_pfield = {
        let mut map = HashMap::with_capacity(pfields.len());
        for pf in pfields.iter() {
            if let Some(ref source_field) = pf.source_field {
                let prev_value = map.insert(source_field.name.as_str(), pf);
                if let Some(prev_value) = prev_value {
                    return Err(common_error::DaftError::ValueError(format!("Duplicate Partitioning Columns found on same source field: {source_field}\n1: {prev_value}\n2: {pf}")));
                }
            }
        }
        map
    };

    let with_part_cols = predicate.transform(&|expr| {
        use Operator::*;
        match expr {
            // Binary Op for Eq
            // All transforms should work as is
            Expr::BinaryOp {
                op: Eq,
                ref left, ref right } => {
                if let Expr::Column(col_name) = left.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_equals() && let Some(new_expr) = apply_partitioning_expr(right.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: Eq, left: col(pfield.field.name.as_str()).into(), right: new_expr.into() }));
                    }
                    Ok(Transformed::No(expr))
                } else if let Expr::Column(col_name) = right.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_equals() && let Some(new_expr) = apply_partitioning_expr(left.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: Eq, left: new_expr.into(), right: col(pfield.field.name.as_str()).into() }));
                    }
                    Ok(Transformed::No(expr))
                } else {
                    Ok(Transformed::No(expr))
                }
            },
            // Binary Op for NotEq
            // Should only work for Identity
            Expr::BinaryOp {
                op: NotEq,
                ref left, ref right } => {
                if let Expr::Column(col_name) = left.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_not_equals() && let Some(new_expr) = apply_partitioning_expr(right.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: NotEq, left: col(pfield.field.name.as_str()).into(), right: new_expr.into() }));
                    }
                    Ok(Transformed::No(expr))
                } else if let Expr::Column(col_name) = right.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_not_equals() && let Some(new_expr) = apply_partitioning_expr(left.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: NotEq, left: new_expr.into(), right: col(pfield.field.name.as_str()).into() }));
                    }
                    Ok(Transformed::No(expr))
                } else {
                    Ok(Transformed::No(expr))
                }
            },
            // Binary Op for Lt | LtEq | Gt | GtEq
            // we need to relax Lt and LtEq and only allow certain Transforms
            Expr::BinaryOp {
                op,
                ref left, ref right } if matches!(op, Lt | LtEq | Gt | GtEq)=> {
                let relaxed_op = match op {
                    Lt | LtEq => LtEq,
                    Gt | GtEq => GtEq,
                    _ => unreachable!("this branch only supports Lt | LtEq | Gt | GtEq")
                };

                if let Expr::Column(col_name) = left.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_comparison() && let Some(new_expr) = apply_partitioning_expr(right.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: relaxed_op, left: col(pfield.field.name.as_str()).into(), right: new_expr.into() }));
                    }
                    Ok(Transformed::No(expr))
                } else if let Expr::Column(col_name) = right.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) {
                    if let Some(tfm) = pfield.transform && tfm.supports_comparison() && let Some(new_expr) = apply_partitioning_expr(left.as_ref().clone(), pfield) {
                        return Ok(Transformed::Yes(Expr::BinaryOp { op: relaxed_op, left: new_expr.into(), right: col(pfield.field.name.as_str()).into() }));
                    }
                    Ok(Transformed::No(expr))
                } else {
                    Ok(Transformed::No(expr))
                }
            },

            Expr::IsNull(ref expr) if let Expr::Column(col_name) = expr.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) => {
                Ok(Transformed::Yes(Expr::IsNull(col(pfield.field.name.as_str()).into())))
            },
            Expr::NotNull(ref expr) if let Expr::Column(col_name) = expr.as_ref() && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) => {
                Ok(Transformed::Yes(Expr::NotNull(col(pfield.field.name.as_str()).into())))
            },
            _ => Ok(Transformed::No(expr))
        }
    })?;

    let p_keys = HashSet::<&str>::from_iter(pfields.iter().map(|p| p.field.name.as_ref()));

    let split = split_conjuction(&with_part_cols);
    let filtered = split
        .into_iter()
        .filter(|p| {
            let mut keep = true;
            p.apply(&mut |e| {
                if let Expr::Column(col_name) = e && !p_keys.contains(col_name.as_ref()) {
                keep = false;

            }
                Ok(VisitRecursion::Continue)
            })
            .unwrap();
            keep
        })
        .cloned()
        .collect::<Vec<_>>();
    Ok(conjuct(filtered))
}
