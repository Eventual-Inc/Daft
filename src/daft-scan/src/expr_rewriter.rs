use std::collections::HashMap;

use common_error::DaftResult;
use daft_dsl::{
    col,
    common_treenode::{Transformed, TreeNode, TreeNodeRecursion},
    functions::{partitioning, FunctionExpr},
    null_lit,
    optimization::split_conjuction,
    Expr, ExprRef, Operator,
};

use crate::{PartitionField, PartitionTransform};

fn unalias(expr: ExprRef) -> DaftResult<ExprRef> {
    let res = expr.transform(&|e: ExprRef| {
        if let Expr::Alias(e, _) = e.as_ref() {
            Ok(Transformed::yes(e.clone()))
        } else {
            Ok(Transformed::no(e))
        }
    })?;
    Ok(res.data)
}

fn apply_partitioning_expr(expr: ExprRef, pfield: &PartitionField) -> Option<ExprRef> {
    use PartitionTransform::*;
    match pfield.transform {
        Some(Identity) => Some(
            pfield
                .source_field
                .as_ref()
                .map(|s| expr.clone().cast(&s.dtype))
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

/// Grouping of clauses in a conjunctive predicate around partitioning semantics.
pub struct PredicateGroups {
    // All partition-only filters, which can be applied directly to partition values and can be dropped from the
    // data-level filter.
    pub partition_only_filter: Vec<ExprRef>,
    // Predicates that only reference data columns (no partition column references) or only reference partition columns
    // but involve non-identity transformations; these need to be applied to the data, but don't require a separate
    // filter op (i.e. they can be pushed into the scan).
    pub data_only_filter: Vec<ExprRef>,
    // Filters needing their own dedicated filter op (unable to be pushed into scan); this includes predicates
    // involving both partition and data columns, and predicates containing UDFs.
    pub needing_filter_op: Vec<ExprRef>,
}

impl PredicateGroups {
    pub fn new(
        partition_only_filter: Vec<ExprRef>,
        data_only_filter: Vec<ExprRef>,
        needing_filter_op: Vec<ExprRef>,
    ) -> Self {
        Self {
            partition_only_filter,
            data_only_filter,
            needing_filter_op,
        }
    }
}

pub fn rewrite_predicate_for_partitioning(
    predicate: &ExprRef,
    pfields: &[PartitionField],
) -> DaftResult<PredicateGroups> {
    let pfields_map: HashMap<&str, &PartitionField> = pfields
        .iter()
        .map(|pfield| (pfield.field.name.as_str(), pfield))
        .collect();

    // Before rewriting predicate for partition filter pushdown, partition predicate clauses into groups that will need
    // to be applied at the data level (i.e. any clauses that aren't pure partition predicates with identity
    // transformations).
    let data_split = split_conjuction(predicate);
    // Predicates that reference both partition columns and data columns.
    let mut needs_filter_op_preds: Vec<ExprRef> = vec![];
    // Predicates that only reference data columns (no partition column references) or only reference partition columns
    // but involve non-identity transformations.
    let mut data_preds: Vec<ExprRef> = vec![];
    for e in data_split.into_iter() {
        let mut all_data_keys = true;
        let mut all_part_keys = true;
        let mut any_non_identity_part_keys = false;
        let mut has_udf = false;
        e.apply(&mut |e: &ExprRef| match e.as_ref() {
            #[cfg(feature = "python")]
            Expr::Function {
                func: FunctionExpr::Python(..),
                ..
            } => {
                has_udf = true;
                Ok(TreeNodeRecursion::Stop)
            }
            Expr::ScalarFunction(_) => {
                // TODO: can we support scalar functions here?
                has_udf = true;
                Ok(TreeNodeRecursion::Stop)
            }

            Expr::Column(col_name) => {
                if let Some(pfield) = pfields_map.get(col_name.as_ref()) {
                    all_data_keys = false;
                    if !matches!(pfield.transform, Some(PartitionTransform::Identity) | None) {
                        any_non_identity_part_keys = true;
                    }
                } else {
                    all_part_keys = false;
                }
                Ok(TreeNodeRecursion::Continue)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        })
        .unwrap();

        // Push to appropriate vec.
        if has_udf || (!all_data_keys && !all_part_keys) {
            needs_filter_op_preds.push(e.clone());
        } else if all_data_keys || all_part_keys && any_non_identity_part_keys {
            data_preds.push(e.clone());
        }
    }
    if pfields.is_empty() {
        return Ok(PredicateGroups::new(
            vec![],
            data_preds,
            needs_filter_op_preds,
        ));
    }

    let predicate = unalias(predicate.clone())?;

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

    let with_part_cols = predicate.transform(&|expr: ExprRef| {
        use Operator::*;
        match expr.as_ref() {
            // Binary Op for Eq
            // All transforms should work as is
            Expr::BinaryOp {
                op: Eq,
                ref left,
                ref right,
            } => {
                if let Expr::Column(col_name) = left.as_ref()
                    && let Some(pfield) = source_to_pfield.get(col_name.as_ref())
                {
                    if let Some(tfm) = pfield.transform
                        && tfm.supports_equals()
                        && let Some(new_expr) = apply_partitioning_expr(right.clone(), pfield)
                    {
                        return Ok(Transformed::yes(
                            Expr::BinaryOp {
                                op: Eq,
                                left: col(pfield.field.name.as_str()),
                                right: new_expr,
                            }
                            .arced(),
                        ));
                    }
                    Ok(Transformed::no(expr))
                } else if let Expr::Column(col_name) = right.as_ref()
                    && let Some(pfield) = source_to_pfield.get(col_name.as_ref())
                {
                    if let Some(tfm) = pfield.transform
                        && tfm.supports_equals()
                        && let Some(new_expr) = apply_partitioning_expr(left.clone(), pfield)
                    {
                        return Ok(Transformed::yes(
                            Expr::BinaryOp {
                                op: Eq,
                                left: new_expr,
                                right: col(pfield.field.name.as_str()),
                            }
                            .arced(),
                        ));
                    }
                    Ok(Transformed::no(expr))
                } else {
                    Ok(Transformed::no(expr))
                }
            }
            // Binary Op for NotEq
            // Should only work for Identity
            Expr::BinaryOp {
                op: NotEq,
                ref left,
                ref right,
            } => {
                if let Expr::Column(col_name) = left.as_ref()
                    && let Some(pfield) = source_to_pfield.get(col_name.as_ref())
                {
                    if let Some(tfm) = pfield.transform
                        && tfm.supports_not_equals()
                        && let Some(new_expr) = apply_partitioning_expr(right.clone(), pfield)
                    {
                        return Ok(Transformed::yes(
                            Expr::BinaryOp {
                                op: NotEq,
                                left: col(pfield.field.name.as_str()),
                                right: new_expr,
                            }
                            .arced(),
                        ));
                    }
                    Ok(Transformed::no(expr))
                } else if let Expr::Column(col_name) = right.as_ref()
                    && let Some(pfield) = source_to_pfield.get(col_name.as_ref())
                {
                    if let Some(tfm) = pfield.transform
                        && tfm.supports_not_equals()
                        && let Some(new_expr) = apply_partitioning_expr(left.clone(), pfield)
                    {
                        return Ok(Transformed::yes(
                            Expr::BinaryOp {
                                op: NotEq,
                                left: new_expr,
                                right: col(pfield.field.name.as_str()),
                            }
                            .arced(),
                        ));
                    }
                    Ok(Transformed::no(expr))
                } else {
                    Ok(Transformed::no(expr))
                }
            }
            // Binary Op for Lt | LtEq | Gt | GtEq
            // we need to relax Lt and LtEq and only allow certain Transforms
            Expr::BinaryOp {
                op,
                ref left,
                ref right,
            } if matches!(op, Lt | LtEq | Gt | GtEq) => {
                let relaxed_op = match op {
                    Lt | LtEq => LtEq,
                    Gt | GtEq => GtEq,
                    _ => unreachable!("this branch only supports Lt | LtEq | Gt | GtEq"),
                };

                if let Expr::Column(col_name) = left.as_ref()
                    && let Some(pfield) = source_to_pfield.get(col_name.as_ref())
                {
                    if let Some(tfm) = pfield.transform
                        && tfm.supports_comparison()
                        && let Some(new_expr) = apply_partitioning_expr(right.clone(), pfield)
                    {
                        return Ok(Transformed::yes(
                            Expr::BinaryOp {
                                op: relaxed_op,
                                left: col(pfield.field.name.as_str()),
                                right: new_expr,
                            }
                            .arced(),
                        ));
                    }
                    Ok(Transformed::no(expr))
                } else if let Expr::Column(col_name) = right.as_ref()
                    && let Some(pfield) = source_to_pfield.get(col_name.as_ref())
                {
                    if let Some(tfm) = pfield.transform
                        && tfm.supports_comparison()
                        && let Some(new_expr) = apply_partitioning_expr(left.clone(), pfield)
                    {
                        return Ok(Transformed::yes(
                            Expr::BinaryOp {
                                op: relaxed_op,
                                left: new_expr,
                                right: col(pfield.field.name.as_str()),
                            }
                            .arced(),
                        ));
                    }
                    Ok(Transformed::no(expr))
                } else {
                    Ok(Transformed::no(expr))
                }
            }

            Expr::IsNull(ref expr)
                if let Expr::Column(col_name) = expr.as_ref()
                    && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) =>
            {
                Ok(Transformed::yes(
                    Expr::IsNull(col(pfield.field.name.as_str())).arced(),
                ))
            }
            Expr::NotNull(ref expr)
                if let Expr::Column(col_name) = expr.as_ref()
                    && let Some(pfield) = source_to_pfield.get(col_name.as_ref()) =>
            {
                Ok(Transformed::yes(
                    Expr::NotNull(col(pfield.field.name.as_str())).arced(),
                ))
            }
            _ => Ok(Transformed::no(expr)),
        }
    })?;

    let with_part_cols = with_part_cols.data;

    // Filter to predicate clauses that only involve partition columns.
    let split = split_conjuction(&with_part_cols);
    let mut part_preds: Vec<ExprRef> = vec![];
    for e in split.into_iter() {
        let mut all_part_keys = true;
        e.apply(&mut |e: &ExprRef| {
            if let Expr::Column(col_name) = e.as_ref()
                && !pfields_map.contains_key(col_name.as_ref())
            {
                all_part_keys = false;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();

        // Push to partition preds vec.
        if all_part_keys {
            part_preds.push(e.clone());
        }
    }
    Ok(PredicateGroups::new(
        part_preds,
        data_preds,
        needs_filter_op_preds,
    ))
}
