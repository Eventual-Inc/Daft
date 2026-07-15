use std::collections::HashMap;

use common_error::DaftResult;
use daft_algebra::boolean::split_conjunction;
use daft_core::prelude::Operator;
use daft_dsl::{
    Column, Expr, ExprRef, ResolvedColumn,
    common_treenode::{Transformed, TreeNode, TreeNodeRecursion},
    functions::{
        FunctionExpr,
        partitioning::{self, PartitioningExpr},
    },
    null_lit, resolved_col,
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
    use PartitionTransform::{
        Day, Hour, IcebergBucket, IcebergTruncate, Identity, Month, Void, Year,
    };
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
    #[must_use]
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
        .map(|pfield| (pfield.field.name.as_ref(), pfield))
        .collect();

    // Before rewriting predicate for partition filter pushdown, partition predicate clauses into groups that will need
    // to be applied at the data level (i.e. any clauses that aren't pure partition predicates with identity
    // transformations).
    let data_split = split_conjunction(predicate);
    // Predicates that reference both partition columns and data columns.
    let mut needs_filter_op_preds: Vec<ExprRef> = vec![];
    // Predicates that only reference data columns (no partition column references) or only reference partition columns
    // but involve non-identity transformations.
    let mut data_preds: Vec<ExprRef> = vec![];
    for e in data_split {
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
            Expr::ScalarFn(_) => {
                // TODO: can we support scalar functions here?
                has_udf = true;
                Ok(TreeNodeRecursion::Stop)
            }

            Expr::Column(Column::Resolved(ResolvedColumn::Basic(col_name))) => {
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
        for pf in pfields {
            if let Some(ref source_field) = pf.source_field {
                let prev_value = map.insert(source_field.name.as_ref(), pf);
                if let Some(prev_value) = prev_value {
                    return Err(common_error::DaftError::ValueError(format!(
                        "Duplicate Partitioning Columns found on same source field: {source_field}\n1: {prev_value}\n2: {pf}"
                    )));
                }
            }
        }
        map
    };

    let get_pfield_for_col = |expr: &ExprRef| -> Option<&&PartitionField> {
        if let Expr::Column(Column::Resolved(ResolvedColumn::Basic(col_name))) = expr.as_ref() {
            source_to_pfield.get(col_name.as_ref())
        } else {
            None
        }
    };

    // Matches expressions that apply a partition field's own transform to that field's source
    // column, e.g. `partition_days(col("ts"))` on a table day-partitioned on "ts". Such an
    // expression evaluates to exactly the partition value, so predicates on it can reference the
    // partition field directly, keeping the original comparison operator and value.
    let get_pfield_for_transform_expr = |expr: &ExprRef| -> Option<&&PartitionField> {
        let Expr::Function {
            func: FunctionExpr::Partitioning(transform_expr),
            inputs,
        } = expr.as_ref()
        else {
            return None;
        };
        let [input] = inputs.as_slice() else {
            return None;
        };
        let pfield = get_pfield_for_col(input)?;
        use PartitionTransform as PT;
        use PartitioningExpr as PE;
        let transform_matches = match (pfield.transform, transform_expr) {
            (Some(PT::Year), PE::Years)
            | (Some(PT::Month), PE::Months)
            | (Some(PT::Day), PE::Days)
            | (Some(PT::Hour), PE::Hours) => true,
            (Some(PT::IcebergBucket(n)), PE::IcebergBucket(m)) => u64::try_from(*m) == Ok(n),
            (Some(PT::IcebergTruncate(w)), PE::IcebergTruncate(v)) => u64::try_from(*v) == Ok(w),
            _ => false,
        };
        transform_matches.then_some(pfield)
    };

    let with_part_cols = predicate.transform(&|expr: ExprRef| {
        use Operator::{Eq, Gt, GtEq, Lt, LtEq, NotEq};
        match expr.as_ref() {
            // Binary Op where one side applies the partition field's own transform to its source
            // column, e.g. `partition_days(col("ts")) == 19723` on a day-partitioned table.
            // The transformed side is exactly the partition value, so the predicate maps directly
            // onto the partition field: the value needs no transformation, all comparison
            // operators are precise, and no boundary relaxation is needed.
            Expr::BinaryOp { op, left, right }
                if matches!(op, Eq | NotEq | Lt | LtEq | Gt | GtEq)
                    && (get_pfield_for_transform_expr(left).is_some()
                        || get_pfield_for_transform_expr(right).is_some()) =>
            {
                let (new_left, new_right) =
                    if let Some(pfield) = get_pfield_for_transform_expr(left) {
                        (resolved_col(pfield.field.name.as_ref()), right.clone())
                    } else {
                        let pfield = get_pfield_for_transform_expr(right).unwrap();
                        (left.clone(), resolved_col(pfield.field.name.as_ref()))
                    };
                Ok(Transformed::yes(
                    Expr::BinaryOp {
                        op: *op,
                        left: new_left,
                        right: new_right,
                    }
                    .arced(),
                ))
            }
            // Binary Op for Eq
            // All transforms should work as is
            Expr::BinaryOp {
                op: Eq,
                left,
                right,
            } => {
                if let Some(pfield) = get_pfield_for_col(left) {
                    if let Some(tfm) = pfield.transform
                        && tfm.supports_equals()
                        && let Some(new_expr) = apply_partitioning_expr(right.clone(), pfield)
                    {
                        return Ok(Transformed::yes(
                            Expr::BinaryOp {
                                op: Eq,
                                left: resolved_col(pfield.field.name.as_ref()),
                                right: new_expr,
                            }
                            .arced(),
                        ));
                    }
                } else if let Some(pfield) = get_pfield_for_col(right)
                    && let Some(tfm) = pfield.transform
                    && tfm.supports_equals()
                    && let Some(new_expr) = apply_partitioning_expr(left.clone(), pfield)
                {
                    return Ok(Transformed::yes(
                        Expr::BinaryOp {
                            op: Eq,
                            left: new_expr,
                            right: resolved_col(pfield.field.name.as_ref()),
                        }
                        .arced(),
                    ));
                }

                Ok(Transformed::no(expr))
            }
            // Binary Op for NotEq
            // Should only work for Identity
            Expr::BinaryOp {
                op: NotEq,
                left,
                right,
            } => {
                if let Some(pfield) = get_pfield_for_col(left) {
                    if let Some(tfm) = pfield.transform
                        && tfm.supports_not_equals()
                        && let Some(new_expr) = apply_partitioning_expr(right.clone(), pfield)
                    {
                        return Ok(Transformed::yes(
                            Expr::BinaryOp {
                                op: NotEq,
                                left: resolved_col(pfield.field.name.as_ref()),
                                right: new_expr,
                            }
                            .arced(),
                        ));
                    }
                } else if let Some(pfield) = get_pfield_for_col(right)
                    && let Some(tfm) = pfield.transform
                    && tfm.supports_not_equals()
                    && let Some(new_expr) = apply_partitioning_expr(left.clone(), pfield)
                {
                    return Ok(Transformed::yes(
                        Expr::BinaryOp {
                            op: NotEq,
                            left: new_expr,
                            right: resolved_col(pfield.field.name.as_ref()),
                        }
                        .arced(),
                    ));
                }

                Ok(Transformed::no(expr))
            }
            // Binary Op for Lt | LtEq | Gt | GtEq
            // For non-identity transforms (e.g. Year, Month, Day, Hour, IcebergTruncate), we need to
            // relax the comparison boundary because the transform is lossy (e.g. a timestamp
            // `2024-03-15` maps to month `2024-03`, so `ts < 2024-03-15` must become
            // `month <= 2024-03` to avoid missing rows at the boundary).
            // For Identity transforms the partition value equals the source value exactly, so the
            // original operator is already precise and must NOT be relaxed.
            Expr::BinaryOp { op, left, right } if matches!(op, Lt | LtEq | Gt | GtEq) => {
                // Compute final_op once given the transform: identity transforms preserve the
                // original operator exactly; lossy transforms must relax the boundary.
                let compute_final_op = |tfm: PartitionTransform| {
                    if matches!(tfm, PartitionTransform::Identity) {
                        *op
                    } else {
                        match op {
                            Lt | LtEq => LtEq,
                            Gt | GtEq => GtEq,
                            _ => unreachable!("this branch only supports Lt | LtEq | Gt | GtEq"),
                        }
                    }
                };

                if let Some(pfield) = get_pfield_for_col(left) {
                    if let Some(tfm) = pfield.transform
                        && tfm.supports_comparison()
                        && let Some(new_expr) = apply_partitioning_expr(right.clone(), pfield)
                    {
                        return Ok(Transformed::yes(
                            Expr::BinaryOp {
                                op: compute_final_op(tfm),
                                left: resolved_col(pfield.field.name.as_ref()),
                                right: new_expr,
                            }
                            .arced(),
                        ));
                    }
                } else if let Some(pfield) = get_pfield_for_col(right)
                    && let Some(tfm) = pfield.transform
                    && tfm.supports_comparison()
                    && let Some(new_expr) = apply_partitioning_expr(left.clone(), pfield)
                {
                    return Ok(Transformed::yes(
                        Expr::BinaryOp {
                            op: compute_final_op(tfm),
                            left: new_expr,
                            right: resolved_col(pfield.field.name.as_ref()),
                        }
                        .arced(),
                    ));
                }

                Ok(Transformed::no(expr))
            }

            // The partitioning transforms matched by `get_pfield_for_transform_expr` are all
            // null-preserving (null in <=> null out), so null checks on the transformed source
            // column map directly onto the partition field.
            Expr::IsNull(expr)
                if let Some(pfield) =
                    get_pfield_for_col(expr).or_else(|| get_pfield_for_transform_expr(expr)) =>
            {
                Ok(Transformed::yes(
                    Expr::IsNull(resolved_col(pfield.field.name.as_ref())).arced(),
                ))
            }
            Expr::NotNull(expr)
                if let Some(pfield) =
                    get_pfield_for_col(expr).or_else(|| get_pfield_for_transform_expr(expr)) =>
            {
                Ok(Transformed::yes(
                    Expr::NotNull(resolved_col(pfield.field.name.as_ref())).arced(),
                ))
            }
            _ => Ok(Transformed::no(expr)),
        }
    })?;

    let with_part_cols = with_part_cols.data;

    // Filter to predicate clauses that only involve partition columns.
    let split = split_conjunction(&with_part_cols);
    let mut part_preds: Vec<ExprRef> = vec![];
    for e in split {
        let mut all_part_keys = true;
        e.apply(&mut |e: &ExprRef| {
            if let Expr::Column(Column::Resolved(ResolvedColumn::Basic(col_name))) = e.as_ref()
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

#[cfg(test)]
mod tests {
    use daft_core::prelude::{DataType, Field, TimeUnit};
    use daft_dsl::{functions::partitioning, lit, resolved_col};

    use super::*;

    fn day_pfield() -> PartitionField {
        PartitionField::new(
            Field::new("ts_day", DataType::Date),
            Some(Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microseconds, None),
            )),
            Some(PartitionTransform::Day),
        )
        .unwrap()
    }

    fn bucket_pfield(num_buckets: u64) -> PartitionField {
        PartitionField::new(
            Field::new("id_bucket", DataType::Int32),
            Some(Field::new("id", DataType::Int64)),
            Some(PartitionTransform::IcebergBucket(num_buckets)),
        )
        .unwrap()
    }

    fn truncate_pfield(width: u64) -> PartitionField {
        PartitionField::new(
            Field::new("name_trunc", DataType::Utf8),
            Some(Field::new("name", DataType::Utf8)),
            Some(PartitionTransform::IcebergTruncate(width)),
        )
        .unwrap()
    }

    #[test]
    fn day_transform_eq_becomes_partition_filter() -> DaftResult<()> {
        let predicate = partitioning::days(resolved_col("ts")).eq(lit(19723));
        let groups = rewrite_predicate_for_partitioning(&predicate, &[day_pfield()])?;
        assert_eq!(
            groups.partition_only_filter,
            vec![resolved_col("ts_day").eq(lit(19723))]
        );
        // The original predicate is still applicable at the data level.
        assert_eq!(groups.data_only_filter, vec![predicate]);
        assert!(groups.needing_filter_op.is_empty());
        Ok(())
    }

    #[test]
    fn day_transform_eq_matches_on_right_side() -> DaftResult<()> {
        let predicate = lit(19723).eq(partitioning::days(resolved_col("ts")));
        let groups = rewrite_predicate_for_partitioning(&predicate, &[day_pfield()])?;
        assert_eq!(
            groups.partition_only_filter,
            vec![lit(19723).eq(resolved_col("ts_day"))]
        );
        Ok(())
    }

    #[test]
    fn day_transform_comparison_is_not_relaxed() -> DaftResult<()> {
        // Unlike `ts < X` (which must relax to `ts_day <= days(X)` because the transform is
        // lossy), `partition_days(ts) < X` is exactly the partition value, so the strict
        // operator is preserved.
        let predicate = partitioning::days(resolved_col("ts")).lt(lit(19723));
        let groups = rewrite_predicate_for_partitioning(&predicate, &[day_pfield()])?;
        assert_eq!(
            groups.partition_only_filter,
            vec![resolved_col("ts_day").lt(lit(19723))]
        );
        Ok(())
    }

    #[test]
    fn day_transform_not_eq_becomes_partition_filter() -> DaftResult<()> {
        // `ts != X` cannot be pushed for lossy transforms, but `partition_days(ts) != X` is a
        // precise predicate on the partition value itself.
        let predicate = partitioning::days(resolved_col("ts")).not_eq(lit(19723));
        let groups = rewrite_predicate_for_partitioning(&predicate, &[day_pfield()])?;
        assert_eq!(
            groups.partition_only_filter,
            vec![resolved_col("ts_day").not_eq(lit(19723))]
        );
        Ok(())
    }

    #[test]
    fn day_transform_null_checks_become_partition_filters() -> DaftResult<()> {
        let predicate = partitioning::days(resolved_col("ts")).is_null();
        let groups = rewrite_predicate_for_partitioning(&predicate, &[day_pfield()])?;
        assert_eq!(
            groups.partition_only_filter,
            vec![resolved_col("ts_day").is_null()]
        );

        let predicate = partitioning::days(resolved_col("ts")).not_null();
        let groups = rewrite_predicate_for_partitioning(&predicate, &[day_pfield()])?;
        assert_eq!(
            groups.partition_only_filter,
            vec![resolved_col("ts_day").not_null()]
        );
        Ok(())
    }

    #[test]
    fn mismatched_transform_is_not_pushed() -> DaftResult<()> {
        // Table is day-partitioned, but the predicate uses the month transform.
        let predicate = partitioning::months(resolved_col("ts")).eq(lit(648));
        let groups = rewrite_predicate_for_partitioning(&predicate, &[day_pfield()])?;
        assert!(groups.partition_only_filter.is_empty());
        assert_eq!(groups.data_only_filter, vec![predicate]);
        Ok(())
    }

    #[test]
    fn transform_on_non_source_column_is_not_pushed() -> DaftResult<()> {
        let predicate = partitioning::days(resolved_col("other")).eq(lit(19723));
        let groups = rewrite_predicate_for_partitioning(&predicate, &[day_pfield()])?;
        assert!(groups.partition_only_filter.is_empty());
        Ok(())
    }

    #[test]
    fn bucket_transform_eq_requires_matching_num_buckets() -> DaftResult<()> {
        let predicate = partitioning::iceberg_bucket(resolved_col("id"), 16).eq(lit(3));
        let groups = rewrite_predicate_for_partitioning(&predicate, &[bucket_pfield(16)])?;
        assert_eq!(
            groups.partition_only_filter,
            vec![resolved_col("id_bucket").eq(lit(3))]
        );

        let groups = rewrite_predicate_for_partitioning(&predicate, &[bucket_pfield(8)])?;
        assert!(groups.partition_only_filter.is_empty());
        Ok(())
    }

    #[test]
    fn truncate_transform_eq_requires_matching_width() -> DaftResult<()> {
        let predicate = partitioning::iceberg_truncate(resolved_col("name"), 4).eq(lit("abcd"));
        let groups = rewrite_predicate_for_partitioning(&predicate, &[truncate_pfield(4)])?;
        assert_eq!(
            groups.partition_only_filter,
            vec![resolved_col("name_trunc").eq(lit("abcd"))]
        );

        let groups = rewrite_predicate_for_partitioning(&predicate, &[truncate_pfield(2)])?;
        assert!(groups.partition_only_filter.is_empty());
        Ok(())
    }

    #[test]
    fn transform_predicate_mixed_with_data_column_needs_filter_op() -> DaftResult<()> {
        // An OR with a data column cannot be pruned on the partition value alone.
        let predicate = partitioning::days(resolved_col("ts"))
            .eq(lit(19723))
            .or(resolved_col("x").gt(lit(5)));
        let groups = rewrite_predicate_for_partitioning(&predicate, &[day_pfield()])?;
        assert!(groups.partition_only_filter.is_empty());
        Ok(())
    }

    #[test]
    fn source_column_comparison_still_relaxed() -> DaftResult<()> {
        // Sanity check that the pre-existing source-column path is unaffected: `ts < X` on a
        // day-partitioned table must relax to `ts_day <= days(X)`.
        let predicate = resolved_col("ts").lt(lit(0i64));
        let groups = rewrite_predicate_for_partitioning(&predicate, &[day_pfield()])?;
        assert_eq!(
            groups.partition_only_filter,
            vec![resolved_col("ts_day").lt_eq(partitioning::days(lit(0i64)))]
        );
        Ok(())
    }
}
