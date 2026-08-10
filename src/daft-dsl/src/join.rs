use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode};
use daft_core::{prelude::*, utils::supertype::try_get_supertype};
use indexmap::IndexSet;

use crate::{Column, Expr, ExprRef, ResolvedColumn, deduplicate_expr_names, unresolved_col};

pub fn get_common_join_cols<'a>(
    left_schema: &'a SchemaRef,
    right_schema: &'a SchemaRef,
) -> impl Iterator<Item = &'a str> {
    left_schema
        .field_names()
        .filter(|name| right_schema.has_field(name))
}

/// Infer the schema of a join operation
pub fn infer_join_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    join_type: JoinType,
) -> DaftResult<SchemaRef> {
    if matches!(join_type, JoinType::Anti | JoinType::Semi) {
        Ok(left_schema.clone())
    } else {
        let common_cols = get_common_join_cols(left_schema, right_schema).collect::<IndexSet<_>>();

        // common columns, then unique left fields, then unique right fields
        let fields = common_cols
            .iter()
            .map(|name| {
                let left_field = left_schema.get_field(name).unwrap();
                let right_field = right_schema.get_field(name).unwrap();

                Ok(match join_type {
                    JoinType::Inner => left_field.clone(),
                    JoinType::Left => left_field.clone(),
                    JoinType::Right => right_field.clone(),
                    JoinType::Outer => {
                        let supertype = try_get_supertype(&left_field.dtype, &right_field.dtype)?;

                        Field::new(*name, supertype)
                    }
                    JoinType::Anti | JoinType::Semi => unreachable!(),
                })
            })
            .chain(
                left_schema
                    .into_iter()
                    .chain(right_schema.fields())
                    .filter_map(|field| {
                        if common_cols.contains(field.name.as_ref()) {
                            None
                        } else {
                            Some(field.clone())
                        }
                    })
                    .map(Ok),
            )
            .collect::<DaftResult<Vec<_>>>()?;

        Ok(Schema::new(fields).into())
    }
}

/// Casts join keys to the same types and make their names unique.
pub fn normalize_join_keys(
    left_on: Vec<ExprRef>,
    right_on: Vec<ExprRef>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
) -> DaftResult<(Vec<ExprRef>, Vec<ExprRef>)> {
    let (left_on, right_on) = left_on
        .into_iter()
        .zip(right_on)
        .map(|(mut l, mut r)| {
            let l_dtype = l.to_field(&left_schema)?.dtype;
            let r_dtype = r.to_field(&right_schema)?.dtype;

            let supertype = try_get_supertype(&l_dtype, &r_dtype)?;

            if l_dtype != supertype {
                l = l.cast(&supertype);
            }

            if r_dtype != supertype {
                r = r.cast(&supertype);
            }

            Ok((l, r))
        })
        .collect::<DaftResult<(Vec<_>, Vec<_>)>>()?;

    let left_on = deduplicate_expr_names(&left_on);
    let right_on = deduplicate_expr_names(&right_on);

    Ok((left_on, right_on))
}

/// Convert `ResolvedColumn::JoinSide(field, _)` markers in a join residual predicate
/// into plain unresolved column references (by the post-deduplication field name),
/// so the predicate can be re-bound against the join output schema.
/// Spatial predicate function names recognized by the spatial-join rewrites.
/// NOTE: the local NLJ operator's acceleration list (`SPATIAL_FNS` in
/// daft-local-execution) intentionally EXCLUDES `st_disjoint` — routing here
/// is broader than acceleration soundness.
pub const SPATIAL_JOIN_PREDICATES: &[&str] = &[
    "st_contains", "st_intersects", "st_within", "st_covers", "st_covered_by",
    "st_disjoint", "st_touches", "st_overlaps", "st_crosses", "st_equals",
    "st_dwithin",
];

/// Bound-column index of arg0 of the first spatial predicate call found under
/// AND/NOT compositions — the "container" geometry used to pick the R-tree
/// build side. Shared by the native and distributed spatial-join translators.
pub fn spatial_join_arg0_bound_index(expr: &ExprRef) -> Option<usize> {
    match expr.as_ref() {
        Expr::ScalarFn(crate::functions::scalar::ScalarFn::Builtin(sf))
            if SPATIAL_JOIN_PREDICATES.contains(&sf.name()) =>
        {
            let arg0 = sf.inputs.required(0).ok()?;
            if let Expr::Column(Column::Bound(bc)) = arg0.as_ref() {
                Some(bc.index)
            } else {
                None
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            spatial_join_arg0_bound_index(left).or_else(|| spatial_join_arg0_bound_index(right))
        }
        Expr::Not(inner) => spatial_join_arg0_bound_index(inner),
        _ => None,
    }
}

pub fn strip_join_side_cols(expr: ExprRef) -> DaftResult<ExprRef> {
    Ok(expr
        .transform(|e| match e.as_ref() {
            Expr::Column(Column::Resolved(ResolvedColumn::JoinSide(field, _))) => {
                Ok(Transformed::yes(unresolved_col(field.name.clone())))
            }
            _ => Ok(Transformed::no(e)),
        })?
        .data)
}
