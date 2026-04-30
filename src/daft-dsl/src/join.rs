use std::collections::HashSet;

use daft_common::error::DaftResult;
use daft_core::{prelude::*, utils::supertype::try_get_supertype};
use indexmap::IndexSet;

use crate::{ExprRef, deduplicate_expr_names};

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

/// Infer the output schema for an asof join.
/// Column order: all left columns in their original schema order, then
/// right columns not in `right_cols_to_drop` in their original schema order.
pub fn infer_asof_join_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    right_cols_to_drop: &HashSet<String>,
) -> DaftResult<SchemaRef> {
    let mut fields = Vec::new();

    for field in left_schema.into_iter() {
        fields.push(field.clone());
    }

    for field in right_schema.into_iter() {
        if !right_cols_to_drop.contains(field.name.as_ref()) {
            fields.push(field.clone());
        }
    }

    Ok(Schema::new(fields).into())
}

/// Compute the right key column names to exclude from the asof join output.
///
/// For by-keys: all right by columns that are direct column references (not complex expressions) are dropped.
/// For on-keys: the right on column is dropped only when it has the same name as the left on column.
///
/// Returns the set of right column names to drop from the output.
pub fn get_right_cols_to_drop<E, F>(
    right_by: &[E],
    left_on: &E,
    right_on: &E,
    extract_name: F,
) -> HashSet<String>
where
    F: Fn(&E) -> Option<String>,
{
    let mut right_cols_to_drop = HashSet::new();

    for r in right_by {
        if let Some(right_name) = extract_name(r) {
            right_cols_to_drop.insert(right_name);
        }
    }

    if let (Some(left_name), Some(right_name)) = (extract_name(left_on), extract_name(right_on))
        && left_name == right_name
    {
        right_cols_to_drop.insert(right_name);
    }

    right_cols_to_drop
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
