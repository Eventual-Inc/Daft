use std::collections::HashSet;

use common_error::DaftResult;
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
///
/// `left_key_cols`: ordered set of left column names that are plain-col keys
///   (by-keys first, then on-key). These appear first in the output and are
///   excluded from the "remaining left" section.
///
/// `right_key_cols`: set of right column names that are plain-col keys.
///   These are excluded from the output entirely.
///
/// Column order: left key columns (in `left_key_cols` order), remaining left
/// columns, then remaining right columns (post-dedup, excluding `right_key_cols`).
pub fn infer_asof_join_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    left_key_cols: &IndexSet<String>,
    right_key_cols: &HashSet<String>,
) -> DaftResult<SchemaRef> {
    let mut fields = Vec::new();
    for name in left_key_cols {
        fields.push(left_schema.get_field(name)?.clone());
    }

    for field in left_schema.into_iter() {
        if !left_key_cols.contains(field.name.as_ref()) {
            fields.push(field.clone());
        }
    }

    for field in right_schema.into_iter() {
        if !right_key_cols.contains(field.name.as_ref()) {
            fields.push(field.clone());
        }
    }

    Ok(Schema::new(fields).into())
}

/// Compute the left and right key column names for an asof join.
///
/// Iterates over `left_by`/`left_on` and `right_by`/`right_on`, applying
/// `extract_name` to each expression. Plain-column expressions return a name
/// and are collected; non-plain expressions (e.g. casts, arithmetic) are
/// skipped.
pub fn get_asof_key_cols<E, F>(
    left_by: &[E],
    right_by: &[E],
    left_on: &E,
    right_on: &E,
    extract_name: F,
) -> (IndexSet<String>, HashSet<String>)
where
    F: Fn(&E) -> Option<String>,
{
    let mut left_key_cols = IndexSet::new();
    for e in left_by.iter().chain(std::iter::once(left_on)) {
        if let Some(name) = extract_name(e) {
            left_key_cols.insert(name);
        }
    }

    let mut right_key_cols = HashSet::new();
    for e in right_by.iter().chain(std::iter::once(right_on)) {
        if let Some(name) = extract_name(e) {
            right_key_cols.insert(name);
        }
    }

    (left_key_cols, right_key_cols)
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
