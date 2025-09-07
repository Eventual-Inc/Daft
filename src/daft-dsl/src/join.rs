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
                        if common_cols.contains(field.name.as_str()) {
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
