#[cfg(test)]
mod tests;

use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::prelude::*;
use indexmap::IndexSet;

use crate::{Expr, ExprRef};

/// Get the columns between the two sides of the join that should be merged in the order of the join keys.
/// Join keys should only be merged if they are column expressions.
pub fn get_common_join_keys<'a>(
    left_on: &'a [ExprRef],
    right_on: &'a [ExprRef],
) -> impl Iterator<Item = &'a Arc<str>> {
    left_on.iter().zip(right_on.iter()).filter_map(|(l, r)| {
        if let (Expr::Column(l_name), Expr::Column(r_name)) = (&**l, &**r)
            && l_name == r_name
        {
            Some(l_name)
        } else {
            None
        }
    })
}

/// Infer the schema of a join operation
///
/// This function assumes that the only common field names between the left and right schemas are the join fields,
/// which is valid because the right columns are renamed during the construction of a join logical operation.
pub fn infer_join_schema(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    left_on: &[ExprRef],
    right_on: &[ExprRef],
    how: JoinType,
) -> DaftResult<SchemaRef> {
    if left_on.len() != right_on.len() {
        return Err(DaftError::ValueError(format!(
            "Length of left_on does not match length of right_on for Join {} vs {}",
            left_on.len(),
            right_on.len()
        )));
    }

    if matches!(how, JoinType::Anti | JoinType::Semi) {
        Ok(left_schema.clone())
    } else {
        let common_join_keys: IndexSet<_> = get_common_join_keys(left_on, right_on)
            .map(|k| k.to_string())
            .collect();

        // common join fields, then unique left fields, then unique right fields
        let fields: Vec<_> = common_join_keys
            .iter()
            .map(|name| {
                left_schema
                    .get_field(name)
                    .expect("Common join key should exist in left schema")
            })
            .chain(left_schema.fields.iter().filter_map(|(name, field)| {
                if common_join_keys.contains(name) {
                    None
                } else {
                    Some(field)
                }
            }))
            .chain(right_schema.fields.iter().filter_map(|(name, field)| {
                if common_join_keys.contains(name) {
                    None
                } else if left_schema.fields.contains_key(name) {
                    unreachable!("Right schema should have renamed columns")
                } else {
                    Some(field)
                }
            }))
            .cloned()
            .collect();

        Ok(Schema::new(fields)?.into())
    }
}
