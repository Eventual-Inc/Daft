use crate::{schema::Schema, DataType, Series};

use arrow2::array::Array;
use common_error::DaftResult;

use arrow2::array::dyn_ord::build_array_compare2;

fn build_dyn_is_equal(
    left: &DataType,
    right: &DataType,
    nulls_equal: bool,
    // nan_equal: bool,
) -> DaftResult<Box<dyn Fn(&dyn Array, &dyn Array, usize, usize) -> bool + Send + Sync>> {
    let is_equal_fn = build_array_compare2(&left.to_arrow()?, &right.to_arrow()?, nulls_equal)?;
    if NULLS_EQUAL {
        Ok(Box::new(move |left, right, i: usize, j: usize| {
            match (left.is_valid(i), right.is_valid(j)) {
                (true, true) => is_equal_fn(left, right, i, j).is_eq(),
                (false, false) => true,
                _ => false,
            }
        }))
    } else {
        Ok(Box::new(move |left, right, i: usize, j: usize| {
            match (left.is_valid(i), right.is_valid(j)) {
                (true, true) => is_equal_fn(left, right, i, j).is_eq(),
                _ => false,
            }
        }))
    }
}

pub fn build_dyn_multi_array_is_equal(
    schema: &Schema,
    nulls_equal: bool,
    nan_equal: bool,
) -> DaftResult<Box<dyn Fn(&[Box<dyn Array>], &[Box<dyn Array>], usize, usize) -> bool + Send + Sync>>
{
    let mut fn_list = Vec::with_capacity(schema.len());
    for field in schema.fields.values() {
        fn_list.push(build_dyn_is_equal(
            &field.dtype,
            &field.dtype,
            nulls_equal,
            nan_equal,
        )?);
    }
    let combined_fn = Box::new(
        move |left: &[Box<dyn Array>], right: &[Box<dyn Array>], i: usize, j: usize| -> bool {
            for (f, (l, r)) in fn_list.iter().zip(left.into_iter().zip(right.into_iter())) {
                if !f(l.as_ref(), r.as_ref(), i, j) {
                    return false;
                }
            }
            true
        },
    );
    Ok(combined_fn)
}
