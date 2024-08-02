use std::cmp::Ordering;

use crate::{schema::Schema, DataType, Series};

use arrow2::array::Array;
use common_error::DaftResult;

use arrow2::array::dyn_ord::build_array_compare2;

fn build_dyn_compare(
    left: &DataType,
    right: &DataType,
    nulls_equal: bool,
    // nan_equal: bool,
) -> DaftResult<Box<dyn Fn(&dyn Array, &dyn Array, usize, usize) -> Ordering + Send + Sync>> {
    Ok(build_array_compare2(
        &left.to_arrow()?,
        &right.to_arrow()?,
        nulls_equal,
    )?)
}

pub fn build_dyn_multi_array_compare(
    schema: &Schema,
    nulls_equal: bool,
    nan_equal: bool,
) -> DaftResult<
    Box<dyn Fn(&[Box<dyn Array>], &[Box<dyn Array>], usize, usize) -> Ordering + Send + Sync>,
> {
    let mut fn_list = Vec::with_capacity(schema.len());
    for field in schema.fields.values() {
        fn_list.push(build_dyn_compare(
            &field.dtype,
            &field.dtype,
            nulls_equal,
            // nan_equal,
        )?);
    }
    let combined_fn = Box::new(
        move |left: &[Box<dyn Array>], right: &[Box<dyn Array>], i: usize, j: usize| -> Ordering {
            for (f, (l, r)) in fn_list.iter().zip(left.into_iter().zip(right.into_iter())) {
                match f(l.as_ref(), r.as_ref(), i, j) {
                    std::cmp::Ordering::Equal => continue,
                    other => return other,
                }
            }
            Ordering::Equal
        },
    );

    Ok(combined_fn)
}
