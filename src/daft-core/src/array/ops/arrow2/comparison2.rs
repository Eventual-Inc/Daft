use std::cmp::Ordering;
use std::fmt::format;

use crate::{schema::Schema, DataType, Series};

use arrow2::array::Array;
use common_error::DaftError;
use common_error::DaftResult;

use arrow2::array::dyn_ord::build_array_compare2;
use arrow2::array::dyn_ord::DynArrayComparator;

pub type MultiDynArrayComparator =
    Box<dyn Fn(&[Box<dyn Array>], &[Box<dyn Array>], usize, usize) -> Ordering + Send + Sync>;
pub fn build_dyn_compare(
    left: &DataType,
    right: &DataType,
    nulls_equal: bool,
    // nan_equal: bool,
) -> DaftResult<DynArrayComparator> {
    if left != right {
        Err(DaftError::TypeError(format!(
            "Types do not match when creating comparator {} vs {}",
            left, right
        )))
    } else {
        Ok(build_array_compare2(
            &left.to_physical().to_arrow()?,
            &right.to_physical().to_arrow()?,
            nulls_equal,
        )?)
    }
}

pub fn build_dyn_multi_array_compare(
    schema: &Schema,
    nulls_equal: bool,
    nan_equal: bool,
) -> DaftResult<MultiDynArrayComparator> {
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
