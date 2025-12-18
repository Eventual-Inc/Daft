use std::cmp::Ordering;

use common_error::{DaftError, DaftResult};
use daft_arrow::array::{
    Array,
    dyn_ord::{DynArrayComparator, build_dyn_array_compare},
};
use daft_schema::schema::Schema;

use crate::datatypes::DataType;

pub type MultiDynArrayComparator =
    Box<dyn Fn(&[Box<dyn Array>], &[Box<dyn Array>], usize, usize) -> Ordering + Send + Sync>;

pub fn build_dyn_compare(
    left: &DataType,
    right: &DataType,
    nulls_equal: bool,
    nans_equal: bool,
) -> DaftResult<DynArrayComparator> {
    if left == right {
        Ok(build_dyn_array_compare(
            &left.to_physical().to_arrow2()?,
            &right.to_physical().to_arrow2()?,
            nulls_equal,
            nans_equal,
        )?)
    } else {
        Err(DaftError::TypeError(format!(
            "Types do not match when creating comparator {left} vs {right}",
        )))
    }
}

pub fn build_dyn_multi_array_compare(
    schema: &Schema,
    nulls_equal: &[bool],
    nans_equal: &[bool],
) -> DaftResult<MultiDynArrayComparator> {
    let mut fn_list = Vec::with_capacity(schema.len());
    for (idx, field) in schema.into_iter().enumerate() {
        fn_list.push(build_dyn_compare(
            &field.dtype,
            &field.dtype,
            nulls_equal[idx],
            nans_equal[idx],
        )?);
    }
    let combined_fn = Box::new(
        move |left: &[Box<dyn Array>], right: &[Box<dyn Array>], i: usize, j: usize| -> Ordering {
            for (f, (l, r)) in fn_list.iter().zip(left.iter().zip(right.iter())) {
                match f(l.as_ref(), r.as_ref(), i, j) {
                    std::cmp::Ordering::Equal => {}
                    other => return other,
                }
            }
            Ordering::Equal
        },
    );

    Ok(combined_fn)
}
