use arrow::{
    array::{Array, ArrowPrimitiveType, PrimitiveArray, make_comparator},
    compute::SortOptions,
    datatypes::{DataType, Float32Type, Float64Type},
};
use common_error::DaftResult;
use num_traits::Float;

use crate::{kernels::search_sorted::cmp_float, series::Series};

fn build_is_equal_float<T>(
    left: &dyn Array,
    right: &dyn Array,
    nan_equal: bool,
) -> Box<dyn Fn(usize, usize) -> bool + Send + Sync>
where
    T: ArrowPrimitiveType,
    T::Native: Float,
{
    let left = left
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .clone();
    let right = right
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .clone();
    if nan_equal {
        Box::new(move |i, j| cmp_float::<T::Native>(&left.value(i), &right.value(j)).is_eq())
    } else {
        Box::new(move |i, j| left.value(i).eq(&right.value(j)))
    }
}

fn build_is_equal_with_nan(
    left: &dyn Array,
    right: &dyn Array,
    nan_equal: bool,
) -> DaftResult<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    match (left.data_type(), right.data_type()) {
        (DataType::Float32, DataType::Float32) => {
            Ok(build_is_equal_float::<Float32Type>(left, right, nan_equal))
        }
        (DataType::Float64, DataType::Float64) => {
            Ok(build_is_equal_float::<Float64Type>(left, right, nan_equal))
        }
        _ => {
            let comp = make_comparator(left, right, SortOptions::new(false, false))?;
            Ok(Box::new(move |i, j| comp(i, j).is_eq()))
        }
    }
}

pub fn build_is_equal(
    left: &dyn Array,
    right: &dyn Array,
    nulls_equal: bool,
    nan_equal: bool,
) -> DaftResult<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    let is_equal_fn = build_is_equal_with_nan(left, right, nan_equal)?;

    let left_data = left.to_data();
    let right_data = right.to_data();
    Ok(Box::new(move |i: usize, j: usize| {
        match (left_data.is_valid(i), right_data.is_valid(j)) {
            (true, true) => is_equal_fn(i, j),
            (false, false) => nulls_equal,
            _ => false,
        }
    }))
}

pub fn build_multi_array_is_equal(
    left: &[Series],
    right: &[Series],
    nulls_equal: &[bool],
    nans_equal: &[bool],
) -> DaftResult<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    let mut fn_list = Vec::with_capacity(left.len());

    for (idx, (l, r)) in left.iter().zip(right.iter()).enumerate() {
        fn_list.push(build_is_equal(
            l.to_arrow()?.as_ref(),
            r.to_arrow()?.as_ref(),
            nulls_equal[idx],
            nans_equal[idx],
        )?);
    }

    let combined_fn = Box::new(move |a_idx: usize, b_idx: usize| -> bool {
        for f in &fn_list {
            if !f(a_idx, b_idx) {
                return false;
            }
        }
        true
    });
    Ok(combined_fn)
}
