use arrow2::{
    array::{ord::build_compare, Array, PrimitiveArray},
    datatypes::DataType,
    error::Result,
};
use num_traits::Float;

use crate::{
    kernels::search_sorted::{build_is_valid, cmp_float},
    series::Series,
};
use common_error::DaftResult;

fn build_is_equal_float<F: Float + arrow2::types::NativeType>(
    left: &dyn Array,
    right: &dyn Array,
    nan_equal: bool,
) -> Box<dyn Fn(usize, usize) -> bool + Send + Sync> {
    let left = left
        .as_any()
        .downcast_ref::<PrimitiveArray<F>>()
        .unwrap()
        .clone();
    let right = right
        .as_any()
        .downcast_ref::<PrimitiveArray<F>>()
        .unwrap()
        .clone();
    if nan_equal {
        Box::new(move |i, j| cmp_float::<F>(&left.value(i), &right.value(j)).is_eq())
    } else {
        Box::new(move |i, j| left.value(i).eq(&right.value(j)))
    }
}

fn build_is_equal_with_nan(
    left: &dyn Array,
    right: &dyn Array,
    nulls_equal: bool,
) -> Result<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    if (left.data_type() == &DataType::Float32) && (right.data_type() == &DataType::Float32) {
        Ok(build_is_equal_float::<f32>(left, right, nulls_equal))
    } else if (left.data_type() == &DataType::Float64) && (right.data_type() == &DataType::Float64)
    {
        Ok(build_is_equal_float::<f64>(left, right, nulls_equal))
    } else {
        let comp = build_compare(left, right)?;
        Ok(Box::new(move |i, j| comp(i, j).is_eq()))
    }
}

fn build_is_equal(
    left: &dyn Array,
    right: &dyn Array,
    nulls_equal: bool,
    nan_equal: bool,
) -> DaftResult<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    let is_equal_fn = build_is_equal_with_nan(left, right, nan_equal)?;
    let left_is_valid = build_is_valid(left);
    let right_is_valid = build_is_valid(right);

    if nulls_equal {
        Ok(Box::new(move |i: usize, j: usize| {
            match (left_is_valid(i), right_is_valid(j)) {
                (true, true) => is_equal_fn(i, j),
                (false, false) => true,
                _ => false,
            }
        }))
    } else {
        Ok(Box::new(move |i: usize, j: usize| {
            match (left_is_valid(i), right_is_valid(j)) {
                (true, true) => is_equal_fn(i, j),
                _ => false,
            }
        }))
    }
}

pub fn build_multi_array_is_equal(
    left: &[Series],
    right: &[Series],
    nulls_equal: bool,
    nan_equal: bool,
) -> DaftResult<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    let mut fn_list = Vec::with_capacity(left.len());

    for (l, r) in left.iter().zip(right.iter()) {
        fn_list.push(build_is_equal(
            l.to_arrow().as_ref(),
            r.to_arrow().as_ref(),
            nulls_equal,
            nan_equal,
        )?);
    }

    let combined_fn = Box::new(move |a_idx: usize, b_idx: usize| -> bool {
        for f in fn_list.iter() {
            if !f(a_idx, b_idx) {
                return false;
            }
        }
        true
    });
    Ok(combined_fn)
}
