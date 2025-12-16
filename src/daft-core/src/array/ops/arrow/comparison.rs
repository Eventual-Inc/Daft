use common_error::DaftResult;
use daft_arrow::{
    array::{Array, FixedSizeListArray, ListArray, PrimitiveArray, equal, ord::build_compare},
    datatypes::DataType,
    error::Result,
};
use num_traits::Float;

use crate::{
    kernels::search_sorted::{build_is_valid, cmp_float},
    series::Series,
};

fn build_is_equal_float<F: Float + daft_arrow::types::NativeType>(
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

fn build_is_equal_list(
    left: &dyn Array,
    right: &dyn Array,
) -> Box<dyn Fn(usize, usize) -> bool + Send + Sync> {
    let left = left
        .as_any()
        .downcast_ref::<ListArray<i64>>()
        .unwrap()
        .clone();
    let right = right
        .as_any()
        .downcast_ref::<ListArray<i64>>()
        .unwrap()
        .clone();

    Box::new(move |i, j| equal(left.value(i).as_ref(), right.value(j).as_ref()))
}

fn build_is_equal_fixed_size_list(
    left: &dyn Array,
    right: &dyn Array,
) -> Box<dyn Fn(usize, usize) -> bool + Send + Sync> {
    let left = left
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap()
        .clone();
    let right = right
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap()
        .clone();

    Box::new(move |i, j| equal(left.value(i).as_ref(), right.value(j).as_ref()))
}

fn build_is_equal_with_nan(
    left: &dyn Array,
    right: &dyn Array,
    nan_equal: bool,
) -> Result<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    match (left.data_type(), right.data_type()) {
        (DataType::Float32, DataType::Float32) => {
            Ok(build_is_equal_float::<f32>(left, right, nan_equal))
        }
        (DataType::Float64, DataType::Float64) => {
            Ok(build_is_equal_float::<f64>(left, right, nan_equal))
        }
        (DataType::LargeList(f1), DataType::LargeList(f2)) if *f1 == *f2 => {
            Ok(build_is_equal_list(left, right))
        }
        (DataType::FixedSizeList(f1, l1), DataType::FixedSizeList(f2, l2))
            if *l1 == *l2 && *f1 == *f2 =>
        {
            Ok(build_is_equal_fixed_size_list(left, right))
        }
        _ => {
            let comp = build_compare(left, right)?;
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
    nulls_equal: &[bool],
    nans_equal: &[bool],
) -> DaftResult<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    let mut fn_list = Vec::with_capacity(left.len());

    for (idx, (l, r)) in left.iter().zip(right.iter()).enumerate() {
        fn_list.push(build_is_equal(
            l.to_arrow2().as_ref(),
            r.to_arrow2().as_ref(),
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
