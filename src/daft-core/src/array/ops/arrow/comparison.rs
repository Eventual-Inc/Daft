#![allow(deprecated, reason = "arrow2->arrow migration")]
use arrow::{
    array::{Array, ArrowPrimitiveType, FixedSizeListArray, LargeListArray, PrimitiveArray},
    datatypes::{Float32Type, Float64Type},
};
use common_error::DaftResult;
use daft_arrow::array::ord::build_compare;
use num_traits::Float;

use crate::{
    kernels::search_sorted::{build_is_valid, cmp_float},
    series::Series,
};

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

fn build_is_equal_list(
    left: &dyn Array,
    right: &dyn Array,
) -> Box<dyn Fn(usize, usize) -> bool + Send + Sync> {
    let left = left
        .as_any()
        .downcast_ref::<LargeListArray>()
        .unwrap()
        .clone();
    let right = right
        .as_any()
        .downcast_ref::<LargeListArray>()
        .unwrap()
        .clone();

    Box::new(move |i, j| left.value(i).to_data() == right.value(j).to_data())
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

    Box::new(move |i, j| left.value(i).to_data() == right.value(j).to_data())
}

fn build_is_equal_with_nan(
    left: &dyn Array,
    right: &dyn Array,
    nan_equal: bool,
) -> DaftResult<Box<dyn Fn(usize, usize) -> bool + Send + Sync>> {
    use arrow::datatypes::DataType;
    match (left.data_type(), right.data_type()) {
        (DataType::Float32, DataType::Float32) => {
            Ok(build_is_equal_float::<Float32Type>(left, right, nan_equal))
        }
        (DataType::Float64, DataType::Float64) => {
            Ok(build_is_equal_float::<Float64Type>(left, right, nan_equal))
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
            let left2 = daft_arrow::array::from_data(&left.to_data());
            let right2 = daft_arrow::array::from_data(&right.to_data());
            let comp = build_compare(left2.as_ref(), right2.as_ref())?;
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
