use std::cmp::Ordering;

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, GenericBinaryArray, GenericStringArray, OffsetSizeTrait,
        PrimitiveArray,
    },
    datatypes::*,
};
use common_error::{DaftError, DaftResult};
use daft_schema::schema::Schema;
use num_traits::Float;

use crate::datatypes::DataType;

/// Compare the values at two arbitrary indices in two arbitrary arrays.
pub type DynArrayComparator =
    Box<dyn Fn(&dyn Array, &dyn Array, usize, usize) -> Ordering + Send + Sync>;

pub type MultiDynArrayComparator =
    Box<dyn Fn(&[ArrayRef], &[ArrayRef], usize, usize) -> Ordering + Send + Sync>;

#[inline]
fn is_valid(arr: &dyn Array, i: usize) -> bool {
    arr.nulls().is_none_or(|n| n.is_valid(i))
}

#[inline]
fn compare_with_nulls<F: FnOnce() -> Ordering>(
    left: &dyn Array,
    right: &dyn Array,
    i: usize,
    j: usize,
    nulls_equal: bool,
    cmp: F,
) -> Ordering {
    assert!(i < left.len());
    assert!(j < right.len());
    match (is_valid(left, i), is_valid(right, j)) {
        (true, true) => cmp(),
        (false, true) => Ordering::Greater,
        (true, false) => Ordering::Less,
        (false, false) => {
            if nulls_equal {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        }
    }
}

#[allow(clippy::eq_op)]
#[inline]
fn cmp_float<F: Float>(l: &F, r: &F, nans_equal: bool) -> Ordering {
    match (l.is_nan(), r.is_nan()) {
        (false, false) => unsafe { l.partial_cmp(r).unwrap_unchecked() },
        (true, true) => {
            if nans_equal {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        }
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
    }
}

fn compare_dyn_floats<T: ArrowPrimitiveType>(
    nulls_equal: bool,
    nans_equal: bool,
) -> DynArrayComparator
where
    T::Native: Float,
{
    Box::new(move |left, right, i, j| {
        let left_arr = left.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        let right_arr = right.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        compare_with_nulls(left, right, i, j, nulls_equal, || {
            cmp_float(&left_arr.value(i), &right_arr.value(j), nans_equal)
        })
    })
}

fn compare_dyn_primitives<T: ArrowPrimitiveType>(nulls_equal: bool) -> DynArrayComparator
where
    T::Native: Ord,
{
    Box::new(move |left, right, i, j| {
        let left_arr = left.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        let right_arr = right.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        compare_with_nulls(left, right, i, j, nulls_equal, || {
            left_arr.value(i).cmp(&right_arr.value(j))
        })
    })
}

fn compare_dyn_string<O: OffsetSizeTrait>(nulls_equal: bool) -> DynArrayComparator {
    Box::new(move |left, right, i, j| {
        let left_arr = left
            .as_any()
            .downcast_ref::<GenericStringArray<O>>()
            .unwrap();
        let right_arr = right
            .as_any()
            .downcast_ref::<GenericStringArray<O>>()
            .unwrap();
        compare_with_nulls(left, right, i, j, nulls_equal, || {
            left_arr.value(i).cmp(right_arr.value(j))
        })
    })
}

fn compare_dyn_binary<O: OffsetSizeTrait>(nulls_equal: bool) -> DynArrayComparator {
    Box::new(move |left, right, i, j| {
        let left_arr = left
            .as_any()
            .downcast_ref::<GenericBinaryArray<O>>()
            .unwrap();
        let right_arr = right
            .as_any()
            .downcast_ref::<GenericBinaryArray<O>>()
            .unwrap();
        compare_with_nulls(left, right, i, j, nulls_equal, || {
            left_arr.value(i).cmp(right_arr.value(j))
        })
    })
}

fn compare_dyn_boolean(nulls_equal: bool) -> DynArrayComparator {
    Box::new(move |left, right, i, j| {
        let left_arr = left.as_any().downcast_ref::<BooleanArray>().unwrap();
        let right_arr = right.as_any().downcast_ref::<BooleanArray>().unwrap();
        compare_with_nulls(left, right, i, j, nulls_equal, || {
            left_arr.value(i).cmp(&right_arr.value(j))
        })
    })
}

fn compare_dyn_null(nulls_equal: bool) -> DynArrayComparator {
    let ordering = if nulls_equal {
        Ordering::Equal
    } else {
        Ordering::Less
    };
    Box::new(move |_, _, _, _| ordering)
}

fn build_arrow_compare(
    dtype: &arrow::datatypes::DataType,
    nulls_equal: bool,
    nans_equal: bool,
) -> DaftResult<DynArrayComparator> {
    use arrow::datatypes::{DataType::*, IntervalUnit::*, TimeUnit::*};
    Ok(match dtype {
        Boolean => compare_dyn_boolean(nulls_equal),
        UInt8 => compare_dyn_primitives::<UInt8Type>(nulls_equal),
        UInt16 => compare_dyn_primitives::<UInt16Type>(nulls_equal),
        UInt32 => compare_dyn_primitives::<UInt32Type>(nulls_equal),
        UInt64 => compare_dyn_primitives::<UInt64Type>(nulls_equal),
        Int8 => compare_dyn_primitives::<Int8Type>(nulls_equal),
        Int16 => compare_dyn_primitives::<Int16Type>(nulls_equal),
        Int32 | Date32 | Time32(Second) | Time32(Millisecond) | Interval(YearMonth) => {
            compare_dyn_primitives::<Int32Type>(nulls_equal)
        }
        Int64
        | Date64
        | Time64(Microsecond)
        | Time64(Nanosecond)
        | Timestamp(Second, None)
        | Timestamp(Millisecond, None)
        | Timestamp(Microsecond, None)
        | Timestamp(Nanosecond, None)
        | Duration(Second)
        | Duration(Millisecond)
        | Duration(Microsecond)
        | Duration(Nanosecond) => compare_dyn_primitives::<Int64Type>(nulls_equal),
        Float32 => compare_dyn_floats::<Float32Type>(nulls_equal, nans_equal),
        Float64 => compare_dyn_floats::<Float64Type>(nulls_equal, nans_equal),
        Decimal128(_, _) => compare_dyn_primitives::<Decimal128Type>(nulls_equal),
        Utf8 => compare_dyn_string::<i32>(nulls_equal),
        LargeUtf8 => compare_dyn_string::<i64>(nulls_equal),
        Binary => compare_dyn_binary::<i32>(nulls_equal),
        LargeBinary => compare_dyn_binary::<i64>(nulls_equal),
        Null => compare_dyn_null(nulls_equal),
        other => {
            return Err(DaftError::TypeError(format!(
                "The data type {other:?} has no natural order"
            )));
        }
    })
}

pub fn build_dyn_compare(
    left: &DataType,
    right: &DataType,
    nulls_equal: bool,
    nans_equal: bool,
) -> DaftResult<DynArrayComparator> {
    if left != right {
        return Err(DaftError::TypeError(format!(
            "Types do not match when creating comparator {left} vs {right}",
        )));
    }
    let arrow_type = left.to_physical().to_arrow()?;
    build_arrow_compare(&arrow_type, nulls_equal, nans_equal)
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
        move |left: &[ArrayRef], right: &[ArrayRef], i: usize, j: usize| -> Ordering {
            for (f, (l, r)) in fn_list.iter().zip(left.iter().zip(right.iter())) {
                match f(l.as_ref(), r.as_ref(), i, j) {
                    Ordering::Equal => {}
                    other => return other,
                }
            }
            Ordering::Equal
        },
    );

    Ok(combined_fn)
}
