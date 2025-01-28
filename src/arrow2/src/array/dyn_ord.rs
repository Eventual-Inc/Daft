use std::cmp::Ordering;

use num_traits::Float;
use ord::total_cmp;

use crate::{array::*, datatypes::*, error::Error, offset::Offset, types::NativeType};

/// Compare the values at two arbitrary indices in two arbitrary arrays.
pub type DynArrayComparator =
    Box<dyn Fn(&dyn Array, &dyn Array, usize, usize) -> Ordering + Send + Sync>;

#[inline]
unsafe fn is_valid(arr: &dyn Array, i: usize) -> bool {
    // avoid dyn function hop by using generic
    arr.validity()
        .as_ref()
        .map(|x| x.get_bit_unchecked(i))
        .unwrap_or(true)
}

#[inline]
fn compare_with_nulls<A: Array, F: FnOnce() -> Ordering>(
    left: &A,
    right: &A,
    i: usize,
    j: usize,
    nulls_equal: bool,
    cmp: F,
) -> Ordering {
    assert!(i < left.len());
    assert!(j < right.len());
    match (unsafe { is_valid(left, i) }, unsafe { is_valid(right, j) }) {
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
fn cmp_float<F: Float>(l: &F, r: &F, nans_equal: bool) -> std::cmp::Ordering {
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

fn compare_dyn_floats<T: NativeType + Float>(
    nulls_equal: bool,
    nans_equal: bool,
) -> DynArrayComparator {
    Box::new(move |left, right, i, j| {
        let left = left.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        let right = right.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        compare_with_nulls(left, right, i, j, nulls_equal, || {
            cmp_float::<T>(
                &unsafe { left.value_unchecked(i) },
                &unsafe { right.value_unchecked(j) },
                nans_equal,
            )
        })
    })
}

fn compare_dyn_primitives<T: NativeType + Ord>(nulls_equal: bool) -> DynArrayComparator {
    Box::new(move |left, right, i, j| {
        let left = left.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        let right = right.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        compare_with_nulls(left, right, i, j, nulls_equal, || {
            total_cmp(&unsafe { left.value_unchecked(i) }, &unsafe {
                right.value_unchecked(j)
            })
        })
    })
}

fn compare_dyn_string<O: Offset>(nulls_equal: bool) -> DynArrayComparator {
    Box::new(move |left, right, i, j| {
        let left = left.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
        let right = right.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
        compare_with_nulls(left, right, i, j, nulls_equal, || {
            unsafe { left.value_unchecked(i) }.cmp(unsafe { right.value_unchecked(j) })
        })
    })
}

fn compare_dyn_binary<O: Offset>(nulls_equal: bool) -> DynArrayComparator {
    Box::new(move |left, right, i, j| {
        let left = left.as_any().downcast_ref::<BinaryArray<O>>().unwrap();
        let right = right.as_any().downcast_ref::<BinaryArray<O>>().unwrap();
        compare_with_nulls(left, right, i, j, nulls_equal, || {
            unsafe { left.value_unchecked(i) }.cmp(unsafe { right.value_unchecked(j) })
        })
    })
}

fn compare_dyn_boolean(nulls_equal: bool) -> DynArrayComparator {
    Box::new(move |left, right, i, j| {
        let left = left.as_any().downcast_ref::<BooleanArray>().unwrap();
        let right = right.as_any().downcast_ref::<BooleanArray>().unwrap();
        compare_with_nulls(left, right, i, j, nulls_equal, || {
            unsafe { left.value_unchecked(i) }.cmp(unsafe { &right.value_unchecked(j) })
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

pub fn build_dyn_array_compare(
    left: &DataType,
    right: &DataType,
    nulls_equal: bool,
    nans_equal: bool,
) -> Result<DynArrayComparator> {
    use DataType::*;
    use IntervalUnit::*;
    use TimeUnit::*;
    Ok(match (left, right) {
        (a, b) if a != b => {
            return Err(Error::InvalidArgumentError(
                "Can't compare arrays of different types".to_string(),
            ));
        }
        (Boolean, Boolean) => compare_dyn_boolean(nulls_equal),
        (UInt8, UInt8) => compare_dyn_primitives::<u8>(nulls_equal),
        (UInt16, UInt16) => compare_dyn_primitives::<u16>(nulls_equal),
        (UInt32, UInt32) => compare_dyn_primitives::<u32>(nulls_equal),
        (UInt64, UInt64) => compare_dyn_primitives::<u64>(nulls_equal),
        (Int8, Int8) => compare_dyn_primitives::<i8>(nulls_equal),
        (Int16, Int16) => compare_dyn_primitives::<i16>(nulls_equal),
        (Int32, Int32)
        | (Date32, Date32)
        | (Time32(Second), Time32(Second))
        | (Time32(Millisecond), Time32(Millisecond))
        | (Interval(YearMonth), Interval(YearMonth)) => compare_dyn_primitives::<i32>(nulls_equal),
        (Int64, Int64)
        | (Date64, Date64)
        | (Time64(Microsecond), Time64(Microsecond))
        | (Time64(Nanosecond), Time64(Nanosecond))
        | (Timestamp(Second, None), Timestamp(Second, None))
        | (Timestamp(Millisecond, None), Timestamp(Millisecond, None))
        | (Timestamp(Microsecond, None), Timestamp(Microsecond, None))
        | (Timestamp(Nanosecond, None), Timestamp(Nanosecond, None))
        | (Duration(Second), Duration(Second))
        | (Duration(Millisecond), Duration(Millisecond))
        | (Duration(Microsecond), Duration(Microsecond))
        | (Duration(Nanosecond), Duration(Nanosecond)) => {
            compare_dyn_primitives::<i64>(nulls_equal)
        }
        (Float32, Float32) => compare_dyn_floats::<f32>(nulls_equal, nans_equal),
        (Float64, Float64) => compare_dyn_floats::<f64>(nulls_equal, nans_equal),
        (Decimal(_, _), Decimal(_, _)) => compare_dyn_primitives::<i128>(nulls_equal),
        (Utf8, Utf8) => compare_dyn_string::<i32>(nulls_equal),
        (LargeUtf8, LargeUtf8) => compare_dyn_string::<i64>(nulls_equal),
        (Binary, Binary) => compare_dyn_binary::<i32>(nulls_equal),
        (LargeBinary, LargeBinary) => compare_dyn_binary::<i64>(nulls_equal),
        // (Dictionary(key_type_lhs, ..), Dictionary(key_type_rhs, ..)) => {
        //     match (key_type_lhs, key_type_rhs) {
        //         (IntegerType::UInt8, IntegerType::UInt8) => dyn_dict!(u8, left, right),
        //         (IntegerType::UInt16, IntegerType::UInt16) => dyn_dict!(u16, left, right),
        //         (IntegerType::UInt32, IntegerType::UInt32) => dyn_dict!(u32, left, right),
        //         (IntegerType::UInt64, IntegerType::UInt64) => dyn_dict!(u64, left, right),
        //         (IntegerType::Int8, IntegerType::Int8) => dyn_dict!(i8, left, right),
        //         (IntegerType::Int16, IntegerType::Int16) => dyn_dict!(i16, left, right),
        //         (IntegerType::Int32, IntegerType::Int32) => dyn_dict!(i32, left, right),
        //         (IntegerType::Int64, IntegerType::Int64) => dyn_dict!(i64, left, right),
        //         (lhs, _) => {
        //             return Err(Error::InvalidArgumentError(format!(
        //                 "Dictionaries do not support keys of type {lhs:?}"
        //             )))
        //         }
        //     }
        // }
        (Null, Null) => compare_dyn_null(nulls_equal),
        (lhs, _) => {
            return Err(Error::InvalidArgumentError(format!(
                "The data type type {lhs:?} has no natural order"
            )))
        }
    })
}
