use ord::total_cmp;

use crate::datatypes::DataType;

use std::cmp::Ordering;

use crate::datatypes::*;
use crate::error::Error;
use crate::offset::Offset;
use crate::{array::*, types::NativeType};

/// Compare the values at two arbitrary indices in two arbitrary arrays.
pub type DynArrayComparator =
    Box<dyn Fn(&dyn Array, &dyn Array, usize, usize) -> Ordering + Send + Sync>;

fn compare_dyn_primitives<T: NativeType + Ord>() -> DynArrayComparator {
    Box::new(move |left, right, i, j| {
        let left = left.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        let right = right.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        total_cmp(&left.value(i), &right.value(j))
    })
}

fn compare_dyn_string<O: Offset>() -> DynArrayComparator {
    Box::new(move |left, right, i, j| {
        let left = left.as_any().downcast_ref::<Utf8Array<O>>().unwrap();
        let right = right.as_any().downcast_ref::<Utf8Array<O>>().unwrap();

        left.value(i).cmp(right.value(j))
    })
}

pub fn build_array_compare2(left: &DataType, right: &DataType) -> Result<DynArrayComparator> {
    use DataType::*;
    use IntervalUnit::*;
    use TimeUnit::*;
    Ok(match (left, right) {
        (a, b) if a != b => {
            return Err(Error::InvalidArgumentError(
                "Can't compare arrays of different types".to_string(),
            ));
        }
        // (Boolean, Boolean) => compare_boolean(left, right),
        (UInt8, UInt8) => compare_dyn_primitives::<u8>(),
        (UInt16, UInt16) => compare_dyn_primitives::<u16>(),
        (UInt32, UInt32) => compare_dyn_primitives::<u32>(),
        (UInt64, UInt64) => compare_dyn_primitives::<u64>(),
        (Int8, Int8) => compare_dyn_primitives::<i8>(),
        (Int16, Int16) => compare_dyn_primitives::<i16>(),
        (Int32, Int32)
        | (Date32, Date32)
        | (Time32(Second), Time32(Second))
        | (Time32(Millisecond), Time32(Millisecond))
        | (Interval(YearMonth), Interval(YearMonth)) => compare_dyn_primitives::<i32>(),
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
        | (Duration(Nanosecond), Duration(Nanosecond)) => compare_dyn_primitives::<i64>(),
        // (Float32, Float32) => compare_f32(left, right),
        // (Float64, Float64) => compare_f64(left, right),
        // (Decimal(_, _), Decimal(_, _)) => compare_primitives::<i128>(left, right),
        (Utf8, Utf8) => compare_dyn_string::<i32>(),
        (LargeUtf8, LargeUtf8) => compare_dyn_string::<i64>(),
        // (Binary, Binary) => compare_binary::<i32>(left, right),
        // (LargeBinary, LargeBinary) => compare_binary::<i64>(left, right),
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
        (lhs, _) => {
            return Err(Error::InvalidArgumentError(format!(
                "The data type type {lhs:?} has no natural order"
            )))
        }
    })
}
