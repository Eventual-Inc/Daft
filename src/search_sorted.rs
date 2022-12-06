use std::{cmp::Ordering, collections::btree_map::Keys};

use arrow2::{
    array::Array,
    array::{BinaryArray, PrimitiveArray, Utf8Array},
    compute::sort,
    datatypes::{DataType, PhysicalType},
    error::{Error, Result},
    types::{NativeType, Offset},
};

fn search_sorted_primitive_array<T: NativeType + PartialOrd>(
    sorted_array: &PrimitiveArray<T>,
    keys: &PrimitiveArray<T>,
) -> PrimitiveArray<u64> {
    let array_size = sorted_array.len() as usize;
    let mut left = 0 as usize;
    let mut right = array_size;
    let input_reversed = false;

    let mut results: Vec<u64> = Vec::with_capacity(array_size);
    let mut last_key = keys.iter().next().unwrap();
    for key_val in keys.iter() {
        let is_last_key_le = match (last_key, key_val) {
            (None, None) => false,
            (Some(last_key), Some(key_val)) => last_key.le(key_val),
            (None, _) => false,
            (_, None) => true,
        };
        if is_last_key_le {
            right = array_size;
        } else {
            left = 0;
            right = if right < array_size {
                right + 1
            } else {
                array_size
            };
        }
        while left < right {
            let mid_idx = left + ((right - left) >> 1);
            let corrected_idx = if input_reversed {
                array_size - mid_idx - 1
            } else {
                mid_idx
            };
            let mid_val = unsafe { sorted_array.value_unchecked(corrected_idx) };
            let is_key_val_le = match (key_val, sorted_array.is_valid(corrected_idx)) {
                (None, true) => false,
                (None, false) => true,
                (Some(key_val), true) => key_val.le(&mid_val),
                (_, false) => true,
            };
            if is_key_val_le {
                right = mid_idx;
            } else {
                left = mid_idx + 1;
            }
        }
        let result_idx = if input_reversed {
            array_size - left
        } else {
            left
        };
        results.push(result_idx.try_into().unwrap());
        last_key = key_val;
    }

    PrimitiveArray::<u64>::new(DataType::UInt64, results.into(), None)
}

macro_rules! with_match_primitive_type {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use arrow2::datatypes::PrimitiveType::*;
    use arrow2::types::{days_ms, months_days_ns};
    match $key_type {
        Int8 => __with_ty__! { i8 },
        Int16 => __with_ty__! { i16 },
        Int32 => __with_ty__! { i32 },
        Int64 => __with_ty__! { i64 },
        Int128 => __with_ty__! { i128 },
        // DaysMs => __with_ty__! { days_ms },
        // MonthDayNano => __with_ty__! { months_days_ns },
        UInt8 => __with_ty__! { u8 },
        UInt16 => __with_ty__! { u16 },
        UInt32 => __with_ty__! { u32 },
        UInt64 => __with_ty__! { u64 },
        Float32 => __with_ty__! { f32 },
        Float64 => __with_ty__! { f64 },
        _ => return Err(Error::NotYetImplemented(format!(
            "search_sorted not implemented for type {:?}",
            $key_type
        )))
    }
})}

pub fn search_sorted(sorted_array: &dyn Array, keys: &dyn Array) -> Result<PrimitiveArray<u64>> {
    use PhysicalType::*;
    Ok(match sorted_array.data_type().to_physical_type() {
        // Boolean => hash_boolean(array.as_any().downcast_ref().unwrap()),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            search_sorted_primitive_array::<$T>(sorted_array.as_any().downcast_ref().unwrap(), keys.as_any().downcast_ref().unwrap())
        }),
        t => {
            return Err(Error::NotYetImplemented(format!(
                "Hash not implemented for type {:?}",
                t
            )))
        }
    })
}
