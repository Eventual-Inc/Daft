use std::{cmp::Ordering, iter::zip};

use arrow::{
    array::{
        Array as ArrowArray, ArrowPrimitiveType, BooleanArray, FixedSizeBinaryArray,
        GenericBinaryArray, GenericStringArray, OffsetSizeTrait, PrimitiveArray, UInt64Builder,
        make_comparator,
    },
    compute::SortOptions,
    datatypes::*,
};
use common_error::{DaftError, DaftResult};
use num_traits::Float;

#[allow(clippy::eq_op)]
fn search_sorted_primitive_array<T>(
    sorted_array: &PrimitiveArray<T>,
    keys: &PrimitiveArray<T>,
    input_reversed: bool,
) -> PrimitiveArray<UInt64Type>
where
    T: ArrowPrimitiveType,
    T::Native: ArrowNativeType + PartialOrd,
{
    let array_size = sorted_array.len();

    let mut left = 0_usize;
    let mut right = array_size;

    let mut array_builder = UInt64Builder::with_capacity(array_size);

    let mut last_key = keys.iter().next().unwrap_or(None);
    let less = |l: T::Native, r: T::Native| l < r || (r != r && l == l);
    for key_val in keys {
        let is_last_key_lt = match (last_key, key_val) {
            (None, None) => false,
            (None, Some(_)) => input_reversed,
            (Some(last_key), Some(key_val)) => {
                if !input_reversed {
                    less(last_key, key_val)
                } else {
                    less(key_val, last_key)
                }
            }
            (Some(_), None) => !input_reversed,
        };
        if is_last_key_lt {
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
            let mid_val = unsafe { sorted_array.value_unchecked(mid_idx) };
            let is_key_val_lt = match (key_val, sorted_array.is_valid(mid_idx)) {
                (None, false) => false,
                (None, true) => input_reversed,
                (Some(key_val), true) => {
                    if !input_reversed {
                        less(key_val, mid_val)
                    } else {
                        less(mid_val, key_val)
                    }
                }
                (Some(_), false) => !input_reversed,
            };

            if is_key_val_lt {
                right = mid_idx;
            } else {
                left = mid_idx + 1;
            }
        }
        array_builder.append_value(left.try_into().unwrap());
        last_key = key_val;
    }

    array_builder.finish()
}

fn search_sorted_utf_array<O: OffsetSizeTrait>(
    sorted_array: &GenericStringArray<O>,
    keys: &GenericStringArray<O>,
    input_reversed: bool,
) -> PrimitiveArray<UInt64Type> {
    let array_size = sorted_array.len();
    let mut left = 0_usize;
    let mut right = array_size;

    let mut array_builder = UInt64Builder::with_capacity(array_size);
    let mut last_key = keys.iter().next().unwrap_or(None);
    for key_val in keys {
        let is_last_key_lt = match (last_key, key_val) {
            (None, None) => false,
            (None, Some(_)) => input_reversed,
            (Some(last_key), Some(key_val)) => {
                if !input_reversed {
                    last_key.lt(key_val)
                } else {
                    last_key.gt(key_val)
                }
            }
            (Some(_), None) => !input_reversed,
        };
        if is_last_key_lt {
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
            let mid_val = unsafe { sorted_array.value_unchecked(mid_idx) };
            let is_key_val_lt = match (key_val, sorted_array.is_valid(mid_idx)) {
                (None, false) => false,
                (None, true) => input_reversed,
                (Some(key_val), true) => {
                    if !input_reversed {
                        key_val.lt(mid_val)
                    } else {
                        mid_val.lt(key_val)
                    }
                }
                (Some(_), false) => !input_reversed,
            };

            if is_key_val_lt {
                right = mid_idx;
            } else {
                left = mid_idx + 1;
            }
        }
        array_builder.append_value(left.try_into().unwrap());
        last_key = key_val;
    }

    array_builder.finish()
}

fn search_sorted_boolean_array(
    sorted_array: &BooleanArray,
    keys: &BooleanArray,
    input_reversed: bool,
) -> PrimitiveArray<UInt64Type> {
    let array_size = sorted_array.len();
    let mut left = 0_usize;
    let mut right = array_size;

    // For boolean arrays, we know there can only be three possible values: true, false, and null
    // We can pre-compute the results for these three values and then reuse them to compute the results for the keys.
    let pre_computed_keys = &[Some(true), Some(false), None];
    let mut pre_computed_results: [u64; 3] = [0, 0, 0];
    let mut last_key = pre_computed_keys.iter().next().unwrap();
    for (i, key_val) in pre_computed_keys.iter().enumerate() {
        let is_last_key_lt = match (last_key, key_val) {
            (None, None) => false,
            (None, Some(_)) => input_reversed,
            (Some(last_key), Some(key_val)) => {
                if !input_reversed {
                    last_key.lt(key_val)
                } else {
                    last_key.gt(key_val)
                }
            }
            (Some(_), None) => !input_reversed,
        };
        if is_last_key_lt {
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
            let mid_val = unsafe { sorted_array.value_unchecked(mid_idx) };
            let is_key_val_lt = match (key_val, sorted_array.is_valid(mid_idx)) {
                (None, false) => false,
                (None, true) => input_reversed,
                (Some(key_val), true) => {
                    if !input_reversed {
                        key_val.lt(&mid_val)
                    } else {
                        mid_val.lt(key_val)
                    }
                }
                (Some(_), false) => !input_reversed,
            };

            if is_key_val_lt {
                right = mid_idx;
            } else {
                left = mid_idx + 1;
            }
        }
        pre_computed_results[i] = left.try_into().unwrap();
        last_key = key_val;
    }

    let mut array_builder = UInt64Builder::with_capacity(keys.len());
    for key_val in keys {
        let result = match key_val {
            Some(true) => pre_computed_results[0],
            Some(false) => pre_computed_results[1],
            None => pre_computed_results[2],
        };
        array_builder.append_value(result);
    }

    array_builder.finish()
}

fn search_sorted_binary_array<O: OffsetSizeTrait>(
    sorted_array: &GenericBinaryArray<O>,
    keys: &GenericBinaryArray<O>,
    input_reversed: bool,
) -> PrimitiveArray<UInt64Type> {
    let array_size = sorted_array.len();
    let mut left = 0_usize;
    let mut right = array_size;

    let mut array_builder = UInt64Builder::with_capacity(array_size);
    let mut last_key = keys.iter().next().unwrap_or(None);
    for key_val in keys {
        let is_last_key_lt = match (last_key, key_val) {
            (None, None) => false,
            (None, Some(_)) => input_reversed,
            (Some(last_key), Some(key_val)) => {
                if !input_reversed {
                    last_key.lt(key_val)
                } else {
                    last_key.gt(key_val)
                }
            }
            (Some(_), None) => !input_reversed,
        };
        if is_last_key_lt {
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
            let mid_val = unsafe { sorted_array.value_unchecked(mid_idx) };
            let is_key_val_lt = match (key_val, sorted_array.is_valid(mid_idx)) {
                (None, false) => false,
                (None, true) => input_reversed,
                (Some(key_val), true) => {
                    if !input_reversed {
                        key_val.lt(mid_val)
                    } else {
                        mid_val.lt(key_val)
                    }
                }
                (Some(_), false) => !input_reversed,
            };

            if is_key_val_lt {
                right = mid_idx;
            } else {
                left = mid_idx + 1;
            }
        }
        array_builder.append_value(left.try_into().unwrap());
        last_key = key_val;
    }

    array_builder.finish()
}

fn search_sorted_fixed_size_binary_array(
    sorted_array: &FixedSizeBinaryArray,
    keys: &FixedSizeBinaryArray,
    input_reversed: bool,
) -> PrimitiveArray<UInt64Type> {
    let array_size = sorted_array.len();
    let mut left = 0_usize;
    let mut right = array_size;

    let mut array_builder = UInt64Builder::with_capacity(array_size);
    let mut last_key = keys.iter().next().unwrap_or(None);
    for key_val in keys {
        let is_last_key_lt = match (last_key, key_val) {
            (None, None) => false,
            (None, Some(_)) => input_reversed,
            (Some(last_key), Some(key_val)) => {
                if !input_reversed {
                    last_key.lt(key_val)
                } else {
                    last_key.gt(key_val)
                }
            }
            (Some(_), None) => !input_reversed,
        };
        if is_last_key_lt {
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
            let mid_val = unsafe { sorted_array.value_unchecked(mid_idx) };
            let is_key_val_lt = match (key_val, sorted_array.is_valid(mid_idx)) {
                (None, false) => false,
                (None, true) => input_reversed,
                (Some(key_val), true) => {
                    if !input_reversed {
                        key_val.lt(mid_val)
                    } else {
                        mid_val.lt(key_val)
                    }
                }
                (Some(_), false) => !input_reversed,
            };

            if is_key_val_lt {
                right = mid_idx;
            } else {
                left = mid_idx + 1;
            }
        }
        array_builder.append_value(left.try_into().unwrap());
        last_key = key_val;
    }

    array_builder.finish()
}

macro_rules! dispatch_primitive_search_sorted {
    ($array:expr, $keys:expr, $reversed:expr, {
        $( $data_type:pat => $arrow_type:ty ),+ $(,)?
    }) => {{
        match $array.data_type() {
            $(
                $data_type => Ok(search_sorted_primitive_array::<$arrow_type>(
                    $array.as_any().downcast_ref().unwrap(),
                    $keys.as_any().downcast_ref().unwrap(),
                    $reversed,
                )),
            )+
            t => Err(DaftError::NotImplemented(format!(
                "search_sorted not implemented for type {t:?}"
            ))),
        }
    }};
}

type IsValid = Box<dyn Fn(usize) -> bool + Send + Sync>;

pub fn build_is_valid(array: &dyn ArrowArray) -> IsValid {
    if let Some(nulls) = array.nulls() {
        let nulls = nulls.clone();
        Box::new(move |x| !nulls.is_null(x)) // Return true if valid (not null)
    } else {
        Box::new(move |_| true)
    }
}

#[allow(clippy::eq_op)]
#[inline]
pub fn cmp_float<F: Float>(l: &F, r: &F) -> std::cmp::Ordering {
    match (l.is_nan(), r.is_nan()) {
        (false, false) => unsafe { l.partial_cmp(r).unwrap_unchecked() },
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
    }
}

/// Compare the values at two arbitrary indices in two arrays.
pub type DynPartialComparator = Box<dyn Fn(usize, usize) -> Option<Ordering> + Send + Sync>;

pub fn build_partial_compare_with_nulls(
    left: &dyn ArrowArray,
    right: &dyn ArrowArray,
    reversed: bool,
) -> DaftResult<DynPartialComparator> {
    let comparator = make_comparator(left, right, SortOptions::new(false, false))?;
    let left_is_valid = build_is_valid(left);
    let right_is_valid = build_is_valid(right);

    if reversed {
        Ok(Box::new(move |i: usize, j: usize| {
            match (left_is_valid(i), right_is_valid(j)) {
                (true, true) => Some(comparator(i, j).reverse()),
                (false, true) => Some(Ordering::Less),
                (true, false) => Some(Ordering::Greater),
                (false, false) => None,
            }
        }))
    } else {
        Ok(Box::new(move |i: usize, j: usize| {
            match (left_is_valid(i), right_is_valid(j)) {
                (true, true) => Some(comparator(i, j)),
                (false, true) => Some(Ordering::Greater),
                (true, false) => Some(Ordering::Less),
                (false, false) => None,
            }
        }))
    }
}

pub fn search_sorted_multi_array(
    sorted_arrays: &Vec<&dyn ArrowArray>,
    key_arrays: &Vec<&dyn ArrowArray>,
    input_reversed: &Vec<bool>,
) -> DaftResult<PrimitiveArray<UInt64Type>> {
    if sorted_arrays.is_empty() || key_arrays.is_empty() {
        return Err(DaftError::ValueError(
            "Passed in empty number of columns".to_string(),
        ));
    }

    if sorted_arrays.len() != key_arrays.len() {
        return Err(DaftError::ValueError(
            "Mismatch in number of columns".to_string(),
        ));
    }

    let sorted_array_size = sorted_arrays[0].len();
    for sorted_arr in sorted_arrays {
        if sorted_arr.len() != sorted_array_size {
            return Err(DaftError::ValueError(format!(
                "Mismatch in number of rows: {} vs {}",
                sorted_arr.len(),
                sorted_array_size
            )));
        }
    }
    let key_array_size = key_arrays[0].len();
    for key_arr in key_arrays {
        if key_arr.len() != key_array_size {
            return Err(DaftError::ValueError(format!(
                "Mismatch in number of rows: {} vs {}",
                key_arr.len(),
                sorted_array_size
            )));
        }
    }
    let mut cmp_list = Vec::with_capacity(sorted_arrays.len());
    for ((sorted_arr, key_arr), reversed) in zip(sorted_arrays, key_arrays).zip(input_reversed) {
        cmp_list.push(make_comparator(
            *sorted_arr,
            *key_arr,
            SortOptions::new(*reversed, false),
        )?);
    }

    let combined_comparator = |a_idx: usize, b_idx: usize| -> Ordering {
        for comparator in &cmp_list {
            match comparator(a_idx, b_idx) {
                Ordering::Equal => {}
                other => return other,
            }
        }
        Ordering::Equal
    };
    let mut array_builder = UInt64Builder::with_capacity(key_array_size);

    for key_idx in 0..key_array_size {
        let mut left = 0;
        let mut right = sorted_array_size;
        while left < right {
            let mid_idx = left + ((right - left) >> 1);
            if combined_comparator(mid_idx, key_idx).is_le() {
                left = mid_idx + 1;
            } else {
                right = mid_idx;
            }
        }
        array_builder.append_value(left.try_into().unwrap());
    }
    Ok(array_builder.finish())
}

pub fn search_sorted(
    sorted_array: &dyn ArrowArray,
    keys: &dyn ArrowArray,
    input_reversed: bool,
) -> DaftResult<PrimitiveArray<UInt64Type>> {
    if sorted_array.data_type() != keys.data_type() {
        let error_string = format!(
            "sorted array data type does not match keys data type: {:?} vs {:?}",
            sorted_array.data_type(),
            keys.data_type()
        );
        return Err(DaftError::TypeError(error_string));
    }

    match sorted_array.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Int64
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => {
            dispatch_primitive_search_sorted!(sorted_array, keys, input_reversed, {
                DataType::Int8 => Int8Type,
                DataType::Int16 => Int16Type,
                DataType::Int32 | DataType::Date32 | DataType::Time32(_) => Int32Type,
                DataType::Int64 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Time64(_) | DataType::Duration(_) => Int64Type,
                DataType::UInt8 => UInt8Type,
                DataType::UInt16 => UInt16Type,
                DataType::UInt32 => UInt32Type,
                DataType::UInt64 => UInt64Type,
                DataType::Float32 => Float32Type,
                DataType::Float64 => Float64Type,
                DataType::Decimal128(_, _) => Decimal128Type,
                DataType::Decimal256(_, _) => Decimal256Type,
            })
        }
        DataType::Utf8 => Ok(search_sorted_utf_array::<i32>(
            sorted_array.as_any().downcast_ref().unwrap(),
            keys.as_any().downcast_ref().unwrap(),
            input_reversed,
        )),
        DataType::LargeUtf8 => Ok(search_sorted_utf_array::<i64>(
            sorted_array.as_any().downcast_ref().unwrap(),
            keys.as_any().downcast_ref().unwrap(),
            input_reversed,
        )),
        DataType::Binary => Ok(search_sorted_binary_array::<i32>(
            sorted_array.as_any().downcast_ref().unwrap(),
            keys.as_any().downcast_ref().unwrap(),
            input_reversed,
        )),
        DataType::LargeBinary => Ok(search_sorted_binary_array::<i64>(
            sorted_array.as_any().downcast_ref().unwrap(),
            keys.as_any().downcast_ref().unwrap(),
            input_reversed,
        )),
        DataType::FixedSizeBinary(_) => Ok(search_sorted_fixed_size_binary_array(
            sorted_array.as_any().downcast_ref().unwrap(),
            keys.as_any().downcast_ref().unwrap(),
            input_reversed,
        )),
        DataType::Boolean => Ok(search_sorted_boolean_array(
            sorted_array.as_any().downcast_ref().unwrap(),
            keys.as_any().downcast_ref().unwrap(),
            input_reversed,
        )),
        t => Err(DaftError::NotImplemented(format!(
            "search_sorted not implemented for type {t:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Decimal128Array, Decimal256Array, Float64Array, Int32Array, Int64Array},
        datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, i256},
    };

    use super::*;

    #[test]
    fn test_search_sorted_int32() {
        // Create arrow-rs sorted array and keys
        let sorted = Int32Array::from(vec![1, 3, 5, 7, 9]);
        let keys = Int32Array::from(vec![0, 2, 5, 8, 10]);

        let result = search_sorted(&sorted, &keys, false).unwrap();

        // Note: This implements "right" insertion semantics (insert after equal values)
        // For sorted = [1, 3, 5, 7, 9]:
        assert_eq!(result.len(), 5);
        assert_eq!(result.value(0), 0); // 0 goes before index 0 (before 1)
        assert_eq!(result.value(1), 1); // 2 goes at index 1 (after 1, before 3)
        assert_eq!(result.value(2), 3); // 5 goes at index 3 (after 5 at index 2)
        assert_eq!(result.value(3), 4); // 8 goes at index 4 (after 7, before 9)
        assert_eq!(result.value(4), 5); // 10 goes at index 5 (after all elements)
    }

    #[test]
    fn test_search_sorted_int64() {
        let sorted = Int64Array::from(vec![10i64, 20, 30, 40, 50]);
        let keys = Int64Array::from(vec![15i64, 25, 35]);

        let result = search_sorted(&sorted, &keys, false).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 1); // 15 goes at index 1
        assert_eq!(result.value(1), 2); // 25 goes at index 2
        assert_eq!(result.value(2), 3); // 35 goes at index 3
    }

    #[test]
    fn test_search_sorted_float64() {
        let sorted = Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
        let keys = Float64Array::from(vec![0.5, 2.2, 3.0, 6.0]);

        let result = search_sorted(&sorted, &keys, false).unwrap();

        // Right insertion semantics
        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), 0); // 0.5 goes before index 0
        assert_eq!(result.value(1), 2); // 2.2 goes at index 2 (after 2.2 at index 1)
        assert_eq!(result.value(2), 2); // 3.0 goes at index 2 (after 2.2, before 3.3)
        assert_eq!(result.value(3), 5); // 6.0 goes after last element
    }

    #[test]
    fn test_search_sorted_decimal128() {
        // Create Decimal128 arrays representing values like 100.00, 200.00, etc. (scale=2)
        let sorted = Decimal128Array::from(vec![
            Some(10000i128), // 100.00
            Some(20000i128), // 200.00
            Some(30000i128), // 300.00
            Some(40000i128), // 400.00
            Some(50000i128), // 500.00
        ])
        .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 2)
        .unwrap();

        let keys = Decimal128Array::from(vec![
            Some(5000i128),  // 50.00
            Some(25000i128), // 250.00
            Some(30000i128), // 300.00
            Some(60000i128), // 600.00
        ])
        .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 2)
        .unwrap();

        let result = search_sorted(&sorted, &keys, false).unwrap();

        // Right insertion semantics
        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), 0); // 50.00 goes before 100.00
        assert_eq!(result.value(1), 2); // 250.00 goes between 200.00 and 300.00
        assert_eq!(result.value(2), 3); // 300.00 goes at index 3 (after 300.00 at index 2)
        assert_eq!(result.value(3), 5); // 600.00 goes after 500.00
    }

    #[test]
    fn test_search_sorted_decimal256() {
        // Create Decimal256 arrays with i256 values
        let sorted = Decimal256Array::from(vec![
            Some(i256::from(1000)),
            Some(i256::from(2000)),
            Some(i256::from(3000)),
            Some(i256::from(4000)),
        ])
        .with_precision_and_scale(DECIMAL256_MAX_PRECISION, 0)
        .unwrap();

        let keys = Decimal256Array::from(vec![
            Some(i256::from(500)),
            Some(i256::from(2500)),
            Some(i256::from(5000)),
        ])
        .with_precision_and_scale(DECIMAL256_MAX_PRECISION, 0)
        .unwrap();

        let result = search_sorted(&sorted, &keys, false).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 0); // 500 goes before 1000
        assert_eq!(result.value(1), 2); // 2500 goes between 2000 and 3000
        assert_eq!(result.value(2), 4); // 5000 goes after 4000
    }

    #[test]
    fn test_search_sorted_with_nulls() {
        let sorted = Int32Array::from(vec![Some(1), Some(3), None, Some(7), Some(9)]);
        let keys = Int32Array::from(vec![Some(2), None, Some(8)]);

        let result = search_sorted(&sorted, &keys, false).unwrap();

        assert_eq!(result.len(), 3);
        // Nulls typically sort to the end in ascending order
    }

    #[test]
    fn test_search_sorted_descending() {
        let sorted = Int32Array::from(vec![9, 7, 5, 3, 1]); // descending
        let keys = Int32Array::from(vec![8, 5, 2]);

        let result = search_sorted(&sorted, &keys, true).unwrap();

        assert_eq!(result.len(), 3);
    }
}
