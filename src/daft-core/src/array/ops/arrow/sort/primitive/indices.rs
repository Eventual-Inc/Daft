use daft_arrow::{
    array::{Array, ArrowPrimitiveType, PrimitiveArray, UInt64Array},
};

use super::common;

/// Unstable sort of indices.
pub fn indices_sorted_unstable_by<T, F>(
    array: &PrimitiveArray<T>,
    cmp: F,
    descending: bool,
    nulls_first: bool,
) -> UInt64Array
where
    T: ArrowPrimitiveType + std::fmt::Debug,
    F: Fn(&T::Native, &T::Native) -> std::cmp::Ordering,
{
    let values = array.values();

    unsafe {
        common::idx_sort(
            array.nulls(),
            |l: &u64, r: &u64| {
                cmp(
                    values.get_unchecked((*l) as usize),
                    values.get_unchecked((*r) as usize),
                )
            },
            array.len(),
            descending,
            nulls_first,
        )
    }
}
