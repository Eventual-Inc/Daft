use arrow::{
    array::{Array, ArrowPrimitiveType, PrimitiveArray},
    datatypes::UInt64Type,
};

use super::common;

/// Unstable sort of indices.
pub fn indices_sorted_unstable_by<T, F>(
    array: &PrimitiveArray<T>,
    cmp: F,
    descending: bool,
    nulls_first: bool,
) -> PrimitiveArray<UInt64Type>
where
    T: ArrowPrimitiveType,
    F: Fn(&T::Native, &T::Native) -> std::cmp::Ordering,
{
    unsafe {
        common::idx_sort(
            array.nulls(),
            |l: &u64, r: &u64| {
                // idx_sort generates indices from array.len(), so conversion back to usize is safe
                cmp(
                    &array.value_unchecked(*l as usize),
                    &array.value_unchecked(*r as usize),
                )
            },
            array.len(),
            descending,
            nulls_first,
        )
    }
}
