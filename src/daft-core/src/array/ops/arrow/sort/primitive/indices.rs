use arrow::{
    array::{Array, ArrowPrimitiveType, PrimitiveArray},
    datatypes::{ArrowNativeType, UInt64Type},
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
    let values = array.values().inner().typed_data::<T::Native>();

    unsafe {
        common::idx_sort(
            array.nulls(),
            |l: &u64, r: &u64| {
                cmp(
                    values.get_unchecked((*l).to_usize().unwrap_unchecked()),
                    values.get_unchecked((*r).to_usize().unwrap_unchecked()),
                )
            },
            array.len(),
            descending,
            nulls_first,
        )
    }
}
