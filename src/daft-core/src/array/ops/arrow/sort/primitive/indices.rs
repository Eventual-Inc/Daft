use daft_arrow::{
    array::PrimitiveArray,
    types::{Index, NativeType},
};

use super::common;

/// Unstable sort of indices.
pub fn indices_sorted_unstable_by<T, F>(
    array: &PrimitiveArray<T>,
    cmp: F,
    descending: bool,
    nulls_first: bool,
) -> PrimitiveArray<u64>
where
    T: NativeType,
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    let values = array.values().as_slice();

    unsafe {
        common::idx_sort(
            array.validity(),
            |l: &u64, r: &u64| {
                cmp(
                    values.get_unchecked((*l).to_usize()),
                    values.get_unchecked((*r).to_usize()),
                )
            },
            array.len(),
            descending,
            nulls_first,
        )
    }
}
