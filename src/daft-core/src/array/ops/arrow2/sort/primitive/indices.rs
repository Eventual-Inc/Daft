use arrow2::{
    array::PrimitiveArray,
    types::{Index, NativeType},
};

use super::common;

/// Unstable sort of indices.
pub fn indices_sorted_unstable_by<I, T, F>(
    array: &PrimitiveArray<T>,
    cmp: F,
    descending: bool,
) -> PrimitiveArray<I>
where
    I: Index,
    T: NativeType,
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    let values = array.values().as_slice();

    unsafe {
        common::idx_sort(
            array.validity(),
            |l: &I, r: &I| {
                cmp(
                    values.get_unchecked((*l).to_usize()),
                    values.get_unchecked((*r).to_usize()),
                )
            },
            array.len(),
            descending,
        )
    }
}
