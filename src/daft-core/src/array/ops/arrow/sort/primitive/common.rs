use arrow::{
    array::{PrimitiveArray, types::UInt64Type},
    buffer::NullBuffer,
};

use crate::utils::ord::DynComparator;

pub fn idx_sort<F>(
    nulls: Option<&NullBuffer>,
    cmp: F,
    length: usize,
    descending: bool,
    nulls_first: bool,
) -> PrimitiveArray<UInt64Type>
where
    F: Fn(&u64, &u64) -> std::cmp::Ordering,
{
    idx_sort_dyn(nulls, &cmp, length, descending, nulls_first)
}

/// Non-generic inner function to avoid monomorphizing the sort for every closure type.
/// The heavy `sort_unstable_by` call (which pulls in `small_sort_network`) is only
/// compiled once regardless of how many different comparators are used.
#[inline(never)]
fn idx_sort_dyn(
    nulls: Option<&NullBuffer>,
    cmp: &dyn Fn(&u64, &u64) -> std::cmp::Ordering,
    length: usize,
    descending: bool,
    nulls_first: bool,
) -> PrimitiveArray<UInt64Type> {
    let (mut indices, start_idx, end_idx) = generate_initial_indices(nulls, length, nulls_first);
    let indices_slice = &mut indices.as_mut_slice()[start_idx..end_idx];

    if !descending {
        indices_slice.sort_unstable_by(|a, b| cmp(a, b));
    } else {
        indices_slice.sort_unstable_by(|a, b| cmp(b, a));
    }
    PrimitiveArray::<UInt64Type>::new(indices.into(), None)
}

pub fn multi_column_idx_sort<F>(
    first_col_nulls: Option<&NullBuffer>,
    overall_cmp: F,
    others_cmp: &DynComparator,
    length: usize,
    first_col_nulls_first: bool,
) -> PrimitiveArray<UInt64Type>
where
    F: Fn(&u64, &u64) -> std::cmp::Ordering,
{
    multi_column_idx_sort_dyn(
        first_col_nulls,
        &overall_cmp,
        others_cmp,
        length,
        first_col_nulls_first,
    )
}

#[inline(never)]
fn multi_column_idx_sort_dyn(
    first_col_nulls: Option<&NullBuffer>,
    overall_cmp: &dyn Fn(&u64, &u64) -> std::cmp::Ordering,
    others_cmp: &DynComparator,
    length: usize,
    first_col_nulls_first: bool,
) -> PrimitiveArray<UInt64Type> {
    let (mut indices, start_idx, end_idx) =
        generate_initial_indices(first_col_nulls, length, first_col_nulls_first);
    let indices_slice = &mut indices.as_mut_slice()[start_idx..end_idx];

    indices_slice.sort_unstable_by(|a, b| overall_cmp(a, b));
    if start_idx > 0 {
        let preslice_indices = &mut indices.as_mut_slice()[..start_idx];
        preslice_indices.sort_unstable_by(|a, b| others_cmp(*a as usize, *b as usize));
    }
    if end_idx < length {
        let postslice_indices = &mut indices.as_mut_slice()[end_idx..];
        postslice_indices.sort_unstable_by(|a, b| others_cmp(*a as usize, *b as usize));
    }

    PrimitiveArray::<UInt64Type>::new(indices.into(), None)
}

fn generate_initial_indices(
    nulls: Option<&NullBuffer>,
    length: usize,
    nulls_first: bool,
) -> (Vec<u64>, usize, usize) {
    if let Some(null_buffer) = nulls {
        // number of null values
        let n_nulls = null_buffer.null_count();
        // number of non null values
        let n_valid = length.saturating_sub(n_nulls);
        let mut indices = vec![u64::default(); length];
        let mut nulls = 0;
        let mut valids = 0;
        null_buffer
            .iter()
            .zip(0..length as u64)
            .for_each(|(is_not_null, index)| {
                match (is_not_null, nulls_first) {
                    // value && nulls first
                    (true, true) => {
                        indices[n_nulls + valids] = index;
                        valids += 1;
                    }
                    // value && nulls last
                    (true, false) => {
                        indices[valids] = index;
                        valids += 1;
                    }
                    // null && nulls first
                    (false, true) => {
                        indices[nulls] = index;
                        nulls += 1;
                    }
                    // null && nulls last
                    (false, false) => {
                        indices[n_valid + nulls] = index;
                        nulls += 1;
                    }
                }
            });

        // either `descending` or `nulls_first` means that nulls come first
        let (start_idx, end_idx) = if nulls_first {
            // since nulls come first, our valid values start at the end of the nulls
            (n_nulls, length)
        } else {
            // since nulls come last, our valid values start at the beginning of the array
            (0, n_valid)
        };

        (indices, start_idx, end_idx)
    } else {
        ((0..length as u64).collect::<Vec<u64>>(), 0, length)
    }
}
