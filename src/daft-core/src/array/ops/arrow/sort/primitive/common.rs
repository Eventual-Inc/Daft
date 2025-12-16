use daft_arrow::{
    array::{PrimitiveArray, ord::DynComparator},
    bitmap::Bitmap,
    types::NativeType,
};

pub fn idx_sort<F>(
    validity: Option<&Bitmap>,
    cmp: F,
    length: usize,
    descending: bool,
    nulls_first: bool,
) -> PrimitiveArray<u64>
where
    F: Fn(&u64, &u64) -> std::cmp::Ordering,
{
    let (mut indices, start_idx, end_idx) = generate_initial_indices(validity, length, nulls_first);
    let indices_slice = &mut indices.as_mut_slice()[start_idx..end_idx];

    if !descending {
        indices_slice.sort_unstable_by(|a, b| cmp(a, b));
    } else {
        indices_slice.sort_unstable_by(|a, b| cmp(b, a));
    }
    let data_type = u64::PRIMITIVE.into();
    PrimitiveArray::<u64>::new(data_type, indices.into(), None)
}

pub fn multi_column_idx_sort<F>(
    first_col_validity: Option<&Bitmap>,
    overall_cmp: F,
    others_cmp: &DynComparator,
    length: usize,
    first_col_nulls_first: bool,
) -> PrimitiveArray<u64>
where
    F: Fn(&u64, &u64) -> std::cmp::Ordering,
{
    let (mut indices, start_idx, end_idx) =
        generate_initial_indices(first_col_validity, length, first_col_nulls_first);
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

    let data_type = u64::PRIMITIVE.into();
    PrimitiveArray::<u64>::new(data_type, indices.into(), None)
}

fn generate_initial_indices(
    validity: Option<&Bitmap>,
    length: usize,
    nulls_first: bool,
) -> (Vec<u64>, usize, usize) {
    if let Some(validity) = validity {
        // number of null values
        let n_nulls = validity.unset_bits();
        // number of non null values
        let n_valid = length.saturating_sub(n_nulls);
        let mut indices = vec![u64::default(); length];
        let mut nulls = 0;
        let mut valids = 0;
        validity
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
