use arrow2::{
    array::{ord::DynComparator, PrimitiveArray},
    bitmap::Bitmap,
    types::Index,
};

pub fn idx_sort<I, F>(
    validity: Option<&Bitmap>,
    cmp: F,
    length: usize,
    descending: bool,
    nulls_first: bool,
) -> PrimitiveArray<I>
where
    I: Index,
    F: Fn(&I, &I) -> std::cmp::Ordering,
{
    let (mut indices, start_idx, end_idx) =
        generate_initial_indices::<I>(validity, length, descending, nulls_first);
    let indices_slice = &mut indices.as_mut_slice()[start_idx..end_idx];

    if !descending {
        indices_slice.sort_unstable_by(|a, b| cmp(a, b));
    } else {
        indices_slice.sort_unstable_by(|a, b| cmp(b, a));
    }
    let data_type = I::PRIMITIVE.into();
    PrimitiveArray::<I>::new(data_type, indices.into(), None)
}

pub fn multi_column_idx_sort<I, F>(
    first_col_validity: Option<&Bitmap>,
    overall_cmp: F,
    others_cmp: &DynComparator,
    length: usize,
    first_col_desc: bool,
    first_col_nulls_first: bool,
) -> PrimitiveArray<I>
where
    I: Index,
    F: Fn(&I, &I) -> std::cmp::Ordering,
{
    let (mut indices, start_idx, end_idx) = generate_initial_indices::<I>(
        first_col_validity,
        length,
        first_col_desc,
        first_col_nulls_first,
    );
    let indices_slice = &mut indices.as_mut_slice()[start_idx..end_idx];

    indices_slice.sort_unstable_by(|a, b| overall_cmp(a, b));
    if start_idx > 0 {
        let preslice_indices = &mut indices.as_mut_slice()[..start_idx];
        preslice_indices.sort_unstable_by(|a, b| others_cmp(a.to_usize(), b.to_usize()));
    }
    if end_idx < length {
        let postslice_indices = &mut indices.as_mut_slice()[end_idx..];
        postslice_indices.sort_unstable_by(|a, b| others_cmp(a.to_usize(), b.to_usize()));
    }

    let data_type = I::PRIMITIVE.into();
    PrimitiveArray::<I>::new(data_type, indices.into(), None)
}

fn generate_initial_indices<I>(
    validity: Option<&Bitmap>,
    length: usize,
    descending: bool,
    nulls_first: bool,
) -> (Vec<I>, usize, usize)
where
    I: Index,
{
    let mut start_idx: usize = 0;
    let mut end_idx: usize = length;

    if let Some(validity) = validity {
        // number of null values
        let n_nulls = validity.unset_bits();
        // number of non null values
        let n_valid = length.saturating_sub(n_nulls);
        let mut indices = vec![I::default(); length];
        let mut nulls = 0;
        let mut valids = 0;
        if descending {
            validity
                .iter()
                .zip(I::range(0, length).unwrap())
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
            start_idx = n_nulls;
        } else {
            validity
                .iter()
                .zip(I::range(0, length).unwrap())
                .for_each(|(is_not_null, index)| {
                    match (is_not_null, nulls_first) {
                        // value && nulls_first
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
            if nulls_first {
                start_idx = n_nulls;
            } else {
                end_idx = n_valid;
            }
        }
        (indices, start_idx, end_idx)
    } else {
        (
            I::range(0, length).unwrap().collect::<Vec<_>>(),
            start_idx,
            end_idx,
        )
    }
}
