use arrow2::array::ord::DynComparator;
use arrow2::{array::PrimitiveArray, bitmap::Bitmap, types::Index};

pub fn idx_sort<I, F>(
    validity: Option<&Bitmap>,
    cmp: F,
    length: usize,
    descending: bool,
) -> PrimitiveArray<I>
where
    I: Index,
    F: Fn(&I, &I) -> std::cmp::Ordering,
{
    let (mut indices, start_idx, end_idx) =
        generate_initial_indices::<I>(validity, length, descending);
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
) -> PrimitiveArray<I>
where
    I: Index,
    F: Fn(&I, &I) -> std::cmp::Ordering,
{
    let (mut indices, start_idx, end_idx) =
        generate_initial_indices::<I>(first_col_validity, length, first_col_desc);
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
) -> (Vec<I>, usize, usize)
where
    I: Index,
{
    let mut start_idx: usize = 0;
    let mut end_idx: usize = length;

    if let Some(validity) = validity {
        let mut indices = vec![I::default(); length];
        if descending {
            let mut nulls = 0;
            let mut valids = 0;
            validity
                .iter()
                .zip(I::range(0, length).unwrap())
                .for_each(|(is_valid, index)| {
                    if is_valid {
                        indices[validity.unset_bits() + valids] = index;
                        valids += 1;
                    } else {
                        indices[nulls] = index;
                        nulls += 1;
                    }
                });
            start_idx = validity.unset_bits();
        } else {
            let last_valid_index = length.saturating_sub(validity.unset_bits());
            let mut nulls = 0;
            let mut valids = 0;
            validity
                .iter()
                .zip(I::range(0, length).unwrap())
                .for_each(|(x, index)| {
                    if x {
                        indices[valids] = index;
                        valids += 1;
                    } else {
                        indices[last_valid_index + nulls] = index;
                        nulls += 1;
                    }
                });
            end_idx = last_valid_index;
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
