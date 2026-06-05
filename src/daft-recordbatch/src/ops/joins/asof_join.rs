use std::cmp::Ordering;

use common_error::{DaftError, DaftResult};
use daft_core::{
    array::ops::full::FullNull,
    datatypes::{DataType, Field, UInt64Array},
    join::AsofJoinStrategy,
    kernels::cmp::{build_partial_compare_with_nulls, is_nearer},
};

use crate::RecordBatch;

/// Two-pointer asof join for pre-sorted input.
///
/// `left` and `right` must each have their by-key columns first (indices 0..num_by_keys)
/// and the on-key column last (index num_by_keys). Both must be sorted ascending by
/// (by_keys..., on_key).
///
/// Returns `(left_indices, right_indices)` where `left_indices` is always `0..left.len()`
/// and `right_indices[i]` is the matched right row index for left row `i` (None = no match).
pub fn asof_join_sorted(
    left: &RecordBatch,
    right: &RecordBatch,
    strategy: AsofJoinStrategy,
) -> DaftResult<(UInt64Array, UInt64Array)> {
    if left.num_columns() != right.num_columns() {
        return Err(DaftError::ValueError(format!(
            "Mismatch of asof key columns: left: {} vs right: {}",
            left.num_columns(),
            right.num_columns()
        )));
    }
    if left.num_columns() == 0 {
        return Err(DaftError::ValueError(
            "No columns were passed in to asof join on".to_string(),
        ));
    }

    let types_not_match = left
        .columns
        .iter()
        .zip(right.columns.iter())
        .any(|(l, r)| l.data_type() != r.data_type());
    if types_not_match {
        return Err(DaftError::SchemaMismatch(
            "Types between left and right asof keys do not match".to_string(),
        ));
    }

    if left.is_empty() {
        return Ok((
            UInt64Array::empty("left_indices", &DataType::UInt64),
            UInt64Array::empty("right_indices", &DataType::UInt64),
        ));
    }
    if right.is_empty() {
        return Ok((
            UInt64Array::from_vec("left_indices", (0..left.len() as u64).collect()),
            UInt64Array::full_null("right_indices", &DataType::UInt64, left.len()),
        ));
    }

    let num_by_keys = left.num_columns() - 1;
    let left_on = left.get_column(num_by_keys);
    let right_on = right.get_column(num_by_keys);

    let left_on_arr = left_on.to_arrow()?;
    let right_on_arr = right_on.to_arrow()?;

    let on_cmp = build_partial_compare_with_nulls(
        left_on_arr.as_ref(),
        right_on_arr.as_ref(),
        false,
    )?;

    let by_cmp_list = (0..num_by_keys)
        .map(|idx| {
            build_partial_compare_with_nulls(
                left.get_column(idx).to_arrow()?.as_ref(),
                right.get_column(idx).to_arrow()?.as_ref(),
                false,
            )
        })
        .collect::<DaftResult<Vec<_>>>()?;

    let cmp_by = |left_idx: usize, right_idx: usize| -> Option<Ordering> {
        for cmp in &by_cmp_list {
            match cmp(left_idx, right_idx) {
                Some(Ordering::Equal) => {}
                other => return other,
            }
        }
        Some(Ordering::Equal)
    };

    let left_by_cmp_list = (0..num_by_keys)
        .map(|idx| {
            build_partial_compare_with_nulls(
                left.get_column(idx).to_arrow()?.as_ref(),
                left.get_column(idx).to_arrow()?.as_ref(),
                false,
            )
        })
        .collect::<DaftResult<Vec<_>>>()?;

    let same_left_group = |prev: usize, curr: usize| -> bool {
        for cmp in &left_by_cmp_list {
            if !matches!(cmp(prev, curr), Some(Ordering::Equal)) {
                return false;
            }
        }
        true
    };

    let mut right_idx = 0usize;
    let mut best_match: Option<u64> = None;
    let mut right_indices: Vec<Option<u64>> = Vec::with_capacity(left.len());

    for left_idx in 0..left.len() {
        if left_idx > 0 && num_by_keys > 0 && !same_left_group(left_idx - 1, left_idx) {
            best_match = None;
        }

        if !left_on_arr.is_valid(left_idx) {
            right_indices.push(None);
            continue;
        }

        // Advance right past rows from earlier by-key groups.
        while right_idx < right.len() {
            match cmp_by(left_idx, right_idx) {
                Some(Ordering::Greater) | None => {
                    right_idx += 1;
                    best_match = None;
                }
                _ => break,
            }
        }

        // If right is exhausted or in a future by-key group, no on-key scan possible.
        if right_idx == right.len()
            || !matches!(cmp_by(left_idx, right_idx), Some(Ordering::Equal))
        {
            right_indices.push(match strategy {
                AsofJoinStrategy::Backward | AsofJoinStrategy::Nearest => best_match,
                AsofJoinStrategy::Forward => None,
            });
            continue;
        }

        match strategy {
            AsofJoinStrategy::Backward => {
                while right_idx < right.len()
                    && matches!(cmp_by(left_idx, right_idx), Some(Ordering::Equal))
                {
                    match on_cmp(left_idx, right_idx) {
                        Some(Ordering::Greater) | Some(Ordering::Equal) => {
                            best_match = Some(right_idx as u64);
                            right_idx += 1;
                        }
                        _ => break,
                    }
                }
                right_indices.push(best_match);
            }
            AsofJoinStrategy::Forward => {
                // Skip right rows where right.on_key < left.on_key.
                while right_idx < right.len()
                    && matches!(cmp_by(left_idx, right_idx), Some(Ordering::Equal))
                {
                    match on_cmp(left_idx, right_idx) {
                        Some(Ordering::Greater) => {
                            right_idx += 1;
                        }
                        _ => break,
                    }
                }
                // Output right[right_idx] if it is in the same group and right.on_key >= left.on_key.
                let forward = if right_idx < right.len()
                    && matches!(cmp_by(left_idx, right_idx), Some(Ordering::Equal))
                    && right_on_arr.is_valid(right_idx)
                    && !matches!(on_cmp(left_idx, right_idx), Some(Ordering::Greater))
                {
                    Some(right_idx as u64)
                } else {
                    None
                };
                right_indices.push(forward);
            }
            AsofJoinStrategy::Nearest => {
                // Backward scan to find best_match (last right.on_key <= left.on_key).
                while right_idx < right.len()
                    && matches!(cmp_by(left_idx, right_idx), Some(Ordering::Equal))
                {
                    match on_cmp(left_idx, right_idx) {
                        Some(Ordering::Greater) | Some(Ordering::Equal) => {
                            best_match = Some(right_idx as u64);
                            right_idx += 1;
                        }
                        _ => break,
                    }
                }
                // Forward candidate: right[right_idx] if still in the same group and non-null.
                let forward = if right_idx < right.len()
                    && matches!(cmp_by(left_idx, right_idx), Some(Ordering::Equal))
                    && right_on_arr.is_valid(right_idx)
                {
                    Some(right_idx as u64)
                } else {
                    None
                };
                let best = match (best_match, forward) {
                    (None, f) => f,
                    (b, None) => b,
                    (Some(b_idx), Some(f_idx)) => {
                        if is_nearer(
                            right_on_arr.as_ref(),
                            f_idx as usize,
                            right_on_arr.as_ref(),
                            b_idx as usize,
                            left_on_arr.as_ref(),
                            left_idx,
                        ) {
                            forward
                        } else {
                            best_match
                        }
                    }
                };
                right_indices.push(best);
            }
        }
    }

    let left_indices =
        UInt64Array::from_vec("left_indices", (0..left.len() as u64).collect());
    let right_indices = UInt64Array::from_iter(
        Field::new("right_indices", DataType::UInt64),
        right_indices.into_iter(),
    );
    Ok((left_indices, right_indices))
}
